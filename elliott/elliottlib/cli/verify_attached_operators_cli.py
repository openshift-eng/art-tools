import json
import re
from io import BytesIO
from typing import Dict, List, Set, Tuple
from zipfile import ZipFile

import click
import koji
import requests
import yaml
from artcommonlib import exectools
from artcommonlib.format_util import green_print, red_print
from errata_tool import Erratum

from elliottlib import brew, constants, errata
from elliottlib.cli.common import cli, move_builds, pass_runtime
from elliottlib.cli.find_builds_cli import find_builds_cli
from elliottlib.exceptions import BrewBuildException, ElliottFatalError
from elliottlib.runtime import Runtime


@cli.command("verify-attached-operators", short_help="Verify attached operator bundle references are (being) shipped")
@click.option(
    "--omit-shipped", required=False, is_flag=True, help="Do not query shipped images to satisfy bundle references"
)
@click.option(
    "--omit-attached",
    required=False,
    is_flag=True,
    help="Do not query images shipping in other advisories to satisfy bundle references",
)
@click.option(
    "--gather-dependencies",
    required=False,
    is_flag=True,
    help="Attach unshipped dependencies to advisory, removing them from any other advisory",
)
@click.argument("advisories", nargs=-1, type=click.IntRange(1), required=True)
@pass_runtime
@click.pass_context
def verify_attached_operators_cli(
    ctx: click.Context,
    runtime: Runtime,
    omit_shipped: bool,
    omit_attached: bool,
    gather_dependencies: bool,
    advisories: Tuple[int, ...],
):
    """
    Verify attached operator bundle references are shipping or already shipped.

    Args are a list of advisory IDs that may contain operator bundle builds
    and any container builds shipping alongside.

    Verifies whether the operator bundle references are expected to be fulfilled when shipped.
    An error is raised if any references are missing, or are not shipping to the expected repos,
    or if bundle CSV contents fail validation.

    By default, references may be fulfilled by containers that have shipped already or are
    shipping in any advisory (not just those specified). The omission options may be used to limit
    what is considered fulfillment; for example, to prepare an early operator release, specify both
    to ensure that only containers attached to the single advisory shipping are considered:

        elliott -g openshift-4.13 verify-attached-operators \\
                --omit-attached --omit-shipped 111422

    Since builds which are attached somewhere (should) include those which have shipped already,
    --omit-shipped has no real effect if --omit-attached is not also specified.
    If only --omit-attached is specified, then builds shipped previously are still considered to
    fulfill bundle references, as are those attached to advisories specified in the args.

    --gather-dependencies can be specified to attach unshipped dependencies to the bundle advisory,
    removing them from any other advisory. This is intended only for operator pre-releases; during
    ordinary z-streams it may risk stealing dependencies from advisories intended to ship earlier.
    """

    if gather_dependencies and omit_shipped:
        red_print("--gather-dependencies is incompatible with --omit-shipped")
        exit(1)  # it could cause us to attempt to attach already-shipped content
    if gather_dependencies and len(advisories) != 1:
        red_print("--gather-dependencies requires specifying exactly one metadata advisory as a destination")
        exit(1)

    runtime.initialize(mode="images")

    problems, invalid, unshipped_builds_by_advisory = _analyze_image_builds(
        runtime, advisories, omit_shipped, omit_attached, gather_dependencies
    )

    if invalid or problems:
        raise ElliottFatalError("Please resolve the errors above before shipping bundles.")

    changes_needed, changes_made = _handle_missing_builds(
        ctx, gather_dependencies, advisories, unshipped_builds_by_advisory
    )
    if changes_needed:
        if changes_made:
            # only happens with --gather-dependencies
            green_print("\nRe-running validation after gathering necessary builds.\n")
            # necessary e.g. if any builds were added, we can now check their CDN repos.
            # this time, consider it a problem when builds are not attached correctly.
            problems, invalid, unshipped_builds_by_advisory = _analyze_image_builds(
                runtime, advisories, omit_shipped, True, False
            )
            if invalid or problems:
                raise ElliottFatalError("Please resolve the errors above before shipping bundles.")
        else:
            raise ElliottFatalError("Please attach builds as indicated before shipping bundles.")

    green_print("All operator bundles were valid and references were found.")


def _analyze_image_builds(
    runtime, advisories: List, omit_shipped: bool, omit_attached: bool, gather_dependencies: bool
) -> (List[str], bool, Dict):
    """
    Look up all the bundles in the advisories and determine if they will ship correctly
    params:
        advisories:
        omit_shipped:
        omit_attached:
        gather_dependencies:
    returns:
        problems: list of NVRs with problems
        invalid: True if any bundle CSVs were invalid
        unshipped_builds_by_advisory: Dict by advisory id (or None) of builds that are not attached
                                      where they are needed
    """
    brew_session = koji.ClientSession(runtime.group_config.urls.brewhub or constants.BREW_HUB)
    image_builds = _get_attached_image_builds(brew_session, advisories)

    bundles = _get_bundle_images(image_builds)
    if not bundles:
        adv_str = ", ".join(str(a) for a in advisories)
        green_print(f"No bundle builds found in advisories ({adv_str}).")
        return [], False, {}

    # check if references are satisfied by any image we are shipping or have shipped
    if not omit_shipped:
        image_builds.extend(_get_shipped_images(runtime, brew_session))
    available_shasums = _extract_available_image_shasums(image_builds)
    problems, unshipped_builds_by_advisory = _missing_references(
        runtime, bundles, available_shasums, omit_attached, gather_dependencies
    )

    if problems:
        problem_str = "\n              ".join(problems)
        red_print(f"""
            Some references had problems (see notes above):
              {problem_str}
            Ensure all bundle references are configured with the necessary repos.
        """)

    invalid = _validate_csvs(bundles)
    if invalid:
        invalid_str = "\n              ".join(invalid)
        red_print(f"""
            The following bundles failed CSV validation:
              {invalid_str}
            Check art.yaml substitutions for failing matches.
        """)

    return problems, invalid, unshipped_builds_by_advisory


def _validate_csvs(bundles):
    invalid = set()
    for bundle in bundles:
        nvr, csv = bundle["nvr"], bundle["csv"]
        # check the CSV for invalid metadata
        try:
            if not re.search(r"-\d{12}", csv["metadata"]["name"]):
                red_print(f"Bundle {nvr} CSV metadata.name has no datestamp: {csv['metadata']['name']}")
                invalid.add(nvr)
            if not re.search(r"-\d{12}", csv["spec"]["version"]):
                red_print(f"Bundle {nvr} CSV spec.version has no datestamp: {csv['spec']['version']}")
                invalid.add(nvr)
        except KeyError as ex:
            red_print(f"Bundle {nvr} CSV is missing key: {ex}")
            invalid.add(nvr)
    return invalid


def _get_attached_image_builds(brew_session, advisories):
    # get all attached image builds
    build_nvrs = []
    for advisory in advisories:
        green_print(f"Retrieving builds from advisory {advisory}")
        advisory = Erratum(errata_id=advisory)
        for build_list in advisory.errata_builds.values():  # one per product version
            build_nvrs.extend(build_list)

    green_print(f"Found {len(build_nvrs)} builds")
    return [build for build in brew.get_build_objects(build_nvrs, brew_session) if _is_image(build)]


def _is_image(build):
    return build.get("extra", {}).get("osbs_build", {}).get("kind") == "container_build"


def _is_bundle(image_build):
    return "operator_bundle" in image_build.get("extra", {}).get("osbs_build", {}).get("subtypes", [])


def _get_bundle_images(image_builds):
    # extract referenced images from bundles to be shipped
    # returns a map[pullspec: bundle_nvr]
    bundles = []
    for image in image_builds:
        if _is_bundle(image):
            image["csv"] = _download_bundle_csv(image)
            bundles.append(image)
    return bundles


def _download_bundle_csv(bundle_build):
    # the CSV is buried in an archive
    url = constants.BREW_DOWNLOAD_TEMPLATE.format(
        name=bundle_build["package_name"],
        version=bundle_build["version"],
        release=bundle_build["release"],
        file_path="operator-manifests/operator_manifests.zip",
    )
    try:
        res = requests.get(url, timeout=10.0)
    except Exception as ex:
        raise ElliottFatalError(f"bundle data download {url} failed: {ex}")
    if res.status_code != 200:
        raise ElliottFatalError(f"bundle data download {url} failed (status_code={res.status_code}): {res.text}")

    csv = {}
    with ZipFile(BytesIO(res.content)) as z:
        for filename in z.namelist():
            if re.match(r"^.*clusterserviceversion.yaml", filename):
                with z.open(filename) as csv_file:
                    if csv:
                        raise ElliottFatalError(f"found more than one CSV in {bundle_build['nvr']}?!? {filename}")
                    csv = yaml.safe_load(csv_file)

    if not csv:
        raise ElliottFatalError(f"could not find the csv for bundle {bundle_build['nvr']}")
    return csv


def _get_shipped_images(runtime: Runtime, brew_session):
    # retrieve all image builds ever shipped for this version (potential operands)
    # NOTE: this will tend to be the slow part, aside from querying ET
    tags = {f"{image.branch()}-container-released" for image in runtime.image_metas()}
    released = brew.get_tagged_builds(
        [(tag, None) for tag in tags], build_type="image", event=None, session=brew_session
    )
    released = brew.get_build_objects([b["build_id"] for b in released], session=brew_session)
    return [b for b in released if _is_image(b)]  # filter out source images


def _extract_available_image_shasums(image_builds):
    # get shasums for all attached or released images
    image_digests = set()
    for img in image_builds:
        for pullspec in img["extra"]["image"]["index"]["pull"]:
            if "@sha256:" in pullspec:
                image_digests.add(pullspec.split("@")[1])
    return image_digests


def _missing_references(
    runtime, bundles, available_shasums, omit_attached, gather_dependencies
) -> (Set[str], Dict[int, List]):
    # check that bundle references are all either shipped or shipping,
    # and that they will/did ship to the right repo on the registry
    references = [
        [ref["image"], build]  # ref => the bundle build that references it
        for build in bundles
        for ref in build["csv"]["spec"]["relatedImages"]
    ]
    green_print(f"Found {len(bundles)} bundles with {len(references)} references")
    problems = set()
    unshipped_builds_by_advisory = {}
    for image_pullspec, build in references:
        # validate an image reference from a bundle is shipp(ed/ing) to the right repo
        repo, digest = image_pullspec.rsplit("@", 1)  # split off the @sha256:...
        _, repo = repo.split("/", 1)  # pick off the registry
        ref = image_pullspec.rsplit("/", 1)[1]  # just need the name@digest
        nvr = _nvr_for_operand_pullspec(runtime, ref)  # convert ref to nvr
        context = f"Bundle {build['nvr']} reference {nvr}:\n   "

        try:
            attached_advisories = _get_attached_advisory_ids(nvr)
        except BrewBuildException as ex:
            # advisory lookup for brew build failed, count as a problem
            red_print(f"{context} failed to look up in errata-tool: {ex}")
            problems.add(nvr)
            continue

        cdn_repos = _get_cdn_repos(attached_advisories, nvr)

        if digest not in available_shasums and not attached_advisories:
            red_print(f"{context} not shipped or attached to any advisory.")
            unshipped_builds_by_advisory.setdefault(None, []).append(nvr)
        elif not cdn_repos:
            red_print(f"{context} does not have any CDN repos on advisory it is attached to")
            problems.add(nvr)
        elif repo not in cdn_repos:
            red_print(f"{context} needs CDN repo '{repo}' but advisory only has {cdn_repos}")
            problems.add(nvr)
        elif digest in available_shasums:
            green_print(f"{context} shipped/shipping as {image_pullspec}")
        elif gather_dependencies or omit_attached:
            # not already shipped (or cmdline omitted shipped), nor in a listed advisory;
            # if we passed above gates, it is attached to some other advisory;
            # but cmdline option says to count that as missing.
            red_print(f"{context} only attached to separate advisory {attached_advisories}")
            for advisory in attached_advisories:
                unshipped_builds_by_advisory.setdefault(advisory, []).append(nvr)
        else:
            green_print(f"{context} attached to separate advisory {attached_advisories}")

    return problems, unshipped_builds_by_advisory


def _handle_missing_builds(
    ctx: click.Context, gather_dependencies: bool, advisories: Tuple[int, ...], builds_by_advisory: Dict[int, List]
) -> (bool, bool):
    """
    If the only problems were builds not being attached to the right advisories, either report what
    needs to change, or perform the change.
    params:
        :ctx: Click context for running find-builds if new attachments needed
        :gather_dependencies: If true, (re-)attach builds to the metadata advisory
        :advisories: List of advisories given on cmdline
        :builds_by_advisory: Per advisory id, the list of Build objects that are attached
    returns:
        :changes_needed: True if there were builds needing to be (re-)attached
        :changes_made: True if gather_dependencies caused builds to actually be (re-)attached
    """

    missing = set(builds_by_advisory.keys()) - set(advisories)
    if not missing:
        return False, False  # all found in the given advisories; nothing to do

    if gather_dependencies:
        # attach everything to our one advisory
        target = advisories[0]
        print(f"Automatically attaching builds not already attached to {target}:")
        for adv in missing:
            missing_nvrs = builds_by_advisory[adv]
            if adv:
                attached_builds = [b for b in errata.get_brew_builds(adv) if b.nvr in missing_nvrs]
                move_builds(attached_builds, "image", adv, target)
            else:
                # attaching builds from scratch is complicated; call out to existing cli
                ctx.invoke(
                    find_builds_cli,
                    advisory_id=target,
                    default_advisory_type=None,
                    builds=missing_nvrs,
                    kind="image",
                    as_json=False,
                    no_cdn_repos=True,
                    payload=False,
                    non_payload=False,
                    include_shipped=False,
                    member_only=False,
                )
        return True, True
    else:
        print(
            "To automatically attach/move builds not already in the specified advisory, run with --gather-dependencies"
        )
        return True, False


def _nvr_for_operand_pullspec(runtime, spec):
    # spec should just be the part after the final "/" e.g. "ose-kube-rbac-proxy@sha256:9211b70..."
    # we can look it up in the internal proxy.
    urls = runtime.group_config.urls
    spec = f"{urls.brew_image_host}/{urls.brew_image_namespace}/openshift-{spec}"
    info = exectools.cmd_assert(
        f"oc image info -o json --filter-by-os=linux/amd64 {spec}",
        retries=3,
        pollrate=5,
    )[0]
    labels = json.loads(info)["config"]["config"]["Labels"]
    return f"{labels['com.redhat.component']}-{labels['version']}-{labels['release']}"


def _get_attached_advisory_ids(nvr):
    return set(ad["id"] for ad in brew.get_brew_build(nvr=nvr).all_errata if ad["status"] != "DROPPED_NO_SHIP")


def _get_cdn_repos(attached_advisories, for_nvr):
    return set(
        cdn_repo
        for ad_id in attached_advisories
        for nvr, cdn_entry in errata.get_cached_image_cdns(ad_id).items()
        for cdn_repo in cdn_entry["docker"]["target"]["external_repos"]
        if nvr == for_nvr
    )
