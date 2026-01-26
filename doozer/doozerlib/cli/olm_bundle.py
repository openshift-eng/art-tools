import os
import sys
import traceback
from datetime import datetime, timezone
from typing import Optional, Tuple
from urllib.parse import urlparse

import click
import dateutil.parser
import koji
from artcommonlib import exectools, logutil
from artcommonlib.constants import BREW_HUB
from artcommonlib.git_helper import gather_git
from artcommonlib.konflux.konflux_build_record import Engine, KonfluxBuildOutcome, KonfluxBundleBuildRecord
from artcommonlib.model import Missing
from artcommonlib.util import convert_remote_git_to_https
from dockerfile_parse import DockerfileParser

from doozerlib import Runtime, brew
from doozerlib.cli import cli, pass_runtime
from doozerlib.olm.bundle import OLMBundle

LOGGER = logutil.get_logger(__name__)


@cli.command("olm-bundle:list-olm-operators", short_help="List all images that are OLM operators")
@pass_runtime
def list_olm_operators(runtime: Runtime):
    """
    Example:
    $ doozer --group openshift-4.5 olm-bundle:list-olm-operators
    """
    runtime.initialize(clone_distgits=False)

    for image in runtime.image_metas():
        if image.enabled and image.config["update-csv"] is not Missing:
            print(image.get_component_name())


@cli.command(
    "olm-bundle:print",
    short_help="Print data for each operator",
    context_settings=dict(
        ignore_unknown_options=True,  # permit patterns starting with dash; allows printing yaml lists
    ),
)
@click.option(
    "--skip-missing",
    default=False,
    is_flag=True,
    help="If no build has been performed, skip printing pattern that would require it",
)
@click.argument("pattern", default="{component}", nargs=1)
@pass_runtime
def olm_bundles_print(runtime: Runtime, skip_missing, pattern: Optional[str]):
    """
    Prints data from each distgit. The pattern specified should be a string
    with replacement fields:

    \b
    {distgit_key} - The distgit key of the operator
    {component} - The component identified in the Dockerfile
    {nvr} - NVR of latest operator build
    {bundle_component} - The operator's bundle component name
    {paired_bundle_nvr} - NVR of bundle associated with the latest operator NVR (may not exist)
    {paired_bundle_pullspec} - The pullspec for the bundle associated with the latest operator NVR (may not exist)
    {bundle_nvr} - NVR of latest operator bundle build
    {bundle_pullspec} - The pullspec for the latest build
    {lf} - Line feed

    If pattern contains no braces, it will be wrapped with them automatically. For example:
    "component" will be treated as "{component}"
    """

    runtime.initialize(config_only=True)
    clone_distgits = bool(runtime.group_config.canonical_builders_from_upstream)
    runtime.initialize(clone_distgits=clone_distgits)

    # If user omitted braces, add them.
    if "{" not in pattern:
        pattern = "{%s}" % pattern.strip()

    for image in runtime.ordered_image_metas():
        if not image.enabled or image.config["update-csv"] is Missing:
            continue
        s = pattern
        s = s.replace("{lf}", "\n")
        s = s.replace("{distgit_key}", image.distgit_key)
        s = s.replace("{component}", image.get_component_name())

        build_info = image.get_latest_brew_build(default=None)
        if build_info is None:
            if "{" in s:
                if skip_missing:
                    runtime.logger.warning(
                        f"No build has been performed for {image.distgit_key} by {s} requires it; skipping"
                    )
                    continue
                else:
                    raise IOError(
                        f"Fields remaining in pattern, but no build was found for {image.distgit_key} with which to populate those fields: {s}"
                    )
        else:
            nvr = build_info["nvr"]
            olm_bundle = OLMBundle(runtime, build_info, dry_run=False)
            s = s.replace("{nvr}", nvr)
            s = s.replace("{bundle_component}", olm_bundle.bundle_brew_component)
            bundle_image_name = olm_bundle.bundle_image_name

            if "{paired_" in s:
                # Paired bundle values must correspond exactly to the latest operator NVR.
                paired_bundle_nvr = olm_bundle.find_bundle_image()
                if not paired_bundle_nvr:
                    paired_bundle_nvr = "None"  # Doesn't exist
                    paired_pullspec = "None"
                else:
                    paired_version_release = paired_bundle_nvr[len(olm_bundle.bundle_brew_component) + 1 :]
                    paired_pullspec = runtime.resolve_brew_image_url(f"{bundle_image_name}:{paired_version_release}")
                s = s.replace("{paired_bundle_nvr}", paired_bundle_nvr)
                s = s.replace("{paired_bundle_pullspec}", paired_pullspec)

            if "{bundle_" in s:
                # Unpaired is just whatever bundle build was most recent
                build = olm_bundle.find_bundle_image()
                if not build:
                    bundle_nvr = "None"
                    bundle_pullspec = "None"
                else:
                    bundle_nvr = build
                    version_release = bundle_nvr[len(olm_bundle.bundle_brew_component) + 1 :]
                    # Build pullspec like: registry-proxy.engineering.redhat.com/rh-osbs/openshift-ose-clusterresourceoverride-operator-bundle:v4.7.0.202012082225.p0-1
                    bundle_pullspec = runtime.resolve_brew_image_url(f"{bundle_image_name}:{version_release}")

                s = s.replace("{bundle_nvr}", bundle_nvr)
                s = s.replace("{bundle_pullspec}", bundle_pullspec)

        if "{" in s:
            raise IOError("Unrecognized fields remaining in pattern: %s" % s)

        click.echo(s)


@cli.command("olm-bundle:rebase-and-build", short_help="Rebase and build the bundle image for a given operator image")
@click.argument("operator_nvrs", nargs=-1, required=False)
@click.option(
    "-f",
    "--force",
    required=False,
    is_flag=True,
    help="Perform a build even if previous bundles for given NVRs already exist",
)
@click.option(
    "--dry-run",
    default=False,
    is_flag=True,
    help="Do not push to distgit or build anything, but print what would be done.",
)
@pass_runtime
def rebase_and_build_olm_bundle(runtime: Runtime, operator_nvrs: Tuple[str, ...], force: bool, dry_run: bool):
    """Rebase and build operator bundles.

    Run this command with operator NVRs to build bundles for the given operator builds.
    Run this command without operator NVRs to build bundles for operator NVRs selected by the runtime assembly.

    If `--force` option is not specified and there's already a bundle build for given operator builds, this command will do nothing but just print the most recent bundle NVR of that operator.

    Examples:
    $ doozer --group openshift-4.9 --assembly art3171 olm-bundle:rebase-and-build
    $ doozer --group openshift-4.9 --assembly art3171 -i cluster-nfd-operator,ptp-operator olm-bundle:rebase-and-build
    $ doozer --group openshift-4.2 olm-bundle:rebase-and-build \
        sriov-network-operator-container-v4.2.30-202004200449 \
        elasticsearch-operator-container-v4.2.30-202004240858 \
        cluster-logging-operator-container-v4.2.30-202004240858
    """

    if runtime.images and operator_nvrs:
        raise click.BadParameter("Do not specify operator NVRs when --images is specified")

    operator_builds = []
    if operator_nvrs:
        LOGGER.info("Fetching given nvrs from brew...")
        brew_session = koji.ClientSession(BREW_HUB)
        images = []
        for index, build in enumerate(brew.get_build_objects(operator_nvrs, brew_session)):
            if not build:
                raise IOError(f"Build {operator_nvrs[index]} doesn't exist in Brew.")
            source_url = urlparse(build["source"])
            image_name = source_url.path.rstrip("/").rsplit("/")[-1]
            images.append(image_name)
            operator_builds.append(build)
        runtime.images = images
        runtime.initialize(clone_distgits=False)
    else:
        # We initialize with config_only first to check if we need to clone distgits or not
        # Cloning distgits is necessary for meta.get_latest_build() to work correctly
        runtime.initialize(config_only=True)
        clone_distgits = bool(runtime.group_config.canonical_builders_from_upstream)
        runtime.initialize(clone_distgits=clone_distgits)

        # Since no operator nvs are explicitly given
        # fetch all latest operator builds from Brew
        # This will respect --images if specified
        operator_metas = [
            meta for meta in runtime.ordered_image_metas() if meta.enabled and meta.config["update-csv"] is not Missing
        ]
        results = exectools.parallel_exec(lambda meta, _: meta.get_latest_brew_build(), operator_metas)
        operator_builds = results.get()

    def rebase_and_build(olm_bundle: OLMBundle):
        record = {
            "status": -1,
            "task_id": "",
            "task_url": "",
            "operator_nvr": "",
            "bundle_nvr": "",
            "message": "Unknown failure",
        }
        operator_nvr = olm_bundle.operator_nvr
        try:
            record["operator_nvr"] = operator_nvr
            if not force:
                runtime.logger.info("%s - Finding most recent bundle build", operator_nvr)
                bundle_nvr = olm_bundle.find_bundle_image()
                if bundle_nvr:
                    runtime.logger.info("%s - Found bundle build %s", operator_nvr, bundle_nvr)
                    record["status"] = 0
                    record["message"] = "Already built"
                    record["bundle_nvr"] = bundle_nvr
                    return record
                runtime.logger.info("%s - No bundle build found", operator_nvr)
            LOGGER.info("%s - Rebasing bundle distgit repo", operator_nvr)
            olm_bundle.rebase()
            LOGGER.info("%s - Building bundle distgit repo", operator_nvr)
            task_id, task_url, build_info = olm_bundle.build()
            record["status"] = 0
            record["message"] = "Success"
            record["task_id"] = task_id
            record["task_url"] = task_url
            record["bundle_nvr"] = build_info["nvr"]
            update_konflux_db(olm_bundle, record, build_info)

        except Exception as err:
            traceback.print_exc()
            LOGGER.error("Error during rebase or build for: {}".format(operator_nvr))
            record["message"] = str(err)

        finally:
            runtime.record_logger.add_record("build_olm_bundle", **record)
            return record

    def update_konflux_db(olm_bundle, record, build_info):
        if dry_run:
            return

        if not runtime.konflux_db:
            LOGGER.warning("Konflux DB connection is not initialized, not writing build record to the Konflux DB.")
            return

        runtime.konflux_db.bind(KonfluxBundleBuildRecord)
        try:
            dfp = DockerfileParser(f"{olm_bundle.bundle_clone_path}/Dockerfile")
            source_repo, commitish = dfp.labels["io.openshift.build.commit.url"].split("/commit/")
            _, rebase_repo_url, _ = gather_git(["-C", olm_bundle.bundle_clone_path, "remote", "get-url", "origin"])
            _, rebase_commitish, _ = gather_git(["-C", olm_bundle.bundle_clone_path, "rev-parse", "HEAD"])

            build_record_params = {
                "name": olm_bundle.bundle_name,
                "group": runtime.group,
                "assembly": runtime.assembly,
                "source_repo": source_repo,
                "commitish": commitish,
                "rebase_repo_url": convert_remote_git_to_https(rebase_repo_url),
                "rebase_commitish": rebase_commitish.strip(),
                "engine": Engine.BREW,
                "art_job_url": os.getenv("BUILD_URL", "n/a"),
                "build_pipeline_url": record["task_url"],
                "pipeline_commit": "n/a",
                "operator_nvr": olm_bundle.operator_dict["nvr"],
            }

            if record["status"] == 0:
                image_pullspec = build_info["extra"]["image"]["index"]["pull"][0]

                build_record_params.update(
                    {
                        "version": build_info["version"],
                        "release": build_info["release"],
                        "start_time": datetime.strptime(build_info["start_time"], "%Y-%m-%d %H:%M:%S.%f"),
                        "end_time": datetime.strptime(build_info["completion_time"], "%Y-%m-%d %H:%M:%S.%f"),
                        "image_pullspec": image_pullspec,
                        "image_tag": build_info["extra"]["image"]["index"]["tags"][0],
                        "operand_nvrs": [v for _, v in olm_bundle.found_image_references.items()],
                        "build_id": str(build_info["id"]),
                        "outcome": KonfluxBuildOutcome.SUCCESS,
                        "nvr": record["bundle_nvr"],
                    }
                )

            else:
                with runtime.shared_koji_client_session() as api:
                    task_info = api.getTaskInfo(record["task_id"])

                task_end_time = task_info["completion_time"]
                task_start_time = task_info["create_time"]

                build_record_params.update(
                    {
                        "outcome": KonfluxBuildOutcome.FAILURE,
                        "start_time": dateutil.parser.parse(task_start_time).replace(tzinfo=timezone.utc),
                        "end_time": dateutil.parser.parse(task_end_time).replace(tzinfo=timezone.utc),
                        "nvr": "n/a",
                    }
                )

            build_record = KonfluxBundleBuildRecord(**build_record_params)
            runtime.konflux_db.add_build(build_record)
            LOGGER.info("Brew build info stored successfully")

        except Exception:
            LOGGER.exception("Failed writing record to the konflux DB")

    olm_bundles = [OLMBundle(runtime, op, dry_run=dry_run) for op in operator_builds]
    get_branches_results = [bundle.does_bundle_branch_exist() for bundle in olm_bundles]
    if not all([result[0] for result in get_branches_results]):
        LOGGER.error(
            "One or more bundle branches do not exist: "
            f"{[result[1] for result in get_branches_results if not result[0]]}. Please create them first."
        )
        sys.exit(1)

    results = exectools.parallel_exec(lambda bundle, _: rebase_and_build(bundle), olm_bundles).get()

    for record in results:
        if record["status"] == 0:
            LOGGER.info("Successfully built %s", record["bundle_nvr"])
            click.echo(record["bundle_nvr"])
        else:
            LOGGER.error("Error building bundle for %s: %s", record["operator_nvr"], record["message"])

    rc = 0 if all(map(lambda i: i["status"] == 0, results)) else 1

    if rc:
        LOGGER.error("One or more bundles failed")

    sys.exit(rc)
