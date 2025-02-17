import asyncio
import functools
import json
import logging
import re
import sys
from typing import Dict, List, Set, Union

import click
import koji
import requests
from errata_tool import ErrataException

from artcommonlib import logutil, exectools
from artcommonlib.arch_util import BREW_ARCHES
from artcommonlib.assembly import assembly_rhcos_config, assembly_metadata_config
from artcommonlib.format_util import red_print, green_prefix, green_print, yellow_print
from artcommonlib.rhcos import get_container_configs, get_build_id_from_rhcos_pullspec
from artcommonlib.release_util import isolate_el_version_in_release
from artcommonlib.rpm_utils import parse_nvr
from elliottlib import Runtime, brew, errata
from elliottlib.build_finder import BuildFinder
from elliottlib.cli.common import (cli, find_default_advisory,
                                   use_default_advisory_option, click_coroutine)
from elliottlib.exceptions import ElliottFatalError
from elliottlib.imagecfg import ImageMetadata
from elliottlib.util import (ensure_erratatool_auth,
                             get_release_version,
                             isolate_el_version_in_brew_tag,
                             parallel_results_with_progress, pbar_header, progress_func)

LOGGER = logutil.get_logger(__name__)

pass_runtime = click.make_pass_decorator(Runtime)


@cli.command('find-builds', short_help='Find or attach builds to ADVISORY')
@click.option(
    '--attach', '-a', 'advisory_id',
    type=int, metavar='ADVISORY',
    help='Attach the builds to ADVISORY (by default only a list of builds are displayed)')
@use_default_advisory_option
@click.option(
    "--builds-file", "-f", "builds_file",
    help="File to read builds from, `-` to read from STDIN.",
    type=click.File("rt"),
)
@click.option(
    '--build', '-b', 'builds',
    multiple=True, metavar='NVR_OR_ID',
    help='Add build NVR_OR_ID to ADVISORY [MULTIPLE]')
@click.option(
    '--kind', '-k', metavar='KIND', required=True,
    type=click.Choice(['rpm', 'image']),
    help='Find builds of the given KIND [rpm, image]')
@click.option(
    '--json', 'as_json', metavar='FILE_NAME',
    help='Dump new builds as JSON array to a file (or "-" for stdout)')
@click.option(
    '--no-cdn-repos', required=False, is_flag=True,
    help='Do not configure CDN repos after attaching images (default to False)')
@click.option(
    '--payload', required=False, is_flag=True,
    help='Only attach payload images')
@click.option(
    '--non-payload', required=False, is_flag=True,
    help='Only attach non-payload images')
@click.option(
    '--include-shipped', required=False, is_flag=True,
    help='Do not filter out shipped builds')
@click.option(
    '--member-only', is_flag=True,
    help='(For rpms) Only sweep member rpms')
@click.option('--clean', is_flag=True,
              help='Remove builds from advisory that were not found in build sweep. Cannot be used with -b or -f')
@click.option('--dry-run', '--noop', is_flag=True,
              help='Do not attach/remove builds from advisory, only show what would be done')
@click_coroutine
@pass_runtime
# # # NOTE: if you change the method signature, be aware that verify_attached_operators_cli.py # # #
# # # invokes find_builds_cli so please avoid breaking it.                                     # # #
async def find_builds_cli(runtime: Runtime, advisory_id, default_advisory_type, builds_file, builds, kind, as_json,
                          no_cdn_repos, payload, non_payload, include_shipped, member_only: bool, clean: bool, dry_run: bool):
    """Automatically or manually find or attach viable rpm or image builds
to ADVISORY. Default behavior searches Brew for viable builds in the
given group. Provide builds manually by giving one or more --build
(-b) options. Manually provided builds are verified against the Errata
Tool API.

\b
  * Attach the builds to ADVISORY by giving --attach
  * Specify the build type using --kind KIND

Example: Assuming --group=openshift-3.7, then a build is a VIABLE
BUILD IFF it meets ALL of the following criteria:

\b
  * HAS the tag in brew: rhaos-3.7-rhel7-candidate
  * DOES NOT have the tag in brew: rhaos-3.7-rhel7
  * IS NOT attached to ANY existing RHBA, RHSA, or RHEA

That is to say, a viable build is tagged as a "candidate", has NOT
received the "shipped" tag yet, and is NOT attached to any PAST or
PRESENT advisory. Here are some examples:

    SHOW the latest OSE 3.6 image builds that would be attached to a
    3.6 advisory:

    $ elliott --group openshift-3.6 find-builds -k image

    ATTACH the latest OSE 3.6 rpm builds to advisory 123456:

\b
    $ elliott --group openshift-3.6 find-builds -k rpm --attach 123456

    VERIFY (no --attach) that the manually provided RPM NVR and build
    ID are viable builds:

    $ elliott --group openshift-3.6 find-builds -k rpm -b megafrobber-1.0.1-2.el7 -a 93170
"""

    if advisory_id and default_advisory_type:
        raise click.BadParameter('Use only one of --use-default-advisory or --attach')
    if payload and non_payload:
        raise click.BadParameter('Use only one of --payload or --non-payload.')
    if builds and builds_file:
        raise click.BadParameter('Use only one of --build or --builds-file.')
    if clean:
        if not (advisory_id or default_advisory_type):
            raise click.BadParameter('Cannot use --clean without --attach or --use-default-advisory.')
        if builds or builds_file:
            raise click.BadParameter('Cannot use --clean with --build or --builds-file.')

    if builds_file:
        if builds_file == "-":
            builds_file = sys.stdin
        builds = [line.strip() for line in builds_file.readlines()]

    runtime.initialize(mode='images' if kind == 'image' else 'rpms')
    replace_vars = runtime.group_config.vars.primitive() if runtime.group_config.vars else {}
    et_data = runtime.get_errata_config(replace_vars=replace_vars)
    tag_pv_map = et_data.get('brew_tag_product_version_mapping')

    if default_advisory_type is not None:
        advisory_id = find_default_advisory(runtime, default_advisory_type)

    ensure_erratatool_auth()  # before we waste time looking up builds we can't process

    nvrps = []

    # get the builds we want to add
    brew_session = runtime.build_retrying_koji_client(caching=True)
    if builds:
        LOGGER.info("Fetching builds from Brew")
        nvrps = _fetch_nvrps_by_nvr_or_id(builds, tag_pv_map, include_shipped=include_shipped,
                                          brew_session=brew_session)
    else:
        if kind == 'image':
            nvrps = await _fetch_builds_by_kind_image(runtime, tag_pv_map, brew_session, payload,
                                                      non_payload, include_shipped)
            if payload:
                rhcos_nvrs = get_rhcos_nvrs_from_assembly(runtime, brew_session)
                rhcos_nvrps = _fetch_nvrps_by_nvr_or_id(rhcos_nvrs, tag_pv_map, include_shipped=include_shipped,
                                                        brew_session=brew_session)
                nvrps.extend(rhcos_nvrps)
        elif kind == 'rpm':
            nvrps = await _fetch_builds_by_kind_rpm(runtime, tag_pv_map, brew_session, include_shipped, member_only)

    LOGGER.info('Fetching info for builds from Errata')
    builds: List[brew.Build] = parallel_results_with_progress(
        nvrps,
        lambda nvrp: errata.get_brew_build(f'{nvrp[0]}-{nvrp[1]}-{nvrp[2]}',
                                           nvrp[3], session=requests.Session())
    )

    _json_dump(as_json, builds, kind, tag_pv_map)
    canonical_nvrs = [b.nvr for b in builds]

    # if we want to attach found builds to an advisory -> filter out already attached builds
    # if we want to report on found builds -> do not filter out
    if advisory_id:
        previous = len(builds)
        builds, attached_to_advisories = _filter_out_attached_builds(builds, include_shipped)
        if len(builds) != previous:
            for attached_ad_id, nvrs in attached_to_advisories.items():
                if attached_ad_id == advisory_id:
                    LOGGER.info(f'{len(nvrs)} builds are already attached to given advisory')
                else:
                    LOGGER.warning(f'Cannot attach {len(nvrs)} build(s), since they are already attached to '
                                   f'ART advisory {attached_ad_id} - {sorted(nvrs)}. Remove them from the advisory '
                                   'and then try again.')

    if not advisory_id:
        LOGGER.info(f'Found {len(builds)} builds')
        for b in sorted(builds):
            click.echo(' ' + b.nvr)
        return

    try:
        LOGGER.info("Fetching advisory")
        erratum = errata.Advisory(errata_id=advisory_id)

        # store nvrs that are already attached to the advisory
        advisory_build_nvrs = []
        for build_list in erratum.errata_builds.values():  # one per product version
            advisory_build_nvrs.extend(build_list)

        if not builds:
            green_print("No new builds found for attaching to advisory")
        elif dry_run:
            yellow_print("[dry-run] Would've moved advisory to NEW_FILES state")
            yellow_print(f"[dry-run] Would've attached {len(builds)} builds to advisory {advisory_id}")
        else:
            erratum.ensure_state('NEW_FILES')
            erratum.attach_builds(builds, kind)

        if clean:
            nvrs_to_remove = set(advisory_build_nvrs) - set(canonical_nvrs)

            # In ET, new nvr replaces already attached nvr for a (package, el_version) in a product version.
            # so filter out nvrs that would be replaced by canonical nvrs
            valid_package_els = set()
            for n in canonical_nvrs:
                parsed_n = parse_nvr(n)
                n_el = isolate_el_version_in_release(parsed_n['release'])
                valid_package_els.add((parsed_n['name'], n_el))

            temp = []
            for n in nvrs_to_remove:
                parsed_n = parse_nvr(n)
                n_el = isolate_el_version_in_release(parsed_n['release'])
                if (parsed_n['name'], n_el) in valid_package_els:
                    temp.append(n)

            nvrs_to_remove = nvrs_to_remove - set(temp)
            if nvrs_to_remove:
                LOGGER.info(f"Removing builds from advisory that were not found in build sweep: {len(nvrs_to_remove)}")
                for b in sorted(nvrs_to_remove):
                    click.echo(' ' + b)
                if dry_run:
                    yellow_print("[dry-run] Would've moved advisory to NEW_FILES state")
                    yellow_print(f"[dry-run] Would've removed {len(nvrs_to_remove)} builds from advisory {advisory_id}")
                else:
                    erratum.ensure_state('NEW_FILES')
                    erratum.remove_builds(list(nvrs_to_remove))

        if not builds:
            return

        cdn_repos = et_data.get('cdn_repos')
        if kind == 'image':
            if dry_run:
                yellow_print("[dry-run] Would've ensured RHCOS file metadata is set")
            else:
                ensure_rhcos_file_meta(advisory_id)
            if cdn_repos and not no_cdn_repos:
                cdn_repos = set(cdn_repos)
                if dry_run:
                    yellow_print("[dry-run] Would've fetched and validated cdn repos for advisory {advisory_id}.")
                    return
                available_repos = set([i['repo']['name'] for i in erratum.metadataCdnRepos()])
                not_available_repos = cdn_repos - available_repos
                repos_to_enable = cdn_repos & available_repos
                if repos_to_enable:
                    if dry_run:
                        yellow_print(f"[dry-run] Would've enabled CDN repos: {repos_to_enable}")
                    else:
                        erratum.set_cdn_repos(repos_to_enable)
                if not_available_repos:
                    raise ValueError("These cdn repos defined in erratatool.yml are not available for the advisory "
                                     f"{advisory_id}: {not_available_repos}. Please remove these or request them to "
                                     "be created.")
    except ErrataException as e:
        red_print(f'Cannot change advisory {advisory_id}: {e}')
        exit(1)


def get_rhcos_nvrs_from_assembly(runtime: Runtime, brew_session: koji.ClientSession = None):
    rhcos_config = assembly_rhcos_config(runtime.get_releases_config(), runtime.assembly)
    build_ids_by_arch = dict()
    nvrs = []

    # Keys under rhcos_config are not necessary payload tags. One exception is `dependencies`
    # make sure we only process payload tags
    rhcos_payload_tags = [c['name'] for c in get_container_configs(runtime)]
    for key, config in rhcos_config.items():
        if key not in rhcos_payload_tags:
            continue

        for arch, pullspec in config['images'].items():
            build_id = get_build_id_from_rhcos_pullspec(pullspec)
            if arch not in build_ids_by_arch:
                build_ids_by_arch[arch] = set()
            build_ids_by_arch[arch].add(build_id)

    for arch, builds in build_ids_by_arch.items():
        for build_id in builds:
            nvr = f'rhcos-{arch}-{build_id}'
            if brew_session.getBuild(nvr):
                LOGGER.info(f'Found rhcos nvr: {nvr}')
                nvrs.append(nvr)
            else:
                LOGGER.warning(f'rhcos nvr not found: {nvr}')
    return nvrs


def ensure_rhcos_file_meta(advisory_id):
    # this assumes that the advisory is in NEW_FILES state

    file_meta = errata.get_file_meta(advisory_id)
    rhcos_file_meta = []
    for f in file_meta:
        # rhcos artifact file path is something like
        # `/mnt/redhat/brewroot/packages/rhcos-x86_64/413.92.202307260246/0/images
        # /coreos-assembler-git.tar.gz`

        # title will be None if it isn't set
        # skip if it is set
        # skip if it is not an rhcos file
        if f['title'] or 'rhcos' not in f['file']['path']:
            continue

        arch = next((a for a in BREW_ARCHES if a in f['file']['path']), None)
        if not arch:
            raise ValueError(f'Unable to determine arch from rhcos file path: {f["file"]["path"]}. Please investigate.')

        title = f'RHCOS Image metadata ({arch})'
        rhcos_file_meta.append({'file': f['file']['id'], 'title': title})
    if rhcos_file_meta:
        errata.put_file_meta(advisory_id, rhcos_file_meta)


def _fetch_nvrps_by_nvr_or_id(ids_or_nvrs, tag_pv_map, include_shipped=False, ignore_product_version=False,
                              brew_session: koji.ClientSession = None):
    builds = brew.get_build_objects(ids_or_nvrs, brew_session)
    nonexistent_builds = list(filter(lambda b: b[1] is None, zip(ids_or_nvrs, builds)))
    if nonexistent_builds:
        raise ValueError("The following builds are not found in Brew: "
                         f"{' '.join(map(lambda b: b[0], nonexistent_builds))}")

    _ensure_accepted_tags(builds, brew_session, tag_pv_map)
    shipped = set()
    if include_shipped:
        LOGGER.info("Do not filter out shipped builds, all builds will be attached")
    else:
        LOGGER.info("Filtering out shipped builds")
        shipped = _find_shipped_builds([b["id"] for b in builds], brew_session)
    unshipped = [b for b in builds if b["id"] not in shipped]
    LOGGER.info(f'Found {len(shipped) + len(unshipped)} builds, of which {len(unshipped)} are new.')

    nvrps = []
    if ignore_product_version:
        for build in unshipped:
            nvrps.append((build["name"], build["version"], build["release"], None))
        return nvrps
    for build in unshipped:
        product_versions = {pv for tag, pv in tag_pv_map.items() if tag in build["_tags"]}
        if not product_versions:
            raise ValueError(f"Build {build['nvr']} doesn't have any of the following whitelisted tags: {list(tag_pv_map.keys())}")
        for pv in product_versions:
            nvrps.append((build["name"], build["version"], build["release"], pv))
    return nvrps


def _gen_nvrp_tuples(builds: List[Dict], tag_pv_map: Dict[str, str]):
    """Returns a list of (name, version, release, product_version) tuples of each build """
    nvrps = [(b['name'], b['version'], b['release'], tag_pv_map[b['tag_name']]) for b in builds]
    return nvrps


def _json_dump(as_json, unshipped_builds, kind, tag_pv_map):
    if as_json:
        builds = []
        tags = []
        reversed_tag_pv_map = {y: x for x, y in tag_pv_map.items()}
        for b in sorted(unshipped_builds):
            builds.append(b.nvr)
            tags.append(reversed_tag_pv_map[b.product_version])
        json_data = dict(builds=builds, base_tag=tags, kind=kind)
        if as_json == '-':
            click.echo(json.dumps(json_data, indent=4, sort_keys=True))
        else:
            with open(as_json, 'w') as json_file:
                json.dump(json_data, json_file, indent=4, sort_keys=True)


def _find_shipped_builds(build_ids: List[Union[str, int]], brew_session: koji.ClientSession) -> Set[Union[str, int]]:
    """ Finds shipped builds
    :param builds: list of Brew build IDs or NVRs
    :param brew_session: Brew session
    :return: a set of shipped Brew build IDs or NVRs
    """
    shipped_ids = set()
    tag_lists = brew.get_builds_tags(build_ids, brew_session)
    released_tag_pattern = re.compile(r"^RH[BSE]A-.+-released$")  # https://issues.redhat.com/browse/ART-3277
    for build_id, tags in zip(build_ids, tag_lists):
        # a shipped build with OCP Errata should have a Brew tag ending with `-released`, like `RHBA-2020:2713-released`
        shipped = any(map(lambda tag: released_tag_pattern.match(tag["name"]), tags))
        if shipped:
            shipped_ids.add(build_id)
    return shipped_ids


async def _fetch_builds_by_kind_image(runtime: Runtime, tag_pv_map: Dict[str, str],
                                      brew_session: koji.ClientSession, payload_only: bool, non_payload_only: bool, include_shipped: bool):
    image_metas: List[ImageMetadata] = []
    for image in runtime.image_metas():
        if image.base_only or not image.is_release:
            continue
        if (payload_only and not image.is_payload) or (non_payload_only and image.is_payload):
            continue
        image_metas.append(image)

    pbar_header(
        'Generating list of images: ',
        f'Hold on a moment, fetching Brew builds for {len(image_metas)} components...')

    tasks = [exectools.to_thread(
        progress_func,
        functools.partial(image.get_latest_build, el_target=image.branch_el_target())) for image in image_metas]
    brew_latest_builds: List[Dict] = list(await asyncio.gather(*tasks))

    _ensure_accepted_tags(brew_latest_builds, brew_session, tag_pv_map)
    shipped = set()
    if include_shipped:
        click.echo("Do not filter out shipped builds, all builds will be attached")
    else:
        click.echo("Filtering out shipped builds...")
        shipped = _find_shipped_builds([b["id"] for b in brew_latest_builds], brew_session)
    unshipped = [b for b in brew_latest_builds if b["id"] not in shipped]
    click.echo(f'Found {len(shipped)+len(unshipped)} builds, of which {len(unshipped)} are new.')
    nvrps = _gen_nvrp_tuples(unshipped, tag_pv_map)
    return nvrps


def _ensure_accepted_tags(builds: List[Dict], brew_session: koji.ClientSession, tag_pv_map: Dict[str, str], raise_exception: bool = True):
    """
    Build dicts returned by koji.listTagged API have their tag names, however other APIs don't set that field.
    Tag names are required because they are associated with Errata product versions.
    For those build dicts whose tags are unknown, we need to query from Brew.
    """
    builds = [b for b in builds if "tag_name" not in b]  # filters out builds whose accepted tag is already set
    unknown_tags_builds = [b for b in builds if "_tags" not in b]  # finds builds whose tags are not cached
    build_tag_lists = brew.get_builds_tags(unknown_tags_builds, brew_session)
    for build, tags in zip(unknown_tags_builds, build_tag_lists):
        build["_tags"] = {tag['name'] for tag in tags}
    # Finds and sets the accepted tag (rhaos-x.y-rhel-z-[candidate|hotfix]) for each build
    for build in builds:
        accepted_tag = next(filter(lambda tag: tag in tag_pv_map, build["_tags"]), None)
        if not accepted_tag:
            msg = f"Build {build['nvr']} has Brew tags {build['_tags']}, but none of them has an associated Errata product version."
            if raise_exception:
                raise IOError(msg)
            else:
                LOGGER.warning(msg)
                continue
        build["tag_name"] = accepted_tag


async def _fetch_builds_by_kind_rpm(runtime: Runtime, tag_pv_map: Dict[str, str], brew_session: koji.ClientSession, include_shipped: bool, member_only: bool):
    assembly = runtime.assembly
    if runtime.assembly_basis_event:
        LOGGER.info(f'Constraining rpm search to stream assembly due to assembly basis event {runtime.assembly_basis_event}')
        # If an assembly has a basis event, its latest rpms can only be sourced from
        # "is:" or the stream assembly.
        assembly = 'stream'

        # ensures the runtime assembly doesn't include any image member specific or rhcos specific dependencies
        image_configs = [assembly_metadata_config(runtime.get_releases_config(), runtime.assembly, 'image', image.distgit_key, image.config) for _, image in runtime.image_map.items()]
        if any(nvr for image_config in image_configs for dep in image_config.dependencies.rpms for _, nvr in dep.items()):
            raise ElliottFatalError(f"Assembly {runtime.assembly} is not appliable for build sweep because it contains image member specific dependencies for a custom release.")
        rhcos_config = assembly_rhcos_config(runtime.get_releases_config(), runtime.assembly)
        if any(nvr for dep in rhcos_config.dependencies.rpms for _, nvr in dep.items()):
            raise ElliottFatalError(f"Assembly {runtime.assembly} is not appliable for build sweep because it contains RHCOS specific dependencies for a custom release.")

    builds: List[Dict] = []

    pinned_nvrs = set()
    if member_only:  # Sweep only member rpms
        for tag in tag_pv_map:
            tasks = [exectools.to_thread(progress_func, functools.partial(rpm.get_latest_build, default=None, el_target=tag)) for rpm in runtime.rpm_metas()]
            builds_for_tag = await asyncio.gather(*tasks)
            builds.extend(filter(lambda b: b is not None, builds_for_tag))

    else:  # Sweep all tagged rpms
        builder = BuildFinder(brew_session, logger=LOGGER)
        for tag in tag_pv_map:
            # keys are rpm component names, values are nvres
            component_builds: Dict[str, Dict] = builder.from_tag("rpm", tag, inherit=False, assembly=assembly, event=runtime.brew_event)
            if runtime.assembly_basis_event:
                # If an assembly has a basis event, rpms pinned by "is" and group dependencies should take precedence over every build from the tag
                el_version = isolate_el_version_in_brew_tag(tag)
                if not el_version:
                    continue  # Only honor pinned rpms if this tag is relevant to a RHEL version

                # Honors pinned NVRs by "is"
                pinned_by_is = builder.from_pinned_by_is(el_version, runtime.assembly, runtime.get_releases_config(), runtime.rpm_map)
                _ensure_accepted_tags(pinned_by_is.values(), brew_session, tag_pv_map)
                pinned_nvrs.update([b['nvr'] for b in pinned_by_is.values()])

                # Builds pinned by "is" should take precedence over every build from tag
                for component, pinned_build in pinned_by_is.items():
                    if component in component_builds and pinned_build["id"] != component_builds[component]["id"]:
                        LOGGER.warning("Swapping stream nvr %s for pinned nvr %s...", component_builds[component]["nvr"], pinned_build["nvr"])

                component_builds.update(pinned_by_is)  # pinned rpms take precedence over those from tags

                # Honors group dependencies
                group_deps = builder.from_group_deps(el_version, runtime.group_config, runtime.rpm_map)  # the return value doesn't include any ART managed rpms
                # Group dependencies should take precedence over anything previously determined except those pinned by "is".
                for component, dep_build in group_deps.items():
                    if component in component_builds and dep_build["id"] != component_builds[component]["id"]:
                        LOGGER.warning("Swapping stream nvr %s for group dependency nvr %s...", component_builds[component]["nvr"], dep_build["nvr"])
                component_builds.update(group_deps)
                pinned_nvrs.update([b['nvr'] for b in group_deps.values()])
            builds.extend(component_builds.values())

    _ensure_accepted_tags(builds, brew_session, tag_pv_map, raise_exception=False)
    qualified_builds = [b for b in builds if "tag_name" in b]
    not_attachable_nvrs = [b["nvr"] for b in builds if "tag_name" not in b]

    if not_attachable_nvrs:
        LOGGER.info(f"The following NVRs will not be swept because they don't have allowed tags"
                    f" {list(tag_pv_map.keys())}: {not_attachable_nvrs}")

    shipped = set()
    if include_shipped:
        LOGGER.info("Including all builds that may have been shipped previously")
    else:
        LOGGER.info("Filtering out shipped builds - except the ones that have been pinned in the assembly")
        shipped = _find_shipped_builds([b["id"] for b in qualified_builds if b["nvr"] not in pinned_nvrs], brew_session)
    unshipped = [b for b in qualified_builds if b["id"] not in shipped]
    LOGGER.info(f'Found {len(shipped)+len(unshipped)} builds, of which {len(unshipped)} are qualified.')
    nvrps = _gen_nvrp_tuples(unshipped, tag_pv_map)
    nvrps = sorted(set(nvrps))  # remove duplicates
    return nvrps


def _filter_out_attached_builds(build_objects: List[brew.Build], include_shipped: bool = False) -> (List[brew.Build], Dict[int, Set[str]]):
    """
    Filter out builds that are already attached to an ART advisory
    """
    unattached_builds: List[brew.Build] = []
    errata_version_cache = {}  # avoid reloading the same errata for multiple builds
    attached_to_advisories: Dict[int, Set[str]] = dict()
    for b in build_objects:
        # check if build is attached to any existing advisory for this version
        in_same_version = False
        for errata_info in b.all_errata:
            eid = errata_info['id']
            if eid not in errata_version_cache:
                release = errata.get_art_release_from_erratum(eid)
                if not release:
                    # Does not contain ART metadata; consider it unversioned
                    LOGGER.warning("Errata {} Does not contain ART metadata\n".format(eid))
                    errata_version_cache[eid] = ''
                    continue
                errata_version_cache[eid] = release
            if errata_version_cache[eid] == get_release_version(b.product_version):
                # We ship 2 different versions of operator builds, first in preGA advisory
                # and second in GA advisory. We use include_shipped to indicate that we still want to include
                # builds that have shipped before in preGA advisory, in the GA advisory.
                # Errata does not allow attaching a build to 2 pending advisories at the same time
                # So filter if advisory is pending otherwise skip if advisory is shipped
                if include_shipped and errata_info['status'] == "SHIPPED_LIVE":
                    continue
                in_same_version = True
                if eid not in attached_to_advisories:
                    attached_to_advisories[eid] = set()
                attached_to_advisories[eid].add(b.nvr)
                break
        if not in_same_version:
            unattached_builds.append(b)
    return unattached_builds, attached_to_advisories
