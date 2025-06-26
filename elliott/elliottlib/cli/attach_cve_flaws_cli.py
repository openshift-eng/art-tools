import json
import logging
import os
import sys
import traceback
from typing import Dict, Iterable, List, Set, Tuple
from urllib.parse import urlparse

import click
import gitlab
from artcommonlib import arch_util
from artcommonlib.assembly import AssemblyTypes, assembly_config_struct
from artcommonlib.rpm_utils import parse_nvr
from artcommonlib.util import new_roundtrip_yaml_handler
from errata_tool import Erratum

from elliottlib import constants
from elliottlib.bzutil import Bug, BugTracker, get_highest_security_impact, is_first_fix_any, sort_cve_bugs
from elliottlib.cli.common import cli, click_coroutine, find_default_advisory, use_default_advisory_option
from elliottlib.errata import is_security_advisory
from elliottlib.errata_async import AsyncErrataAPI, AsyncErrataUtils
from elliottlib.runtime import Runtime
from elliottlib.shipment_model import CveAssociation, ReleaseNotes, ShipmentConfig
from elliottlib.util import get_advisory_boilerplate

LOGGER = logging.getLogger(__name__)


@cli.command('attach-cve-flaws', short_help='Attach corresponding flaw bugs for trackers in advisory (first-fix only)')
@click.option('--advisory', '-a', 'advisory_id', type=int, help='Find tracker bugs in given advisory')
@click.option(
    "--noop",
    "--dry-run",
    required=False,
    default=False,
    is_flag=True,
    help="Print what would change, but don't change anything",
)
@use_default_advisory_option
@click.option(
    "--into-default-advisories", is_flag=True, help='Run for all advisories values defined in [group|releases].yml'
)
@click.pass_obj
@click_coroutine
async def attach_cve_flaws_cli(
    runtime: Runtime, advisory_id: int, noop: bool, default_advisory_type: str, into_default_advisories: bool
):
    """Attach corresponding flaw bugs for trackers in advisory (first-fix only).

    Also converts advisory to RHSA, if not already.

    Example:

    $ elliott --group openshift-4.6 attach-cve-flaws --use-default-advisory image
    INFO Cloning config data from https://github.com/openshift-eng/ocp-build-data.git
    INFO Using branch from group.yml: rhaos-4.6-rhel-8
    INFO found 114 tracker bugs attached to the advisory
    INFO found 58 corresponding flaw bugs
    INFO 23 out of 58 flaw bugs considered "first-fix"
    INFO Adding the following BZs to the advisory: [1880456, 1858827, 1880460,
    1847310, 1857682, 1857550, 1857551, 1857559, 1848089, 1848092, 1849503,
    1851422, 1866148, 1858981, 1852331, 1861044, 1857081, 1857977, 1848647,
    1849044, 1856529, 1843575, 1840253]
    """

    if sum(map(bool, [advisory_id, default_advisory_type, into_default_advisories])) != 1:
        raise click.BadParameter("Use one of --use-default-advisory or --advisory or --into-default-advisories")
    runtime.initialize()

    if runtime.build_system == 'konflux':
        if into_default_advisories or advisory_id:
            raise click.UsageError(
                "Konflux does not yet support --into-default-advisories or --advisory options, "
                "please use --use-default-advisory instead"
            )

        release_notes = await handle_konflux_cve_flaws(runtime, default_advisory_type)
        click.echo(json.dumps(release_notes.model_dump(mode='json'), indent=4))

    elif runtime.build_system == 'brew':
        await handle_brew_cve_flaws(
            runtime,
            into_default_advisories,
            advisory_id,
            default_advisory_type,
            noop,
        )


async def handle_konflux_cve_flaws(runtime: Runtime, default_advisory_type: str):
    """
    Handle attaching CVE flaws in a Konflux environment.
    """

    # Read shipment block from the assembly config
    assembly_group_config = assembly_config_struct(runtime.get_releases_config(), runtime.assembly, "group", {})
    shipment = assembly_group_config.get("shipment", {})
    advisories = shipment.get("advisories", [])
    if not advisories:
        LOGGER.info("No advisories found in the shipment block, exiting.")
        sys.exit(0)

    # Validate default advisory type
    kinds = [advisory.get("kind") for advisory in advisories]
    if default_advisory_type not in kinds:
        raise click.UsageError(
            f"Default advisory type '{default_advisory_type}' not found in shipment advisories: {advisories.keys()}"
        )

    # Get the shipment configs from the merge request URL
    mr_url = shipment.get("url")
    if not mr_url:
        raise click.UsageError("Shipment block does not contain a 'url' field for the merge request")
    LOGGER.info(f"Fetching shipment configs from merge request: {mr_url}")

    # Fetch the shipment configs from the merge request
    release_notes = get_release_notes_from_mr(mr_url, default_advisory_type)

    # Fetch the bug IDs from the release notes
    bug_ids = [issue.id for issue in release_notes.issues.fixed] if release_notes.issues else []

    if not bug_ids:
        LOGGER.info("No fixed issues found in the release notes, exiting.")
        sys.exit(0)

    # Get the bug trackers
    tracker_bugs = get_attached_trackers(bug_ids, runtime.get_bug_tracker('jira'))
    tracker_flaws, flaw_bugs = get_flaws(runtime.get_bug_tracker('bugzilla'), tracker_bugs)

    if flaw_bugs:
        LOGGER.info(f"Found {len(flaw_bugs)} flaw bugs, updating release notes.")

        # Turn the advisory type into an RHSA
        release_notes.type = 'RHSA'

        # Add the CVE component mapping to the cve field
        cve_component_mapping = get_cve_component_mapping(flaw_bugs, tracker_bugs, tracker_flaws)
        cves = [
            {'cve_id': cve_id, 'component': component}
            for cve_id, components in cve_component_mapping.items()
            for component in components
        ]
        for cve in cves:
            # tracker_flaw = [tracker_flaws[t.id]
            release_notes.cves.append(CveAssociation(key=cve['cve_id'], component=cve['component']))

    return release_notes


async def handle_brew_cve_flaws(
    runtime: Runtime, into_default_advisories: bool, advisory_id, default_advisory_type: str, noop: bool
):
    """
    Handle attaching CVE flaws in a Brew environment.
    """

    if into_default_advisories:
        advisories = runtime.group_config.advisories.values()
    elif default_advisory_type:
        advisories = [find_default_advisory(runtime, default_advisory_type)]
    else:
        advisories = [advisory_id]

    # Get the advisory kind-id mapping
    advisories_by_kind = runtime.group_config.advisories

    advisory_kind = next((k for k, v in advisories_by_kind.items() if v == advisory_id), None)

    exit_code = 0
    flaw_bug_tracker = runtime.get_bug_tracker('bugzilla')
    errata_config = runtime.get_errata_config()
    errata_api = AsyncErrataAPI(errata_config.get("server", constants.errata_url))
    brew_api = runtime.build_retrying_koji_client()
    for advisory_id in advisories:
        LOGGER.info("Getting advisory %s", advisory_id)
        advisory = Erratum(errata_id=advisory_id)

        attached_trackers = []
        for bug_tracker in [runtime.get_bug_tracker('jira'), runtime.get_bug_tracker('bugzilla')]:
            advisory_bug_ids = bug_tracker.advisory_bug_ids(advisory)
            attached_trackers.extend(get_attached_trackers(advisory_bug_ids, bug_tracker))

        tracker_flaws, flaw_bugs = get_flaws(flaw_bug_tracker, attached_trackers, brew_api)

        try:
            if flaw_bugs:
                _update_advisory(runtime, advisory, advisory_kind, flaw_bugs, flaw_bug_tracker, noop)
                # Associate builds with CVEs
                LOGGER.info('Associating CVEs with builds')
                await associate_builds_with_cves(
                    errata_api, advisory, flaw_bugs, attached_trackers, tracker_flaws, noop
                )
            else:
                pass  # TODO: convert RHSA back to RHBA
        except Exception as e:
            LOGGER.error(traceback.format_exc())
            LOGGER.error(f'Exception: {e}')
            exit_code = 1

    await errata_api.close()
    sys.exit(exit_code)


def get_shipment_configs_from_mr(
    mr_url: str, kinds: Tuple[str, ...] = ("rpm", "image", "extras", "microshift", "metadata")
) -> Dict[str, ShipmentConfig]:
    """Fetch shipment configs from a merge request URL.

    :param mr_url: URL of the merge request
    :param kinds: List of kinds to fetch shipment configs for
    :return: Dict of {kind: ShipmentConfig}
    """

    shipment_configs: Dict[str, ShipmentConfig] = {}

    gitlab_token = os.getenv("GITLAB_TOKEN")
    parsed_url = urlparse(mr_url)
    project_path = parsed_url.path.strip('/').split('/-/merge_requests')[0]
    mr_id = parsed_url.path.split('/')[-1]
    gitlab_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

    gl = gitlab.Gitlab(gitlab_url, private_token=gitlab_token)
    gl.auth()

    project = gl.projects.get(project_path)
    mr = project.mergerequests.get(mr_id)
    source_project = gl.projects.get(mr.source_project_id)

    diff_info = mr.diffs.list(all=True)[0]
    diff = mr.diffs.get(diff_info.id)
    for file_diff in diff.diffs:
        file_path = file_diff.get('new_path') or file_diff.get('old_path')
        if not file_path or not file_path.endswith(('.yaml', '.yml')):
            continue

        filename = file_path.split('/')[-1]
        parts = filename.replace('.yaml', '').replace('.yml', '')
        kind = next((k for k in kinds if k in parts), None)
        if not kind:
            continue

        file_content = source_project.files.get(file_path, mr.source_branch)
        content = file_content.decode().decode('utf-8')

        yaml = new_roundtrip_yaml_handler()
        shipment_data = ShipmentConfig(**yaml.load(content))
        if kind in shipment_configs:
            raise ValueError(f"Multiple shipment configs found for {kind}")
        shipment_configs[kind] = shipment_data

    return shipment_configs


def get_release_notes_from_mr(mr_url: str, kind: str) -> ReleaseNotes:
    """Fetch release notes from a merge request URL."""

    shipment_configs: Dict[str, ShipmentConfig] = get_shipment_configs_from_mr(mr_url, (kind,))
    shipment_config = shipment_configs.get(kind)

    if not shipment_config:
        raise ValueError(f"No shipment config found for kind: {kind}")

    release_notes = shipment_config.shipment.data.releaseNotes
    if not release_notes:
        raise ValueError(f"No release notes found in shipment config for {kind}")

    return release_notes


def get_attached_trackers(bugs_ids: List[str], bug_tracker: BugTracker) -> List[Bug]:
    """
    Get attached tracker bugs from a list of bug IDs.
    """

    if not bugs_ids:
        return []

    attached_tracker_bugs: List[Bug] = bug_tracker.get_tracker_bugs(bugs_ids)
    if not attached_tracker_bugs:
        LOGGER.info(f'Found 0 {bug_tracker.type} tracker bugs attached')

    LOGGER.info(
        f'Found {len(attached_tracker_bugs)} {bug_tracker.type} tracker bugs attached: '
        f'{sorted([b.id for b in attached_tracker_bugs])}'
    )
    return attached_tracker_bugs


def get_flaws(flaw_bug_tracker: BugTracker, tracker_bugs: Iterable[Bug], brew_api=None) -> (Dict, List):
    # validate and get target_release
    if not tracker_bugs:
        return {}, []  # Bug.get_target_release will panic on empty array
    current_target_release = Bug.get_target_release(tracker_bugs)
    tracker_flaws, flaw_tracker_map = BugTracker.get_corresponding_flaw_bugs(
        tracker_bugs,
        flaw_bug_tracker,
        brew_api,
    )
    LOGGER.info(
        f'Found {len(flaw_tracker_map)} {flaw_bug_tracker.type} corresponding flaw bugs:'
        f' {sorted(flaw_tracker_map.keys())}'
    )

    # current_target_release can be digit.digit.([z|0])?
    # if current_target_release is GA then run first-fix bug filtering
    # for GA not every flaw bug is considered first-fix
    # for z-stream every flaw bug is considered first-fix
    # https://docs.engineering.redhat.com/display/PRODSEC/Security+errata+-+First+fix
    if current_target_release[-1] == 'z':
        LOGGER.info("Detected z-stream target release, every flaw bug is considered first-fix")
        first_fix_flaw_bugs = [f['bug'] for f in flaw_tracker_map.values()]
    else:
        LOGGER.info("Detected GA release, applying first-fix filtering..")
        first_fix_flaw_bugs = [
            flaw_bug_info['bug']
            for flaw_bug_info in flaw_tracker_map.values()
            if is_first_fix_any(flaw_bug_info['bug'], flaw_bug_info['trackers'], current_target_release)
        ]

    LOGGER.info(f'{len(first_fix_flaw_bugs)} out of {len(flaw_tracker_map)} flaw bugs considered "first-fix"')
    return tracker_flaws, first_fix_flaw_bugs


def _update_advisory(runtime, advisory, advisory_kind, flaw_bugs, bug_tracker, noop):
    advisory_id = advisory.errata_id
    errata_config = runtime.get_errata_config()

    cve_boilerplate = get_advisory_boilerplate(
        runtime=runtime, et_data=errata_config, art_advisory_key=advisory_kind, errata_type='RHSA'
    )

    try:
        assembly = runtime.get_major_minor_patch()
        versions = {
            'major': assembly[0],
            'minor': assembly[1],
            'patch': assembly[2],
        }
    except ValueError:
        if runtime.assembly_type == AssemblyTypes.STANDARD:
            raise
        major, minor = runtime.get_major_minor()
        versions = {
            'major': major,
            'minor': minor,
            'patch': "0",
        }

    advisory, updated = get_updated_advisory_rhsa(cve_boilerplate, advisory, flaw_bugs, versions)
    if not noop and updated:
        LOGGER.info("Updating advisory details %s", advisory_id)
        advisory.commit()

    flaw_ids = [flaw_bug.id for flaw_bug in flaw_bugs]
    LOGGER.info(f'Attaching {len(flaw_ids)} flaw bugs')
    bug_tracker.attach_bugs(flaw_ids, advisory_obj=advisory, noop=noop)


def get_cve_component_mapping(
    flaw_bugs: Iterable[Bug],
    attached_tracker_bugs: List[Bug],
    tracker_flaws: Dict[int, Iterable],
    attached_components: Iterable = [],
) -> Dict[str, Set[str]]:
    """
    Get a mapping of CVE IDs to component names based on the attached tracker bugs and flaw bugs.
    """

    cve_components_mapping: Dict[str, Set[str]] = {}

    for tracker in attached_tracker_bugs:
        whiteboard_component = tracker.whiteboard_component
        if not whiteboard_component:
            raise ValueError(f"Bug {tracker.id} doesn't have a valid whiteboard component.")
        if whiteboard_component == "rhcos":
            # rhcos trackers are special, since they have per-architecture component names
            # (rhcos-x86_64, rhcos-aarch64, ...) in Brew,
            # but the tracker bug has a generic "rhcos" component name
            # so we need to associate this CVE with all per-architecture component names
            component_names = attached_components & arch_util.RHCOS_BREW_COMPONENTS
        else:
            component_names = {whiteboard_component}

        flaw_id_bugs = {flaw_bug.id: flaw_bug for flaw_bug in flaw_bugs}
        for flaw_id in tracker_flaws[tracker.id]:
            if flaw_id not in flaw_id_bugs:
                continue  # non-first-fix
            alias = [k for k in flaw_id_bugs[flaw_id].alias if k.startswith('CVE-')]
            if len(alias) != 1:
                raise ValueError(f"Bug {flaw_id} should have exactly 1 CVE alias.")
            cve = alias[0]
            cve_components_mapping.setdefault(cve, set()).update(component_names)

    return cve_components_mapping


async def associate_builds_with_cves(
    errata_api: AsyncErrataAPI,
    advisory: Erratum,
    flaw_bugs: Iterable[Bug],
    attached_tracker_bugs: List[Bug],
    tracker_flaws: Dict[int, Iterable],
    dry_run: bool,
):
    # `Erratum.errata_builds` doesn't include RHCOS builds. Use AsyncErrataAPI instead.
    attached_builds = await errata_api.get_builds_flattened(advisory.errata_id)
    attached_components = {parse_nvr(build)["name"] for build in attached_builds}
    cve_components_mapping = get_cve_component_mapping(
        flaw_bugs, attached_tracker_bugs, tracker_flaws, attached_components
    )

    await AsyncErrataUtils.associate_builds_with_cves(
        errata_api, advisory.errata_id, attached_builds, cve_components_mapping, dry_run=dry_run
    )


def get_updated_advisory_rhsa(cve_boilerplate: dict, advisory: Erratum, flaw_bugs, versions):
    """Given an advisory object, get updated advisory to RHSA

    :param cve_boilerplate: cve template for rhsa
    :param advisory: advisory object to update
    :param flaw_bugs: Collection of flaw bug objects to be attached to the advisory
    :returns: updated advisory object and a boolean indicating if advisory was updated
    """
    minor = versions['minor']
    patch = versions['patch']
    updated = False
    if not is_security_advisory(advisory):
        LOGGER.info('Advisory type is {}, converting it to RHSA'.format(advisory.errata_type))
        updated = True
        security_impact = 'Low'
        advisory.update(
            errata_type='RHSA',
            security_reviewer=cve_boilerplate['security_reviewer'],
            synopsis=cve_boilerplate['synopsis'].format(MINOR=minor, PATCH=patch),
            topic=cve_boilerplate['topic'].format(IMPACT=security_impact, MINOR=minor, PATCH=patch),
            solution=cve_boilerplate['solution'].format(MINOR=minor),
            security_impact=security_impact,
        )

    flaw_bugs = sort_cve_bugs(flaw_bugs)
    cve_names = [b.alias[0] for b in flaw_bugs]
    cve_str = ' '.join(cve_names)
    if advisory.cve_names != cve_str:
        advisory.update(cve_names=cve_str)
        updated = True

    if updated:
        formatted_cve_list = '\n'.join(
            [f'* {b.summary.replace(b.alias[0], "").strip()} ({b.alias[0]})' for b in flaw_bugs]
        )
        formatted_description = cve_boilerplate['description'].format(CVES=formatted_cve_list, MINOR=minor, PATCH=patch)
        advisory.update(description=formatted_description)

    highest_impact = get_highest_security_impact(flaw_bugs)
    if highest_impact != advisory.security_impact:
        if constants.security_impact_map[advisory.security_impact] < constants.security_impact_map[highest_impact]:
            LOGGER.info(f'Adjusting advisory security impact from {advisory.security_impact} to {highest_impact}')
            advisory.update(security_impact=highest_impact)
            updated = True
        else:
            LOGGER.info(
                f'Advisory current security impact {advisory.security_impact} is higher than {highest_impact} no need to adjust'
            )

    if highest_impact not in advisory.topic:
        topic = cve_boilerplate['topic'].format(IMPACT=highest_impact, MINOR=minor, PATCH=patch)
        LOGGER.info('Topic updated to include impact of {}'.format(highest_impact))
        advisory.update(topic=topic)

    return advisory, updated
