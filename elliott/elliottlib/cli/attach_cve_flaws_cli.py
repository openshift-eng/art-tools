import logging
import sys
import traceback
from typing import Dict, Iterable, List, Set

import click
from artcommonlib import arch_util
from artcommonlib.rpm_utils import parse_nvr
from errata_tool import Erratum

from elliottlib import constants
from elliottlib.bzutil import Bug, BugTracker, get_highest_security_impact, is_first_fix_any, sort_cve_bugs
from elliottlib.cli.common import cli, click_coroutine, find_default_advisory, use_default_advisory_option
from elliottlib.errata import is_security_advisory
from elliottlib.errata_async import AsyncErrataAPI, AsyncErrataUtils
from elliottlib.runtime import Runtime
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
    if into_default_advisories:
        advisories = runtime.group_config.advisories.values()
    elif default_advisory_type:
        advisories = [find_default_advisory(runtime, default_advisory_type)]
    else:
        advisories = [advisory_id]

    # Get the advisory kind-id mapping
    releases_config = runtime.get_releases_config()
    advisories_by_kind = releases_config["releases"][runtime.assembly]['assembly']['group']['advisories']

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
            attached_trackers.extend(get_attached_trackers(advisory, bug_tracker))

        tracker_flaws, flaw_bugs = get_flaws(flaw_bug_tracker, attached_trackers, brew_api)

        try:
            if flaw_bugs:
                _update_advisory(runtime, advisory, advisories_by_kind, flaw_bugs, flaw_bug_tracker, noop)
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


def get_attached_trackers(advisory: Erratum, bug_tracker: BugTracker):
    # get attached bugs from advisory
    advisory_bug_ids = bug_tracker.advisory_bug_ids(advisory)
    if not advisory_bug_ids:
        LOGGER.info(f'Found 0 {bug_tracker.type} bugs attached')
        return []

    attached_tracker_bugs: List[Bug] = bug_tracker.get_tracker_bugs(advisory_bug_ids)
    LOGGER.info(
        f'Found {len(attached_tracker_bugs)} {bug_tracker.type} tracker bugs attached: '
        f'{sorted([b.id for b in attached_tracker_bugs])}'
    )
    return attached_tracker_bugs


def get_flaws(flaw_bug_tracker: BugTracker, tracker_bugs: Iterable[Bug], brew_api) -> (Dict, List):
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


def _update_advisory(runtime, advisory, advisories_by_kind, flaw_bugs, bug_tracker, noop):
    advisory_id = advisory.errata_id
    errata_config = runtime.get_errata_config()

    # Get the name of the advisory (eg. image|rpm|metadata|microshift etc.)
    art_advisory_key = next((k for k, v in advisories_by_kind.items() if v == advisory_id), None)

    if not art_advisory_key:
        raise ValueError(f'ART advisory key not found for advisory: {advisory_id} in list {advisories_by_kind.items()}')

    cve_boilerplate = get_advisory_boilerplate(
        runtime=runtime, et_data=errata_config, art_advisory_key=art_advisory_key, errata_type='RHSA'
    )

    advisory, updated = get_updated_advisory_rhsa(cve_boilerplate, advisory, flaw_bugs)
    if not noop and updated:
        LOGGER.info("Updating advisory details %s", advisory_id)
        advisory.commit()

    flaw_ids = [flaw_bug.id for flaw_bug in flaw_bugs]
    LOGGER.info(f'Attaching {len(flaw_ids)} flaw bugs')
    bug_tracker.attach_bugs(flaw_ids, advisory_obj=advisory, noop=noop)


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

    await AsyncErrataUtils.associate_builds_with_cves(
        errata_api, advisory.errata_id, attached_builds, cve_components_mapping, dry_run=dry_run
    )


def get_updated_advisory_rhsa(cve_boilerplate: dict, advisory: Erratum, flaw_bugs):
    """Given an advisory object, get updated advisory to RHSA

    :param cve_boilerplate: cve template for rhsa
    :param advisory: advisory object to update
    :param flaw_bugs: Collection of flaw bug objects to be attached to the advisory
    :returns: updated advisory object and a boolean indicating if advisory was updated
    """
    updated = False
    if not is_security_advisory(advisory):
        LOGGER.info('Advisory type is {}, converting it to RHSA'.format(advisory.errata_type))
        updated = True
        advisory.update(
            errata_type='RHSA',
            security_reviewer=cve_boilerplate['security_reviewer'],
            synopsis=cve_boilerplate['synopsis'],
            topic=cve_boilerplate['topic'].format(IMPACT="Low"),
            # solution=cve_boilerplate['solution'],
            security_impact='Low',
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
        formatted_description = cve_boilerplate['description'].format(CVES=formatted_cve_list)
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
        topic = cve_boilerplate['topic'].format(IMPACT=highest_impact)
        LOGGER.info('Topic updated to include impact of {}'.format(highest_impact))
        advisory.update(topic=topic)

    return advisory, updated
