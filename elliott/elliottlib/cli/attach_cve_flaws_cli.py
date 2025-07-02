import json
import logging
import sys
import traceback
from typing import Dict, Iterable, List, Optional, Set

import click
from artcommonlib import arch_util
from artcommonlib.assembly import assembly_config_struct
from artcommonlib.rpm_utils import parse_nvr
from artcommonlib.util import new_roundtrip_yaml_handler
from errata_tool import Erratum

from elliottlib import constants
from elliottlib.bzutil import Bug, BugTracker, get_flaws, get_highest_security_impact, is_first_fix_any, sort_cve_bugs
from elliottlib.cli.common import cli, click_coroutine, find_default_advisory, use_default_advisory_option
from elliottlib.errata import is_security_advisory
from elliottlib.errata_async import AsyncErrataAPI, AsyncErrataUtils
from elliottlib.runtime import Runtime
from elliottlib.shipment_model import CveAssociation, ReleaseNotes, ShipmentConfig
from elliottlib.shipment_utils import get_shipment_configs_from_mr
from elliottlib.util import get_advisory_boilerplate

YAML = new_roundtrip_yaml_handler()


class AttachCveFlaws:
    def __init__(
        self,
        runtime: Runtime,
        advisory_id: int,
        into_default_advisories: bool,
        default_advisory_type: str,
        output: str,
        noop: bool,
    ):
        self.runtime = runtime
        self.into_default_advisories = into_default_advisories
        self.advisory_id = advisory_id
        self.default_advisory_type = default_advisory_type
        self.output = output
        self.noop = noop
        self.logger = logging.getLogger(__name__)

        # Get the advisory kind-id mapping
        self.errata_config = self.runtime.get_errata_config()

        if default_advisory_type:
            self.advisory_kind = default_advisory_type
        elif advisory_id:
            self.advisory_kind = next(
                (k for k, v in self.runtime.group_config.advisories.items() if v == self.advisory_id), None
            )

        self.errata_api: Optional[AsyncErrataAPI] = None
        self.major, self.minor, self.patch = self.runtime.get_major_minor_patch()

    async def run(self):
        if self.runtime.build_system == 'konflux':
            if self.into_default_advisories or self.advisory_id:
                raise click.UsageError(
                    "Konflux does not yet support --into-default-advisories or --advisory options, "
                    "please use --use-default-advisory instead"
                )

            release_notes = await self.handle_konflux_cve_flaws()

            if self.output == 'json':
                click.echo(json.dumps(release_notes.model_dump(mode='json'), indent=4))
            else:
                YAML.dump(release_notes.model_dump(mode='python'), sys.stdout)

        elif self.runtime.build_system == 'brew':
            await self.handle_brew_cve_flaws()

    async def handle_konflux_cve_flaws(self) -> ReleaseNotes:
        """
        Handle attaching CVE flaws in a Konflux environment.
        """

        # Read shipment block from the assembly config
        assembly_group_config = assembly_config_struct(
            self.runtime.get_releases_config(), self.runtime.assembly, "group", {}
        )
        shipment = assembly_group_config.get("shipment", {})
        advisories = shipment.get("advisories", [])
        if not advisories:
            self.logger.info("No advisories found in the shipment block, exiting.")
            sys.exit(0)

        # Validate default advisory type
        kinds = [advisory.get("kind") for advisory in advisories]
        if self.default_advisory_type not in kinds:
            raise click.UsageError(
                f"Default advisory type '{self.default_advisory_type}' not found in shipment advisories: {advisories.keys()}"
            )

        # Get the shipment configs from the merge request URL
        mr_url = shipment.get("url")
        if not mr_url:
            raise click.UsageError("Shipment block does not contain a 'url' field for the merge request")
        self.logger.info(f"Fetching shipment configs from merge request: {mr_url}")

        # Fetch the shipment configs from the merge request
        release_notes = self.get_release_notes_from_mr(mr_url)

        # Fetch the bug IDs from the release notes
        bug_ids = [issue.id for issue in release_notes.issues.fixed] if release_notes.issues else []

        if not bug_ids:
            self.logger.info("No fixed issues found in the release notes, exiting.")
            sys.exit(0)

        # Get the bug trackers
        tracker_bugs = self.get_attached_trackers(bug_ids, self.runtime.get_bug_tracker('jira'))
        tracker_flaws, flaw_bugs = get_flaws(self.runtime.get_bug_tracker('bugzilla'), tracker_bugs)

        if flaw_bugs:
            self.logger.info(f"Found {len(flaw_bugs)} flaw bugs, updating release notes.")

            # Turn the advisory type into an RHSA
            self.update_konflux_release_notes_to_rhsa(release_notes, flaw_bugs, tracker_bugs, tracker_flaws)

        return release_notes

    def get_release_notes_from_mr(self, mr_url: str) -> ReleaseNotes:
        """Fetch release notes from a merge request URL."""

        kinds = (
            (self.default_advisory_type,)
            if self.default_advisory_type
            else ("rpm", "image", "extras", "microshift", "metadata")
        )
        shipment_configs: Dict[str, ShipmentConfig] = get_shipment_configs_from_mr(mr_url, kinds)
        shipment_config = shipment_configs.get(self.default_advisory_type)

        if not shipment_config:
            raise ValueError(f"No shipment config found for kind: {self.default_advisory_type}")

        release_notes = shipment_config.shipment.data.releaseNotes
        if not release_notes:
            raise ValueError(f"No release notes found in shipment config for {self.default_advisory_type}")

        return release_notes

    def get_attached_trackers(self, bugs_ids: List[str], bug_tracker: BugTracker) -> List[Bug]:
        """
        Get attached tracker bugs from a list of bug IDs.
        """

        if not bugs_ids:
            return []

        attached_tracker_bugs: List[Bug] = bug_tracker.get_tracker_bugs(bugs_ids)
        if not attached_tracker_bugs:
            self.logger.info(f'Found 0 {bug_tracker.type} tracker bugs attached')
            return []

        self.logger.info(
            f'Found {len(attached_tracker_bugs)} {bug_tracker.type} tracker bugs attached: '
            f'{sorted([b.id for b in attached_tracker_bugs])}'
        )
        return attached_tracker_bugs

    def update_konflux_release_notes_to_rhsa(
        self,
        release_notes: ReleaseNotes,
        flaw_bugs: Iterable[Bug],
        tracker_bugs: List[Bug],
        tracker_flaws: Dict[int, Iterable],
    ):
        """
        Update the release notes to convert it to an RHSA type. Also adds CVE associations and flaw bugs.
        """

        if release_notes.type != 'RHSA':
            # Set the release notes type to RHSA
            self.logger.info("Converting release notes to RHSA type.")
            release_notes.type = 'RHSA'

        # Add the CVE component mapping to the cve field
        cve_component_mapping = self.get_cve_component_mapping(flaw_bugs, tracker_bugs, tracker_flaws)
        for cve_id, components in cve_component_mapping.items():
            for component in components:
                release_notes.cves.append(CveAssociation(key=cve_id, component=component))

        # Attach the flaw bugs to the release notes issues
        release_notes.issues.fixed.extend([{'id': bug.id, 'source': "bugzilla.redhat.com"} for bug in flaw_bugs])

        # Update synopsis, topic and solution
        cve_boilerplate = get_advisory_boilerplate(
            runtime=self.runtime, et_data=self.errata_config, art_advisory_key=self.advisory_kind, errata_type='RHSA'
        )
        release_notes.synopsis = cve_boilerplate['synopsis'].format(MINOR=self.minor, PATCH=self.patch)
        highest_impact = get_highest_security_impact(flaw_bugs)
        release_notes.topic = cve_boilerplate['topic'].format(IMPACT=highest_impact, MINOR=self.minor, PATCH=self.patch)
        release_notes.solution = cve_boilerplate['solution'].format(MINOR=self.minor)

        # Add CVE component mapping
        cve_component_mapping = self.get_cve_component_mapping(flaw_bugs, tracker_bugs, tracker_flaws)
        for cve_id, components in cve_component_mapping.items():
            for component in components:
                release_notes.cves.append(CveAssociation(key=cve_id, component=component))

    async def handle_brew_cve_flaws(self):
        """
        Handle attaching CVE flaws in a Brew environment.
        """

        if self.into_default_advisories:
            advisories = self.runtime.group_config.advisories.values()
        elif self.default_advisory_type:
            advisories = [find_default_advisory(self.runtime, self.default_advisory_type)]
        else:
            advisories = [self.advisory_id]

        exit_code = 0
        flaw_bug_tracker = self.runtime.get_bug_tracker('bugzilla')
        self.errata_api = AsyncErrataAPI(self.errata_config.get("server", constants.errata_url))
        brew_api = self.runtime.build_retrying_koji_client()

        for advisory_id in advisories:
            self.logger.info("Getting advisory %s", advisory_id)
            advisory = Erratum(errata_id=advisory_id)

            attached_trackers = []
            for bug_tracker in [self.runtime.get_bug_tracker('jira'), self.runtime.get_bug_tracker('bugzilla')]:
                advisory_bug_ids = bug_tracker.advisory_bug_ids(advisory)
                attached_trackers.extend(self.get_attached_trackers(advisory_bug_ids, bug_tracker))

            tracker_flaws, flaw_bugs = get_flaws(flaw_bug_tracker, attached_trackers, brew_api)

            try:
                if flaw_bugs:
                    self._update_advisory(advisory, self.advisory_kind, flaw_bugs, flaw_bug_tracker, self.noop)
                    # Associate builds with CVEs
                    self.logger.info('Associating CVEs with builds')
                    await self.associate_builds_with_cves(advisory, flaw_bugs, attached_trackers, tracker_flaws)
                else:
                    pass  # TODO: convert RHSA back to RHBA
            except Exception as e:
                self.logger.error(traceback.format_exc())
                self.logger.error(f'Exception: {e}')
                exit_code = 1

        await self.errata_api.close()
        sys.exit(exit_code)

    def _update_advisory(self, advisory, advisory_kind, flaw_bugs, bug_tracker, noop):
        advisory_id = advisory.errata_id
        errata_config = self.runtime.get_errata_config()

        cve_boilerplate = get_advisory_boilerplate(
            runtime=self.runtime, et_data=errata_config, art_advisory_key=advisory_kind, errata_type='RHSA'
        )

        versions = {
            'major': self.major,
            'minor': self.minor,
            'patch': self.patch,
        }
        advisory, updated = self.get_updated_advisory_rhsa(cve_boilerplate, advisory, flaw_bugs, versions)

        if not noop and updated:
            self.logger.info("Updating advisory details %s", advisory_id)
            advisory.commit()

        flaw_ids = [flaw_bug.id for flaw_bug in flaw_bugs]
        self.logger.info(f'Attaching {len(flaw_ids)} flaw bugs')
        bug_tracker.attach_bugs(flaw_ids, advisory_obj=advisory, noop=noop)

    @staticmethod
    def get_cve_component_mapping(
        flaw_bugs: Iterable[Bug],
        attached_tracker_bugs: List[Bug],
        tracker_flaws: Dict[int, Iterable],
        attached_components: Set = None,
    ) -> Dict[str, Set[str]]:
        """
        Get a mapping of CVE IDs to component names based on the attached tracker bugs and flaw bugs.
        """

        attached_components = attached_components if attached_components else set()
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
        self,
        advisory: Erratum,
        flaw_bugs: Iterable[Bug],
        attached_tracker_bugs: List[Bug],
        tracker_flaws: Dict[int, Iterable],
    ):
        # `Erratum.errata_builds` doesn't include RHCOS builds. Use AsyncErrataAPI instead.
        attached_builds = await self.errata_api.get_builds_flattened(advisory.errata_id)
        attached_components = {parse_nvr(build)["name"] for build in attached_builds}
        cve_components_mapping = self.get_cve_component_mapping(
            flaw_bugs, attached_tracker_bugs, tracker_flaws, attached_components
        )

        await AsyncErrataUtils.associate_builds_with_cves(
            self.errata_api, advisory.errata_id, attached_builds, cve_components_mapping, dry_run=self.noop
        )

    def get_updated_advisory_rhsa(self, cve_boilerplate: dict, advisory: Erratum, flaw_bugs, versions):
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
            self.logger.info('Advisory type is {}, converting it to RHSA'.format(advisory.errata_type))
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
            formatted_description = cve_boilerplate['description'].format(
                CVES=formatted_cve_list, MINOR=minor, PATCH=patch
            )
            advisory.update(description=formatted_description)

        highest_impact = get_highest_security_impact(flaw_bugs)
        if highest_impact != advisory.security_impact:
            if constants.security_impact_map[advisory.security_impact] < constants.security_impact_map[highest_impact]:
                self.logger.info(
                    f'Adjusting advisory security impact from {advisory.security_impact} to {highest_impact}'
                )
                advisory.update(security_impact=highest_impact)
                updated = True
            else:
                self.logger.info(
                    f'Advisory current security impact {advisory.security_impact} is higher than {highest_impact} no need to adjust'
                )

        if highest_impact not in advisory.topic:
            topic = cve_boilerplate['topic'].format(IMPACT=highest_impact, MINOR=minor, PATCH=patch)
            self.logger.info('Topic updated to include impact of {}'.format(highest_impact))
            advisory.update(topic=topic)

        return advisory, updated


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
@click.option('--output', default='json', type=click.Choice(['yaml', 'json']), help='Output format')
@click.pass_obj
@click_coroutine
async def attach_cve_flaws_cli(
    runtime: Runtime,
    advisory_id: int,
    noop: bool,
    default_advisory_type: str,
    into_default_advisories: bool,
    output: str,
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

    pipeline = AttachCveFlaws(
        runtime=runtime,
        advisory_id=advisory_id,
        into_default_advisories=into_default_advisories,
        default_advisory_type=default_advisory_type,
        output=output,
        noop=noop,
    )
    await pipeline.run()
