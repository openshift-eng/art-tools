import copy
import json
import logging
import sys
import traceback
from typing import Dict, Iterable, List, Optional, Set

import click
from artcommonlib import arch_util
from artcommonlib.assembly import assembly_config_struct
from artcommonlib.gitdata import SafeFormatter
from artcommonlib.rpm_utils import parse_nvr
from artcommonlib.util import new_roundtrip_yaml_handler
from doozerlib.backend.konflux_image_builder import KonfluxImageBuilder
from errata_tool import Erratum

from elliottlib import constants
from elliottlib.bzutil import Bug, BugTracker, get_flaws, get_highest_security_impact, sort_cve_bugs
from elliottlib.cli.common import cli, click_coroutine, find_default_advisory, use_default_advisory_option
from elliottlib.errata import get_errata_live_id, is_security_advisory
from elliottlib.errata_async import AsyncErrataAPI, AsyncErrataUtils
from elliottlib.runtime import Runtime
from elliottlib.shipment_model import CveAssociation, ReleaseNotes
from elliottlib.shipment_utils import get_shipment_config_from_mr, set_bugzilla_bug_ids
from elliottlib.util import get_advisory_boilerplate, get_component_by_delivery_repo

YAML = new_roundtrip_yaml_handler()


def get_konflux_component_by_component(runtime: Runtime, component_name: str) -> Optional[str]:
    """Get the konflux build component name from the component name
    For example, "sriov-network-device-plugin-container" -> "ose-4-18-sriov-network-device-plugin"
    """
    if not runtime.image_metas():
        raise ValueError("No image metas found. Forgot to initialize runtime with mode='images'?")

    image_meta = None
    for image in runtime.image_metas():
        if component_name == image.get_component_name():
            image_meta = image
            break
    if not image_meta:
        return None

    application = KonfluxImageBuilder.get_application_name(runtime.group)
    return KonfluxImageBuilder.get_component_name(application, image_meta.distgit_key)


def _get_components_using_builder(runtime: Runtime, attached_components: Set[str], logger) -> Set[str]:
    """
    Find components that use the golang builder by checking their image configurations.

    Args:
        runtime: Runtime instance with image metadata
        attached_components: Set of component names to filter from
        logger: Logger instance for logging

    Returns:
        Set of component names that use the golang builder
    """
    if not runtime.image_metas():
        logger.warning("No image metadata available for builder analysis")
        return set()

    matching_components = set()
    components_using_builder = []  # For detailed logging
    components_not_using_builder = []  # For detailed logging
    components_filtered_out = []  # Components that use builder but not in shipment

    logger.info(f"Analyzing {len(runtime.image_metas())} image metadata entries for golang builder usage")

    # Get Konflux component names for all images that use the builder
    for image_meta in runtime.image_metas():
        image_name = image_meta.distgit_key

        # Check if this image uses the golang builder in its configuration
        if _image_uses_builder(image_meta, logger):
            # Get the Konflux component name for this image
            application = KonfluxImageBuilder.get_application_name(runtime.group)
            konflux_component_name = KonfluxImageBuilder.get_component_name(application, image_meta.distgit_key)

            components_using_builder.append(f"{image_name} -> {konflux_component_name}")

            # Only include if it's in the attached components
            if konflux_component_name in attached_components:
                matching_components.add(konflux_component_name)
            else:
                components_filtered_out.append(f"{image_name} -> {konflux_component_name}")
        else:
            components_not_using_builder.append(image_name)

    # Log detailed analysis results
    logger.info(f"Images using golang builder: {len(components_using_builder)} total")

    if components_filtered_out:
        logger.info(f"Images using golang builder but NOT in shipment ({len(components_filtered_out)} filtered out):")
        for component in components_filtered_out:
            logger.info(f"  - {component}")

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"Images NOT using golang builder ({len(components_not_using_builder)} total):")
        for component in components_not_using_builder[:10]:  # Limit to first 10 to avoid spam
            logger.debug(f"  - {component}")
        if len(components_not_using_builder) > 10:
            logger.debug(f"  - ... and {len(components_not_using_builder) - 10} more")

    logger.info(f"Final result: {len(matching_components)} components in shipment use golang builder")

    return matching_components


def _image_uses_builder(image_meta, logger) -> bool:
    """
    Check if an image metadata uses the golang builder.

    Args:
        image_meta: ImageMetadata instance
        logger: Logger instance for logging

    Returns:
        True if the image uses the golang builder
    """
    try:
        # Get the image configuration
        config = image_meta.config
        image_name = image_meta.distgit_key

        # Check in the from.builder section for streams containing "golang"
        from_streams = []
        if 'from' in config and 'builder' in config['from']:
            for builder_config in config['from']['builder']:
                if isinstance(builder_config, dict) and 'stream' in builder_config:
                    stream_name = builder_config['stream']
                    from_streams.append(stream_name)
                    # Check if the stream name contains "golang"
                    if "golang" in stream_name:
                        # Check if this image should be included based on for_release field
                        # Include if for_release is not explicitly false
                        for_release = config.get('for_release', True)  # Default to True if not specified
                        if for_release is not False:
                            logger.debug(
                                f"Image '{image_name}' uses golang builder via from.builder stream: '{stream_name}' (for_release: {for_release})"
                            )
                            return True
                        else:
                            logger.debug(
                                f"Image '{image_name}' uses golang builder but has for_release: false, excluding"
                            )

        # Log debug info if no matches found
        if from_streams:
            logger.debug(f"Image '{image_name}' has streams {from_streams} but none contain 'golang'")
        elif 'from' in config:
            logger.debug(f"Image '{image_name}' has from section but no valid builder streams")
        else:
            logger.debug(f"Image '{image_name}' has no from section in config")

        return False
    except Exception as e:
        logger.debug(f"Error checking builder for image '{getattr(image_meta, 'distgit_key', 'unknown')}': {e}")
        # If there's any error accessing the config, assume it doesn't use the builder
        return False


class AttachCveFlaws:
    def __init__(
        self,
        runtime: Runtime,
        advisory_id: int,
        into_default_advisories: bool,
        default_advisory_type: str,
        output: str,
        noop: bool,
        reconcile: bool = False,
    ):
        self.runtime = runtime
        self.into_default_advisories = into_default_advisories
        self.advisory_id = advisory_id
        self.default_advisory_type = default_advisory_type
        self.output = output
        self.noop = noop
        self.reconcile = reconcile
        self.logger = logging.getLogger(__name__)

        self.errata_config = self.runtime.get_errata_config()

        if default_advisory_type:
            self.advisory_kind = default_advisory_type
        elif advisory_id:
            self.advisory_kind = next(
                (k for k, v in self.runtime.group_config.advisories.items() if v == self.advisory_id), None
            )

        self.errata_api: Optional[AsyncErrataAPI] = None
        self.major, self.minor, self.patch = self.runtime.get_major_minor_patch()
        self._replace_vars = {"MAJOR": self.major, "MINOR": self.minor, "PATCH": self.patch}
        rpm_advisory = self.runtime.group_config.advisories.get("rpm")
        if rpm_advisory is not None:
            live_id = get_errata_live_id(rpm_advisory)
            self._replace_vars.update({"RPM_ADVISORY": live_id})

    async def run(self):
        if self.runtime.build_system == 'konflux':
            if self.into_default_advisories or self.advisory_id:
                raise click.UsageError(
                    "Konflux does not yet support --into-default-advisories or --advisory options, "
                    "please use --use-default-advisory instead"
                )

            release_notes = await self.handle_konflux_cve_flaws()
            if not release_notes:
                self.logger.info("No changes made, exiting.")
                sys.exit(0)

            if self.output == 'json':
                click.echo(
                    json.dumps(release_notes.model_dump(mode='json', exclude_unset=True, exclude_none=True), indent=4)
                )
            else:
                YAML.dump(release_notes.model_dump(mode='python', exclude_unset=True, exclude_none=True), sys.stdout)

        elif self.runtime.build_system == 'brew':
            if self.reconcile:
                raise click.UsageError("Reconciliation is not supported for Brew")
            await self.handle_brew_cve_flaws()

    async def handle_konflux_cve_flaws(self) -> Optional[ReleaseNotes]:
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
            return None

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
        shipment_config = get_shipment_config_from_mr(mr_url, self.default_advisory_type)
        if not shipment_config:
            raise ValueError(f"No shipment config found for kind: {self.default_advisory_type}")

        release_notes = shipment_config.shipment.data.releaseNotes
        if not release_notes:
            raise ValueError(f"No release notes found in shipment config for {self.default_advisory_type}")

        # Extract component names from the shipment snapshot
        attached_components = set()
        if shipment_config.shipment.snapshot and shipment_config.shipment.snapshot.spec.components:
            attached_components = {comp.name for comp in shipment_config.shipment.snapshot.spec.components}
        self.logger.info(f"Found {len(attached_components)} components in shipment")

        # Fetch the bug IDs from the release notes
        bug_ids = [issue.id for issue in release_notes.issues.fixed] if release_notes.issues else []
        self.logger.info(f"Found {len(bug_ids)} bugs in shipment")

        # Get tracker bugs
        tracker_bugs = self.get_attached_trackers(bug_ids, self.runtime.get_bug_tracker('jira')) if bug_ids else []
        self.logger.info(f"Found {len(tracker_bugs)} tracker bugs in shipment")

        # Get flaw bugs
        tracker_flaws, flaw_bugs = get_flaws(self.runtime, tracker_bugs) if tracker_bugs else ({}, [])
        self.logger.info(f"Found {len(flaw_bugs)} eligible flaw bugs for shipment to be attached")

        # Update the release notes
        updated_release_notes = copy.deepcopy(release_notes)
        self.update_release_notes(updated_release_notes, flaw_bugs, tracker_bugs, tracker_flaws, attached_components)
        if updated_release_notes == release_notes:
            self.logger.info("No changes made to the release notes")
            return None

        return updated_release_notes

    def get_release_notes_from_mr(self, mr_url: str) -> ReleaseNotes:
        """Fetch release notes from a merge request URL."""

        shipment_config = get_shipment_config_from_mr(mr_url, self.default_advisory_type)
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

    def update_release_notes(
        self,
        release_notes: ReleaseNotes,
        flaw_bugs: Iterable[Bug],
        tracker_bugs: List[Bug],
        tracker_flaws: Dict[int, Iterable],
        attached_components: Set[str] = None,
    ):
        """
        Update the release notes to convert it to an RHSA type. Also adds CVE associations and flaw bugs.
        """
        formatter = SafeFormatter()
        if flaw_bugs:
            if release_notes.type != 'RHSA':
                # Set the release notes type to RHSA
                self.logger.info("Converting release notes to RHSA type.")
                release_notes.type = 'RHSA'

            # Re-fetch RPM advisory live ID in case it was also converted to RHSA
            rpm_advisory = self.runtime.group_config.advisories.get("rpm")
            if rpm_advisory is not None:
                live_id = get_errata_live_id(rpm_advisory)
                self._replace_vars["RPM_ADVISORY"] = live_id
                self.logger.info(f"Updated RPM_ADVISORY placeholder to: {live_id}")

            # Add the CVE component mapping to the cve field
            cve_component_mapping = AttachCveFlaws.get_cve_component_mapping(
                self.runtime,
                flaw_bugs,
                tracker_bugs,
                tracker_flaws,
                attached_components,
                konflux=True,
                logger=self.logger,
            )
            release_notes.cves = [
                CveAssociation(key=cve_id, component=component)
                for cve_id, components in cve_component_mapping.items()
                for component in components
            ]
            release_notes.cves.sort(key=lambda x: x.key)  # Sort by CVE ID

            set_bugzilla_bug_ids(release_notes, [b.id for b in flaw_bugs])

            # Update synopsis, topic and solution
            cve_boilerplate = get_advisory_boilerplate(
                runtime=self.runtime,
                et_data=self.errata_config,
                art_advisory_key=self.advisory_kind,
                errata_type='RHSA',
            )
            highest_impact = get_highest_security_impact(flaw_bugs)
            self._replace_vars["IMPACT"] = highest_impact
            formatted_cve_list = '\n'.join(
                [f'* {b.summary.replace(b.alias[0], "").strip()} ({b.alias[0]})' for b in flaw_bugs]
            )
            self._replace_vars['CVES'] = formatted_cve_list
            release_notes.synopsis = formatter.format(cve_boilerplate['synopsis'], **self._replace_vars)
            release_notes.topic = formatter.format(cve_boilerplate['topic'], **self._replace_vars)
            release_notes.solution = formatter.format(cve_boilerplate['solution'], **self._replace_vars)

            # Update description
            release_notes.description = formatter.format(cve_boilerplate['description'], **self._replace_vars)
        elif self.reconcile:
            # Convert RHSA back to RHBA
            if release_notes.type == 'RHBA':
                self.logger.info("Advisory is already RHBA, skipping reconciliation")
                return

            self.logger.info("Converting RHSA back to RHBA")
            release_notes.type = 'RHBA'

            # Remove CVE associations
            self.logger.info("Removing CVE associations")
            release_notes.cves = None

            # Remove flaw bugs if any are attached
            set_bugzilla_bug_ids(release_notes, [])

            # Reset release notes
            self.logger.info("Resetting release notes text fields")
            boilerplate = get_advisory_boilerplate(
                runtime=self.runtime,
                et_data=self.errata_config,
                art_advisory_key=self.advisory_kind,
                errata_type='RHBA',
            )
            release_notes.synopsis = formatter.format(boilerplate['synopsis'], **self._replace_vars)
            release_notes.topic = formatter.format(boilerplate['topic'], **self._replace_vars)
            release_notes.solution = formatter.format(boilerplate['solution'], **self._replace_vars)
            release_notes.description = formatter.format(boilerplate['description'], **self._replace_vars)

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

        for advisory_id in advisories:
            self.logger.info("Getting advisory %s", advisory_id)
            advisory = Erratum(errata_id=advisory_id)

            attached_trackers = []
            for bug_tracker in [self.runtime.get_bug_tracker('jira'), self.runtime.get_bug_tracker('bugzilla')]:
                advisory_bug_ids = bug_tracker.advisory_bug_ids(advisory)
                attached_trackers.extend(self.get_attached_trackers(advisory_bug_ids, bug_tracker))

            tracker_flaws, flaw_bugs = get_flaws(self.runtime, attached_trackers) if attached_trackers else ({}, [])
            self.logger.info(f"Found {len(flaw_bugs)} eligible flaw bugs for advisory {advisory_id} to be attached")

            try:
                if flaw_bugs:
                    self.update_advisory_brew(advisory, self.advisory_kind, flaw_bugs, flaw_bug_tracker, self.noop)
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

    def update_advisory_brew(self, advisory, advisory_kind, flaw_bugs, bug_tracker, noop):
        advisory_id = advisory.errata_id
        errata_config = self.runtime.get_errata_config()

        cve_boilerplate = get_advisory_boilerplate(
            runtime=self.runtime, et_data=errata_config, art_advisory_key=advisory_kind, errata_type='RHSA'
        )

        advisory, updated = self.get_updated_advisory_rhsa(cve_boilerplate, advisory, flaw_bugs)

        if not noop and updated:
            self.logger.info("Updating advisory details %s", advisory_id)
            advisory.commit()

        flaw_ids = [flaw_bug.id for flaw_bug in flaw_bugs]
        self.logger.info(f'Attaching {len(flaw_ids)} flaw bugs')
        bug_tracker.attach_bugs(flaw_ids, advisory_obj=advisory, noop=noop)

    @staticmethod
    def get_cve_component_mapping(
        runtime: Runtime,
        flaw_bugs: Iterable[Bug],
        attached_tracker_bugs: List[Bug],
        tracker_flaws: Dict[int, Iterable],
        attached_components: Set = None,
        konflux: bool = False,
        logger=None,
    ) -> Dict[str, Set[str]]:
        """
        Get a mapping of CVE IDs to component names based on the attached tracker bugs and flaw bugs.
        """

        if logger is None:
            logger = logging.getLogger(__name__)

        attached_components = attached_components if attached_components else set()
        cve_components_mapping: Dict[str, Set[str]] = {}

        for tracker in attached_tracker_bugs:
            if not tracker.whiteboard_component:
                raise ValueError(f"Bug {tracker.id} doesn't have a valid whiteboard component.")

            whiteboard_component = tracker.whiteboard_component
            if "openshift4/" in whiteboard_component:
                # this means the component here is the delivery repo name
                # we need to translate it to build component name
                new_component = get_component_by_delivery_repo(runtime, whiteboard_component)
                if not new_component:
                    raise ValueError(f"Component {whiteboard_component} could not be translated")
                whiteboard_component = new_component

            component_names = None  # Initialize to ensure it's always defined

            if konflux:
                new_component = get_konflux_component_by_component(runtime, whiteboard_component)
                if not new_component:
                    # Special case for builder containers: they should map to all components that use this builder
                    if whiteboard_component == "openshift-golang-builder-container":
                        # Check which components actually use the golang builder
                        logger.info(f"Processing builder container CVE for '{whiteboard_component}' (golang builder)")

                        logger.info(f"Total components in shipment: {len(attached_components)}")

                        components_using_builder = _get_components_using_builder(runtime, attached_components, logger)
                        if components_using_builder:
                            logger.info(
                                f"Found {len(components_using_builder)} components using golang builder: {sorted(components_using_builder)}"
                            )
                            component_names = components_using_builder
                        else:
                            logger.warning(
                                f"No components found using golang builder, falling back to all {len(attached_components)} components in shipment"
                            )
                            # Fallback to all components if we can't determine usage
                            component_names = attached_components.copy()
                    else:
                        raise ValueError(f"Component {whiteboard_component} could not be translated")
                else:
                    whiteboard_component = new_component

            if component_names is None:  # Only set if not already set for builder containers
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

                # Log the final CVE to component mapping
                logger.info(
                    f"Mapped CVE {cve} (from tracker {tracker.id}) to {len(component_names)} components: {sorted(component_names)}"
                )

        logger.info(f"Final CVE component mapping: {len(cve_components_mapping)} CVEs mapped to components")
        for cve, components in cve_components_mapping.items():
            logger.info(f"  {cve} -> {len(components)} components: {sorted(components)}")

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
        cve_components_mapping = AttachCveFlaws.get_cve_component_mapping(
            self.runtime, flaw_bugs, attached_tracker_bugs, tracker_flaws, attached_components, logger=self.logger
        )

        await AsyncErrataUtils.associate_builds_with_cves(
            self.errata_api, advisory.errata_id, attached_builds, cve_components_mapping, dry_run=self.noop
        )

    def get_updated_advisory_rhsa(self, cve_boilerplate: dict, advisory: Erratum, flaw_bugs):
        """Given an advisory object, get updated advisory to RHSA

        :param cve_boilerplate: cve template for rhsa
        :param advisory: advisory object to update
        :param flaw_bugs: Collection of flaw bug objects to be attached to the advisory
        :returns: updated advisory object and a boolean indicating if advisory was updated
        """
        updated = False
        formatter = SafeFormatter()
        if not is_security_advisory(advisory):
            self.logger.info('Advisory type is {}, converting it to RHSA'.format(advisory.errata_type))
            updated = True
            low_impact = 'Low'
            self._replace_vars['IMPACT'] = low_impact

            # Re-fetch RPM advisory live ID in case it was also converted to RHSA
            rpm_advisory = self.runtime.group_config.advisories.get("rpm")
            if rpm_advisory is not None:
                live_id = get_errata_live_id(rpm_advisory)
                self._replace_vars["RPM_ADVISORY"] = live_id
                self.logger.info(f"Updated RPM_ADVISORY placeholder to: {live_id}")

            advisory.update(
                errata_type='RHSA',
                security_reviewer=cve_boilerplate['security_reviewer'],
                synopsis=formatter.format(cve_boilerplate['synopsis'], **self._replace_vars),
                topic=formatter.format(cve_boilerplate['topic'], **self._replace_vars),
                solution=formatter.format(cve_boilerplate['solution'], **self._replace_vars),
                security_impact=low_impact,
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
            self._replace_vars['CVES'] = formatted_cve_list
            formatted_description = formatter.format(cve_boilerplate['description'], **self._replace_vars)
            advisory.update(description=formatted_description)

        highest_impact = get_highest_security_impact(flaw_bugs)
        if highest_impact != advisory.security_impact:
            if constants.security_impact_map[advisory.security_impact] < constants.security_impact_map[highest_impact]:
                self.logger.info(
                    f'Adjusting advisory security impact from {advisory.security_impact} to {highest_impact}'
                )
                self._replace_vars['IMPACT'] = highest_impact
                advisory.update(security_impact=highest_impact)
                updated = True
            else:
                self.logger.info(
                    f'Advisory current security impact {advisory.security_impact} is higher than {highest_impact} no need to adjust'
                )
                self._replace_vars['IMPACT'] = highest_impact = advisory.security_impact
        if highest_impact not in advisory.topic:
            topic = formatter.format(cve_boilerplate['topic'], **self._replace_vars)
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
@click.option(
    "--reconcile", is_flag=True, help='Converts RHSA back to RHBA, removes flaw bugs and CVE associations if applicable'
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
    reconcile: bool,
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
    runtime.initialize(mode="images")

    pipeline = AttachCveFlaws(
        runtime=runtime,
        advisory_id=advisory_id,
        into_default_advisories=into_default_advisories,
        default_advisory_type=default_advisory_type,
        output=output,
        noop=noop,
        reconcile=reconcile,
    )
    await pipeline.run()
