import asyncio
import json
import re
import sys
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

import click
import requests
from artcommonlib import exectools, rhcos
from artcommonlib.arch_util import go_arch_for_brew_arch, go_suffix_for_arch
from artcommonlib.assembly import AssemblyTypes
from artcommonlib.constants import ART_PROD_IMAGE_REPO, KONFLUX_IMAGESTREAM_OVERRIDE_VERSIONS, RHCOS_RELEASES_STREAM_URL
from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome, KonfluxBuildRecord
from artcommonlib.konflux.package_rpm_finder import PackageRpmFinder
from artcommonlib.model import Missing, Model
from artcommonlib.release_util import isolate_el_version_in_release
from artcommonlib.util import get_assembly_release_date
from requests.adapters import HTTPAdapter
from ruamel.yaml import YAML
from semver import VersionInfo
from urllib3.util.retry import Retry

from doozerlib import brew, release_inspector, util
from doozerlib.brew import brew_event_from_datetime
from doozerlib.build_info import BrewBuildRecordInspector, BuildRecordInspector, KonfluxBuildRecordInspector
from doozerlib.cli import cli, click_coroutine, pass_runtime
from doozerlib.rpmcfg import RPMMetadata
from doozerlib.runtime import Runtime


@cli.group("release:gen-assembly", short_help="Output assembly metadata based on inputs")
@click.option(
    '--name',
    metavar='ASSEMBLY_NAME',
    required=True,
    help='The name of the assembly (e.g. "4.9.99", "art1234") to scaffold',
)
@click.pass_context
def releases_gen_assembly(ctx, name):
    ctx.ensure_object(dict)
    ctx.obj['ASSEMBLY_NAME'] = name
    pass


@releases_gen_assembly.command(
    'from-releases', short_help='Outputs assembly metadata based on a set of specified releases'
)
@click.option(
    '--nightly',
    'nightlies',
    metavar='NIGHTLY_NAME',
    default=[],
    multiple=True,
    help='A nightly release name for each architecture (e.g. 4.7.0-0.nightly-2021-07-07-214918)',
)
@click.option(
    '--standard',
    'standards',
    metavar='4.y.z-ARCH',
    default=[],
    multiple=True,
    help='The name and arch of an official release (e.g. 4.8.3-x86_64) '
    'where ARCH in [x86_64, s390x, ppc64le, aarch64].',
)
@click.option(
    "--custom",
    default=False,
    is_flag=True,
    help="If specified, weaker conformance criteria are applied (e.g. a nightly is not required for every arch).",
)
@click.option(
    "--pre-ga-mode",
    type=click.Choice(["prerelease"], case_sensitive=False),
    help="Prepare the advisory for 'prerelease' operator release",
)
@click.option('--in-flight', 'in_flight', metavar='EDGE', help='An in-flight release that can upgrade to this release')
@click.option(
    '--previous',
    'previous_list',
    metavar='EDGES',
    default=[],
    multiple=True,
    help='A list of releases that can upgrade to this release',
)
@click.option(
    '--auto-previous',
    'auto_previous',
    is_flag=True,
    help='If specified, previous list is calculated from Cincinnati graph',
)
@click.option(
    "--graph-url",
    metavar='GRAPH_URL',
    required=False,
    default='https://api.openshift.com/api/upgrades_info/v1/graph',
    help="When using --auto-previous, set custom Cincinnati graph URL to query",
)
@click.option(
    "--graph-content-stable",
    metavar='JSON_FILE',
    required=False,
    help="When using --auto-previous, override content from stable channel - primarily for testing",
)
@click.option(
    "--graph-content-candidate",
    metavar='JSON_FILE',
    required=False,
    help="When using --auto-previous, override content from candidate channel - primarily for testing",
)
@click.option(
    "--suggestions-url",
    metavar='SUGGESTIONS_URL',
    required=False,
    default="https://raw.githubusercontent.com/openshift/cincinnati-graph-data/master/build-suggestions/",
    help="When using --auto-previous, set custom suggestions URL, load from {major}-{minor}-{arch}.yaml",
)
@click.option(
    '--output-file', '-o', required=False, help='Specify a file path to write the generated assembly definition to'
)
@click.option(
    "--gen-microshift",
    'gen_microshift',
    default=False,
    is_flag=True,
    help="Create microshift entry for assembly release.",
)
@click.option("--date", metavar="YYYY-MMM-DD", help="Expected release date (e.g. 2020-Nov-25)")
@pass_runtime
@click_coroutine
@click.pass_context
async def gen_assembly_from_releases(
    ctx,
    runtime: Runtime,
    nightlies: Tuple[str, ...],
    standards: Tuple[str, ...],
    custom: bool,
    pre_ga_mode: str,
    in_flight: Optional[str],
    previous_list: Tuple[str, ...],
    auto_previous: bool,
    graph_url: Optional[str],
    graph_content_stable: Optional[str],
    graph_content_candidate: Optional[str],
    suggestions_url: Optional[str],
    output_file: Optional[str],
    gen_microshift: bool,
    date: Optional[str],
):
    # Initialize group config: we need this to determine the canonical builders behavior
    runtime.initialize(config_only=True)

    if runtime.group_config.canonical_builders_from_upstream and runtime.build_system == 'brew':
        runtime.initialize(mode="both", clone_distgits=True, clone_source=False, prevent_cloning=False)
    else:
        runtime.initialize(mode='both', clone_distgits=False, clone_source=False, prevent_cloning=True)

    assembly_def = await GenAssemblyCli(
        runtime=runtime,
        gen_assembly_name=ctx.obj['ASSEMBLY_NAME'],
        nightlies=nightlies,
        standards=standards,
        custom=custom,
        pre_ga_mode=pre_ga_mode,
        in_flight=in_flight,
        previous_list=previous_list,
        auto_previous=auto_previous,
        graph_url=graph_url,
        graph_content_stable=graph_content_stable,
        graph_content_candidate=graph_content_candidate,
        suggestions_url=suggestions_url,
        gen_microshift=gen_microshift,
        release_date=date,
    ).run()

    # ruamel.yaml configuration
    yaml = YAML()
    yaml.default_flow_style = False
    yaml.preserve_quotes = True
    yaml.indent(mapping=2, sequence=4, offset=2)
    yaml.width = 4096

    def represent_datetime(dumper, data):
        return dumper.represent_scalar('tag:yaml.org,2002:str', data.isoformat(), style='"')

    yaml.representer.add_representer(datetime, represent_datetime)

    yaml.dump(assembly_def, sys.stdout)
    if output_file:
        with open(output_file, 'w') as file:
            yaml.dump(assembly_def, file)


class GenAssemblyCli:
    def __init__(
        self,
        # leave these all optional to make testing easier
        runtime: Runtime = None,
        gen_assembly_name: str = '',
        nightlies: Tuple[str, ...] = [],
        standards: Tuple[str, ...] = [],
        custom: bool = False,
        pre_ga_mode: str = '',
        in_flight: Optional[str] = None,
        previous_list: Tuple[str, ...] = None,
        auto_previous: bool = False,
        graph_url: Optional[str] = None,
        graph_content_stable: Optional[str] = None,
        graph_content_candidate: Optional[str] = None,
        suggestions_url: Optional[str] = None,
        gen_microshift: bool = False,
        release_date: Optional[str] = None,
    ):
        self.runtime = runtime
        # The name of the assembly we are going to output
        self.gen_assembly_name = gen_assembly_name
        self.nightlies = nightlies
        self.standards = standards
        self.custom = custom
        self.pre_ga_mode = pre_ga_mode
        self.in_flight = in_flight
        self.previous_list = previous_list
        self.auto_previous = auto_previous
        self.graph_url = graph_url
        self.graph_content_stable = graph_content_stable
        self.graph_content_candidate = graph_content_candidate
        self.suggestions_url = suggestions_url
        self.logger = self.runtime.logger
        self.release_pullspecs: Dict[str, str] = dict()
        self.gen_microshift = gen_microshift
        # Maps brew arch name to nightly name
        self.reference_releases_by_arch: Dict[str, str] = dict()
        # Maps RHCOS container name(s) to brew arch name to pullspec(s) from nightly
        self.rhcos_by_tag: Dict[str, Dict[str, str]] = dict()
        # Maps component package_name to brew build dict found for nightly
        self.component_image_builds: Dict[str, BuildRecordInspector] = dict()
        # Dict[ package_name ] -> Dict[ el? ] -> brew build dict
        self.component_rpm_builds: Dict[str, Dict[int, Dict]] = dict()
        self.basis_event: int = 0
        self.basis_event_ts: float = 0.0
        self.assembly_basis_time: datetime = None  # used by Konflux
        # A set of package_names whose NVRs are not correctly sourced by the estimated basis_event
        self.force_is: Set[str] = set()
        self.primary_rhcos_tag: str = ''
        self.rhcos_version = ''
        self.rhcos_node_id = ''
        self.final_previous_list: List[VersionInfo] = []
        self.release_date = release_date

        # Infer assembly type
        self.assembly_type = util.infer_assembly_type(self.custom, self.gen_assembly_name)

        # Load releases_config
        self.releases_config = self.runtime.get_releases_config()

        # Check if OCP should have the new release versioning scheme (i.e. 4.20.0-0)
        self.includes_release_version = getattr(self.runtime.group_config, 'new_payload_versioning_scheme', None)

        # Create a map of package_name to RPMMetadata
        self.package_rpm_meta: Dict[str, RPMMetadata] = {
            rpm_meta.get_package_name(): rpm_meta for rpm_meta in self.runtime.rpm_metas()
        }

        # ECs are always prerelease
        if self.assembly_type == AssemblyTypes.PREVIEW:
            self.pre_ga_mode = 'prerelease'

        # Microshift should always be built and its advisory prepared for preview and candidate assemblies
        # which will eventually go out at GA time
        if self.assembly_type in [AssemblyTypes.PREVIEW, AssemblyTypes.CANDIDATE]:
            self.gen_microshift = True

        # Bind Konflux DB to the "builds" table
        self.runtime.konflux_db.bind(KonfluxBuildRecord)

        self.package_rpm_finder = PackageRpmFinder(runtime)

    async def run(self):
        self._validate_params()
        self._get_release_pullspecs()
        await self._select_images()
        self._get_rhcos_container()
        await self._select_rpms()
        self._calculate_previous_list()
        return self._generate_assembly_definition()

    @staticmethod
    def _exit_with_error(msg):
        print(msg, file=sys.stderr)
        exit(1)

    def _validate_params(self):
        if self.runtime.assembly != 'stream':
            self._exit_with_error(
                '--assembly must be "stream" in order to populate an assembly definition from nightlies'
            )

        if not self.nightlies and not self.standards:
            self._exit_with_error('At least one release (--nightly or --standard) must be specified')

        if self.auto_previous and self.previous_list:
            self._exit_with_error('Cannot use `--previous` and `--auto-previous` at the same time.')

        if self.assembly_type in [AssemblyTypes.CUSTOM]:
            if self.auto_previous or self.previous_list or self.in_flight:
                self._exit_with_error("Custom releases don't have previous list.")

        if self.pre_ga_mode == "prerelease" and self.assembly_type not in [
            AssemblyTypes.PREVIEW,
            AssemblyTypes.CANDIDATE,
        ]:
            self._exit_with_error("Prerelease is only valid for preview and candidate assemblies.")

        if self.assembly_type is AssemblyTypes.STANDARD and self.includes_release_version is True:
            if "-" not in self.gen_assembly_name:
                self._exit_with_error(
                    f"Invalid assembly name: {self.gen_assembly_name}. Has to include release field. Eg: 4.18.0-0"
                )

    def _get_release_pullspecs(self):
        for nightly_name in self.nightlies:
            major_minor, brew_cpu_arch, priv = util.isolate_nightly_name_components(nightly_name)
            if major_minor != self.runtime.get_minor_version():
                self._exit_with_error(f'Specified nightly {nightly_name} does not match group major.minor')
            self.reference_releases_by_arch[brew_cpu_arch] = nightly_name
            rc_suffix = go_suffix_for_arch(brew_cpu_arch, priv)

            if (
                self.runtime.build_system == 'konflux'
                and self.runtime.group.removeprefix('openshift-') not in KONFLUX_IMAGESTREAM_OVERRIDE_VERSIONS
            ):
                release_suffix = f'konflux-release{rc_suffix}'
            else:
                release_suffix = f'release{rc_suffix}'
            nightly_pullspec = f'registry.ci.openshift.org/ocp{rc_suffix}/{release_suffix}:{nightly_name}'
            if brew_cpu_arch in self.release_pullspecs:
                raise ValueError(
                    f'Cannot process {nightly_name} since {self.release_pullspecs[brew_cpu_arch]} is already included'
                )
            self.release_pullspecs[brew_cpu_arch] = nightly_pullspec

        for standard_release_name in self.standards:
            version, brew_cpu_arch = standard_release_name.split('-')  # 4.7.22-s390x => ['4.7.22', 's390x']
            major_minor = '.'.join(
                version.split('.')[:2]
            )  # isolate just x.y from version names like '4.77.22' and '4.8.0-rc.3'
            if major_minor != self.runtime.get_minor_version():
                self._exit_with_error(f'Specified release {standard_release_name} does not match group major.minor')
            standard_pullspec = f'quay.io/openshift-release-dev/ocp-release:{standard_release_name}'
            if brew_cpu_arch in self.release_pullspecs:
                raise ValueError(
                    f'Cannot process {standard_release_name} since '
                    f'{self.release_pullspecs[brew_cpu_arch]} is already included'
                )
            self.release_pullspecs[brew_cpu_arch] = standard_pullspec

    async def _select_images(self):
        await self._determine_basis_brew_event()
        await self._collect_outliers()

    @exectools.limit_concurrency(500)
    async def _process_release(self, brew_cpu_arch, pullspec, rhcos_tag_names):
        self.runtime.logger.info(f'Processing release: {pullspec}')

        # Use shared utility to introspect release
        release_info = await release_inspector.introspect_release(pullspec)

        if not release_info["displayVersions"]["machine-os"]["Version"]:
            self._exit_with_error(f'Could not find machine-os version in release: {pullspec}')
        # get rhcos version eg. 417.94.202410250757-0
        self.rhcos_version = release_info["displayVersions"]["machine-os"]["Version"]

        # Only look at payload tags that were included in the invocation to speed up testing
        payload_names = [meta.get_payload_tag_info()[0] for meta in self.runtime.image_metas() if meta.is_payload]
        payload_names.extend(rhcos_tag_names)

        for component_tag in release_info.references.spec.tags:
            if component_tag['name'] not in payload_names:
                continue
            payload_tag_name = component_tag.name  # e.g. "aws-ebs-csi-driver"
            payload_tag_pullspec = component_tag['from'].name  # quay pullspec
            image_info = await util.oc_image_info_for_arch_async(
                payload_tag_pullspec, go_arch_for_brew_arch(brew_cpu_arch)
            )
            if payload_tag_name in rhcos_tag_names:
                self.runtime.logger.info(f'Record rhcos tag name {payload_tag_name}')
                self.rhcos_by_tag.setdefault(payload_tag_name, {})[brew_cpu_arch] = payload_tag_pullspec
                if "coreos.build.manifest-list-tag" in image_info['config']['config']['Labels']:
                    # 4.19-9.6-202504230114-node-image => 202504230114
                    self.rhcos_node_id = image_info['config']['config']['Labels'][
                        'coreos.build.manifest-list-tag'
                    ].split("-")[2]
                    self.runtime.logger.info(f'Find rhcos node image id {self.rhcos_node_id}')
                continue

            # Use shared utility to extract NVR and get build inspector
            name, version, release_ver = await release_inspector.extract_nvr_from_pullspec(payload_tag_pullspec)
            package_name = name
            build_nvr = f"{name}-{version}-{release_ver}"

            # For Konflux, use shared utility to get build inspector
            build_inspector = await release_inspector.get_build_inspector_from_nvr(self.runtime, build_nvr)

            if package_name in self.component_image_builds:
                # If we have already encountered this package once in the list of releases we are
                # processing, then make sure that the original NVR we found matches the new NVR.
                # We want the releases to be populated with identical builds.
                existing_nvr = self.component_image_builds[package_name].get_nvr()
                if build_nvr != existing_nvr:
                    # Ignore possible outdated payload tag if one is explicitly defined
                    image_meta = build_inspector.get_image_meta()
                    payload_name, explicit = image_meta.get_payload_tag_info()
                    if explicit:
                        if payload_name != payload_tag_name:
                            self.logger.warning(
                                f'Ignoring payload tag {payload_tag_name} since payload_name={payload_name} is '
                                'explicitly defined in image config'
                            )
                            continue
                        else:
                            # override the build for this package
                            self.logger.warning(
                                f'Selecting payload tag {payload_tag_name} since payload_name={payload_name} is '
                                'explicitly defined in image config'
                            )
                            self.component_image_builds[package_name] = build_inspector
                    else:
                        self._exit_with_error(
                            'Found disparate nvrs between releases; '
                            f'{existing_nvr} in processed and {build_nvr} in {pullspec}'
                        )
            else:
                # Otherwise, record the build as the first time we've seen an NVR for this
                # package.
                self.component_image_builds[package_name] = build_inspector

            # We now try to determine a basis brew event that will
            # find this image during get_latest_build-like operations
            # for the assembly. At the time of this writing, metadata.get_latest_build
            # will only look for builds *completed* before the basis event. This could
            # be changed to *created* before the basis event in the future. However,
            # other logic that is used to find latest builds requires the build to be
            # tagged into an rhaos tag before the basis event.
            # To choose a safe / reliable basis brew event, we first find the
            # time at which a build was completed, then add 5 minutes.
            # That extra 5 minutes ensures brew will have had time to tag the
            # build appropriately for its build target. The 5 minutes is also
            # short enough to ensure that no other build of this image could have
            # completed before the basis event.

            if self.runtime.build_system == 'brew':
                completion_ts: float = build_inspector.get_build_obj()['completion_ts']
                # If the basis event for this image is > the basis_event capable of
                # sweeping images we've already analyzed, increase the basis_event_ts.
                self.basis_event_ts = max(self.basis_event_ts, completion_ts + (60.0 * 5))
            else:
                if not self.assembly_basis_time:
                    self.assembly_basis_time = build_inspector.get_build_end_time()
                else:
                    self.assembly_basis_time = max(self.assembly_basis_time, build_inspector.get_build_end_time())

    async def _determine_basis_brew_event(self):
        rhcos_tag_names = rhcos.get_container_names(self.runtime)
        await asyncio.gather(
            *[
                self._process_release(brew_cpu_arch, pullspec, rhcos_tag_names)
                for brew_cpu_arch, pullspec in self.release_pullspecs.items()
            ]
        )

        # basis_event_ts (for Brew) or assembly_basis_time (for Konflux) should now be greater than the build completion
        # / target tagging operation for any (non RHCOS) image in the nightlies. Because images are built after RPMs,
        # it must also hold that the basis_event_ts is also greater than build completion & tagging
        # of any member RPM.

        if self.runtime.build_system == 'brew':
            # Let's turn the approximate basis_event_ts into a brew event number
            with self.runtime.shared_koji_client_session() as koji_api:
                self.basis_event = koji_api.getLastEvent(before=self.basis_event_ts)['id']
            self.logger.info(
                'The following image package_names were detected in the specified releases: %s',
                self.component_image_builds.keys(),
            )
        else:
            # For konflux, compute the latest Brew event that came before the assembly time
            with self.runtime.shared_koji_client_session() as koji_api:
                self.basis_event = brew_event_from_datetime(self.assembly_basis_time, koji_api)

        self.logger.info(f'Estimated basis event: {self.basis_event}')

    async def _collect_outliers(self):
        """
        Things happen. Let's say image component X was built in build X1 and X2.
        Image component Y was build in Y1. Let's say that the ordering was X1, X2, Y1 and, for
        whatever reason, we find X1 and Y1 in the user specified nightly. This means the basis_event_ts
        we find for Y1 is going to find X2 instead of X1 if we used it as part of an assembly's basis event.

        To avoid that, we now evaluate whether any images or RPMs defy our assumption that the nightly
        corresponds to the basis_event_ts we have calculated. If we find something that will not be swept
        correctly by the estimated basis event, we collect up the outliers (hopefully few in number) into
        a list of packages which must be included in the assembly as 'is:'. This might happen if, for example,
        an artist accidentally builds an image on the command line for the stream assembly; without this logic,
        that build might be found by our basis event, but we will explicitly pin to the image in the nightly
        component's NVR as an override in the assembly definition.
        """

        for image_meta in self.runtime.image_metas():
            if image_meta.base_only or not image_meta.for_release:
                continue

            dgk = image_meta.distgit_key

            package_name = image_meta.get_component_name()
            if self.runtime.build_system == 'brew':
                basis_event_dict = image_meta.get_latest_brew_build(
                    default=None, complete_before_event=self.basis_event
                )
            else:
                basis_event_dict = await image_meta.get_latest_konflux_build(
                    default=None, completed_before=self.assembly_basis_time
                )

            if not basis_event_dict:
                self._exit_with_error(
                    f'No image was found for assembly {self.runtime.assembly} for component {dgk} '
                    f'at estimated brew event {self.basis_event}. No normal reason for this to '
                    f'happen so exiting out of caution.'
                )

            if self.runtime.build_system == 'brew':
                basis_event_build_dict: BuildRecordInspector = BrewBuildRecordInspector(
                    self.runtime, basis_event_dict['id']
                )
            else:
                basis_event_build_dict: KonfluxBuildRecordInspector = KonfluxBuildRecordInspector(
                    self.runtime, basis_event_dict
                )
            basis_event_build_nvr = basis_event_build_dict.get_nvr()

            if not image_meta.is_payload:
                # If this is not for the payload, the nightlies cannot have informed our NVR decision; just
                # pick whatever the estimated basis will pull and let the user know. If they want to change
                # it, they will need to pin it.
                self.logger.info(
                    f'{dgk} non-payload build {basis_event_build_nvr} will be swept by estimated assembly basis event'
                )
                self.component_image_builds[package_name] = basis_event_build_dict
                continue

            # Otherwise, the image_meta is destined for the payload and analyzing the nightlies should
            # have given us an NVR which is expected to be selected by the assembly.

            if package_name not in self.component_image_builds:
                if self.custom:
                    self.logger.warning(
                        'Unable to find %s in releases despite it being marked as is_payload '
                        'in ART metadata; this may be because the image is not built for every arch '
                        'or it is not labeled appropriately for the payload. '
                        'Choosing what was in the estimated basis event sweep: %s',
                        dgk,
                        basis_event_build_nvr,
                    )
                else:
                    self.logger.error(
                        'Unable to find %s in releases despite it being marked as is_payload '
                        'in ART metadata; this may mean the image does not have the proper labeling for '
                        'being in the payload. Choosing what was in the estimated basis event sweep: %s',
                        dgk,
                        basis_event_build_nvr,
                    )
                self.component_image_builds[package_name] = basis_event_build_dict
                continue

            ref_releases_component_build = self.component_image_builds[package_name]
            ref_nightlies_component_build_nvr = ref_releases_component_build.get_nvr()

            if basis_event_build_nvr != ref_nightlies_component_build_nvr:
                self.logger.info(
                    '%s build %s was selected by estimated basis event. That is not what is in the '
                    'specified releases, so this image will be pinned.',
                    dgk,
                    basis_event_build_nvr,
                )
                self.force_is.add(package_name)
                continue

            # Otherwise, the estimated basis event resolved the image nvr we found in the nightlies. The
            # image NVR does not need to be pinned. Yeah!

    def _get_rhcos_container(self):
        # We should have found an RHCOS container for each architecture in the group for a standard assembly
        self.primary_rhcos_tag = rhcos.get_primary_container_name(self.runtime)
        major_minor = self.runtime.get_minor_version()
        rhcos_el_major = self.runtime.group_config.vars.RHCOS_EL_MAJOR
        rhcos_el_minor = self.runtime.group_config.vars.RHCOS_EL_MINOR

        for arch in self.runtime.group_config.arches:
            if arch in self.rhcos_by_tag[self.primary_rhcos_tag]:
                continue

            if self.custom:
                # This is permitted for custom assemblies which do not need to be assembled for every
                # architecture. The customer may just need x86_64.
                self.logger.info(
                    'Did not find RHCOS "%s" image for active group architecture: %s; '
                    'ignoring for custom assembly type.',
                    self.primary_rhcos_tag,
                    arch,
                )
            else:
                if not self.rhcos_version:
                    self._exit_with_error(
                        f"Did not find RHCOS {self.primary_rhcos_tag} image for active group architecture: {arch}"
                    )
                # get rhcos pullspecs for this arch from rhcos version value if not full arch nightly provided
                for tag in rhcos.get_container_configs(self.runtime):
                    if self.rhcos_by_tag[tag.name].get(arch):
                        continue
                    if self.runtime.group_config.rhcos.get("layered_rhcos", False):
                        if not self.rhcos_by_tag[tag.name].get("x86_64"):
                            self._exit_with_error(
                                f"Did not find RHCOS {tag.name} image for architecture: x86_64 in any nightly"
                            )
                        amd64_rhcos_info = util.oc_image_info_for_arch(self.rhcos_by_tag[tag.name]["x86_64"], "amd64")
                        rhcos_info = util.oc_image_info_for_arch(
                            f"{ART_PROD_IMAGE_REPO}:{amd64_rhcos_info['config']['config']['Labels']['coreos.build.manifest-list-tag']}",
                            go_arch_for_brew_arch(arch),
                        )
                        self.rhcos_by_tag[tag.name][arch] = (
                            f"quay.io/openshift-release-dev/ocp-v4.0-art-dev@{rhcos_info['digest']}"
                        )
                    else:
                        url_key = (
                            f"{major_minor}-{rhcos_el_major}.{rhcos_el_minor}" if rhcos_el_major > 8 else major_minor
                        )
                        rhcos_build_url = (
                            f"{RHCOS_RELEASES_STREAM_URL}/{url_key}/builds/{self.rhcos_version}/{arch}/meta.json"
                        )
                        session = requests.Session()
                        session.mount('https://', HTTPAdapter(max_retries=Retry(total=3)))
                        rhcos_meta_json = session.get(rhcos_build_url).json()
                        if tag.build_metadata_key not in rhcos_meta_json:
                            self._exit_with_error(
                                f'Did not find RHCOS "{tag.name}" image for active group architecture: {arch}'
                            )
                        rhcos_image = rhcos_meta_json[tag.build_metadata_key]
                        self.rhcos_by_tag[tag.name][arch] = f"{rhcos_image['image']}@{rhcos_image['digest']}"
                    self.logger.info(f'Find RHCOS image {tag.name} for {arch}: {self.rhcos_by_tag[tag.name][arch]}')

    async def _select_rpms(self):
        """
        We now have a list of image builds that should be selected by the assembly basis event
        and those that will need to be forced with 'is'. We now need to perform a similar step
        for RPMs. Look at the image contents, see which RPMs are in use. If we build them,
        then the NVRs in the image must be selected by the estimated basis event. If they are
        not, then we must pin the NVRs in the assembly definition.
        """

        with self.runtime.shared_koji_client_session() as koji_api:
            if self.runtime.build_system == 'brew':
                archive_lists = brew.list_archives_by_builds(
                    build_ids=[b.get_build_id() for b in self.component_image_builds.values()],
                    build_type="image",
                    session=koji_api,
                )
                rpm_build_ids = {rpm["build_id"] for archives in archive_lists for ar in archives for rpm in ar["rpms"]}
            else:
                rpm_build_dicts = []
                for bri in self.component_image_builds.values():
                    rpm_build_dicts.extend(self.package_rpm_finder.get_brew_rpms_from_build_record(bri.get_build_obj()))
                rpm_build_ids = {rpm["build_id"] for rpm in rpm_build_dicts}

            self.logger.info("Querying Brew build information for %s RPM builds...", len(rpm_build_ids))
            # We now have a list of all RPM builds which have been installed into the various images which
            # ART builds. Specifically the ART builds which went into composing the nightlies.
            ref_releases_rpm_builds: List[Dict] = brew.get_build_objects(rpm_build_ids, koji_api)

            for ref_releases_rpm_build in ref_releases_rpm_builds:
                package_name = ref_releases_rpm_build['package_name']
                if package_name not in self.package_rpm_meta:  # Does ART build this package?
                    continue

                rpm_meta = self.package_rpm_meta[package_name]
                dgk = rpm_meta.distgit_key
                rpm_build_nvr = ref_releases_rpm_build['nvr']

                # If so, what RHEL version is this build for?
                el_ver = isolate_el_version_in_release(ref_releases_rpm_build['release'])
                if not el_ver:
                    self._exit_with_error(f'Unable to isolate el? version in {rpm_build_nvr}')

                if package_name not in self.component_rpm_builds:
                    # If this is the first time we've seen this ART package, bootstrap a dict for its
                    # potentially different builds for different RHEL versions.
                    self.component_rpm_builds[package_name]: Dict[int, Dict] = dict()

                if el_ver in self.component_rpm_builds[package_name]:
                    # We've already captured the build in our results
                    continue

                # Now it is time to see whether a query for the RPM from the basis event
                # estimate comes up with this RPM NVR.
                basis_event_build_dict = rpm_meta.get_latest_brew_build(
                    el_target=el_ver, complete_before_event=self.basis_event
                )

                if not basis_event_build_dict:
                    self._exit_with_error(
                        f'No RPM was found for assembly {self.runtime.assembly} for component {dgk} '
                        f'at estimated brew event {self.basis_event}. No normal reason for this to '
                        f'happen so exiting out of caution.'
                    )

                if el_ver in self.component_rpm_builds[package_name]:
                    # We've already logged a build for this el version before
                    continue

                self.component_rpm_builds[package_name][el_ver] = ref_releases_rpm_build
                basis_event_build_nvr = basis_event_build_dict['nvr']
                self.logger.info(f'{dgk} build {basis_event_build_nvr} selected by scan against estimated basis event')

                if basis_event_build_nvr != ref_releases_rpm_build['nvr']:
                    # The basis event estimate did not find the RPM from the nightlies. We have to pin the package.
                    self.logger.info(
                        '%s build %s was selected by estimated basis event. '
                        'That is not what is in the specified releases, so this RPM will be pinned.',
                        dgk,
                        basis_event_build_nvr,
                    )
                    self.force_is.add(package_name)

    def _calculate_previous_list(self):
        final_previous_list: Set[VersionInfo] = set()
        if self.in_flight:
            final_previous_list.add(VersionInfo.parse(self.in_flight))
        if self.previous_list:
            final_previous_list |= set(map(VersionInfo.parse, self.previous_list))
        elif self.auto_previous:
            # gen_assembly_name should be in the form of `ec.0`, `fc.0`, `rc.1`, or `4.10.1`
            if self.assembly_type == AssemblyTypes.CANDIDATE or self.assembly_type == AssemblyTypes.PREVIEW:
                major_minor = self.runtime.get_minor_version()  # x.y
                # Support both 4.19+ and lower versions (former has release version while the latter does not)
                version = (
                    f"{major_minor}.0-0.{self.gen_assembly_name}"
                    if self.includes_release_version
                    else f"{major_minor}.0-{self.gen_assembly_name}"
                )
            else:
                version = self.gen_assembly_name
            for arch in self.runtime.arches:
                self.logger.info("Calculating previous list for %s", arch)
                previous_list = util.get_release_calc_previous(
                    version,
                    arch,
                    self.graph_url,
                    self.graph_content_stable,
                    self.graph_content_candidate,
                    self.suggestions_url,
                )
                final_previous_list |= set(map(VersionInfo.parse, previous_list))
        self.final_previous_list = sorted(final_previous_list)

    def _get_advisories_release_jira(self) -> Tuple[Dict[str, int], str]:
        # Add placeholder advisory numbers and JIRA key.
        # Those values will be replaced with real values by pyartcd when preparing a release.
        preGA_advisory_type = ['prerelease']

        if self.runtime.build_system == 'brew':
            advisories = {
                'image': -1,
                'rpm': -1,
                'extras': -1,
                'metadata': -1,
            }
            for key in preGA_advisory_type:
                if self.pre_ga_mode == key:
                    advisories[key] = -1
        else:  # konflux
            advisories = {
                'rpm': -1,
                'rhcos': -1,
            }
            # for konflux, prerelease advisories are noted in the `shipment` field.
            # No need to add it to the advisories map.

        # For OCP >= 4.14, also microshift advisory placeholder must be created
        major, minor = self.runtime.get_major_minor_fields()
        if (major, minor) >= (4, 14) and self.gen_microshift:
            advisories['microshift'] = -1

        release_jira = "ART-0"

        if self.assembly_type not in [AssemblyTypes.PREVIEW, AssemblyTypes.CANDIDATE]:
            # For standalone assembly, if the advisories and jira already exist, reuse them
            if self.gen_assembly_name in self.releases_config.releases:
                advisories = self.releases_config.releases[self.gen_assembly_name].assembly.group.advisories.primitive()
                release_jira = self.releases_config.releases[self.gen_assembly_name].assembly.group.release_jira
            return advisories, release_jira

        # if this assembly is (e|r)c.X, then check if there is a previously defined (e|r)c.X-1
        # or if this assembly is rc.0 then check if there is a previously defined ec.X
        # pick advisories and release ticket from there

        split = re.split(r'(\d+)', self.gen_assembly_name)  # ['rc.', '2', '']
        current_v = int(split[1])

        if self.assembly_type == AssemblyTypes.PREVIEW and current_v == 0:
            return advisories, release_jira

        # For RCs
        if current_v == 0 and self.assembly_type == AssemblyTypes.CANDIDATE:
            ec_assemblies = sorted([a for a in self.releases_config.releases if a.startswith("ec.")])
            if not ec_assemblies:
                self.logger.info("No matching previous assembly found")
                return advisories, release_jira
            previous_assembly = ec_assemblies[-1]
        else:
            previous_assembly = f"{split[0]}{current_v - 1}"
            if previous_assembly not in self.releases_config.releases:
                self.logger.info("No matching previous assembly found")
                return advisories, release_jira

        previous_group = self.releases_config.releases[previous_assembly].assembly.group
        previous_advisories = previous_group.advisories.primitive()

        # preGA advisories (prerelease) associated with an assembly should not be reused
        # they should be shipped or dropped if not shipping
        for key in preGA_advisory_type:
            previous_advisories.pop(key, None)

        advisories.update(previous_advisories)

        release_jira = previous_group.release_jira
        self.logger.info(
            f"Reusing advisories and release ticket from previous assembly {previous_assembly}, {advisories}, {release_jira}"
        )

        # If build system is konflux, make sure that old-style advisories are not present for image, extras and metadata
        if self.runtime.build_system == 'konflux':
            advisories.pop('image', None)
            advisories.pop('extras', None)
            advisories.pop('metadata', None)

        return advisories, release_jira

    def _generate_assembly_definition(self) -> dict:
        image_member_overrides, rpm_member_overrides = self._get_member_overrides()

        group_info = {}
        if self.custom:
            # Custom payloads don't require advisories.
            # If the user has specified fewer nightlies than is required by this
            # group, then we need to override the group arches.
            group_info = {
                'arches!': list(self.rhcos_by_tag[self.primary_rhcos_tag].keys()),
            }

        if self.assembly_type != AssemblyTypes.CUSTOM:
            group_info['advisories'], group_info["release_jira"] = self._get_advisories_release_jira()
            if self.pre_ga_mode == 'prerelease':
                group_info['operator_index_mode'] = 'pre-release'

        if self.runtime.build_system == 'konflux':
            group_info['shipment'] = self._get_shipment_info()
            group_info['release_date'] = self._get_release_date()

        if self.final_previous_list:
            group_info['upgrades'] = ','.join(map(str, self.final_previous_list))

        basis_event_key = 'brew_event' if self.runtime.build_system == 'brew' else 'time'
        basis_event_value = self.basis_event if self.runtime.build_system == 'brew' else self.assembly_basis_time

        return {
            'releases': {
                self.gen_assembly_name: {
                    "assembly": {
                        'type': self.assembly_type.value,
                        'basis': {
                            basis_event_key: basis_event_value,
                            'reference_releases': self.reference_releases_by_arch,
                        },
                        'group': group_info,
                        'rhcos': {
                            tag: dict(images={arch: pullspec for arch, pullspec in specs_by_arch.items()})
                            for tag, specs_by_arch in self.rhcos_by_tag.items()
                        },
                        'members': {
                            'rpms': rpm_member_overrides,
                            'images': image_member_overrides,
                        },
                    },
                },
            },
        }

    def _get_release_date(self):
        if self.release_date:
            return self.release_date
        if self.assembly_type != AssemblyTypes.STANDARD:
            raise ValueError("For non standard release you need to manually set release date from job")
        self.logger.info("Release date not provided. Fetching release date from release schedule...")
        try:
            self.release_date = get_assembly_release_date(self.gen_assembly_name, self.runtime.group)
        except Exception as ex:
            raise ValueError(f"Failed to fetch release date from release schedule for {self.gen_assembly_name}: {ex}")
        self.logger.info("Release date: %s", self.release_date)
        return self.release_date

    def _get_member_overrides(self):
        """
        self.component_image_builds now contains a mapping of package_name -> BuildRecordInspector
        for all images that should be included in the assembly.

        self.component_rpm_builds now contains a mapping of package_name to different RHEL versions
        that should be included in the assembly.

        self.force_is is a set of package_names which were not successfully selected by the estimated basis event.
        """

        image_member_overrides: List[Dict] = []
        rpm_member_overrides: List[Dict] = []

        for package_name in self.force_is:
            if package_name in self.component_image_builds:
                build_inspector: BrewBuildRecordInspector = self.component_image_builds[package_name]
                dgk = build_inspector.get_image_meta().distgit_key
                image_member_overrides.append(
                    {
                        'distgit_key': dgk,
                        'why': 'Query from assembly basis event failed to replicate '
                        'referenced nightly content exactly. Pinning to replicate.',
                        'metadata': {
                            'is': {
                                'nvr': build_inspector.get_nvr(),
                            },
                        },
                    }
                )
            elif package_name in self.component_rpm_builds:
                dgk = self.package_rpm_meta[package_name].distgit_key
                rpm_member_overrides.append(
                    {
                        'distgit_key': dgk,
                        'why': 'Query from assembly basis event failed to replicate '
                        'referenced nightly content exactly. Pinning to replicate.',
                        'metadata': {
                            'is': {
                                f'el{el_ver}': self.component_rpm_builds[package_name][el_ver]['nvr']
                                for el_ver in self.component_rpm_builds[package_name]
                            },
                        },
                    }
                )

        return image_member_overrides, rpm_member_overrides

    def _get_default_shipment(self, env: str = None) -> dict:
        """
        Get the default assembly.group.shipment template for an assembly.
        """

        # Note: env=prod means shipment will be pushed to stage and prod
        # env=stage means shipment will be pushed to stage only
        # We do not want to set 'prod' env explicitly, as it is the default environment
        # and mentioning it in every assembly definition is redundant
        if env and env not in ['stage', 'prod']:
            raise ValueError(f"Invalid environment: {env}")

        default_shipment = {
            'advisories': [
                {'kind': 'image'},
                {'kind': 'extras'},
                {'kind': 'metadata'},
                {'kind': 'fbc'},
            ],
        }
        if env:
            default_shipment['env'] = env
        if self.pre_ga_mode == 'prerelease':
            default_shipment['advisories'].append({'kind': 'prerelease'})
        return default_shipment

    def _get_previous_shipment_info(self) -> dict:
        """
        if this assembly is (e|r)c.X, then check if there is a previously defined (e|r)c.X-1
        or if this assembly is rc.0 then check if there is a previously defined ec.X
        pick shipment info from there
        """

        split = re.split(r'(\d+)', self.gen_assembly_name)  # ['rc.', '2', '']
        current_v = int(split[1])

        # For EC.0, this is the first shipment info
        # EC and RC shipments will target stage environment
        if self.assembly_type == AssemblyTypes.PREVIEW and current_v == 0:
            return self._get_default_shipment(env="stage")

        # For RC.0, inherit from last EC
        if current_v == 0 and self.assembly_type == AssemblyTypes.CANDIDATE:
            ec_assemblies = sorted([a for a in self.releases_config.releases if a.startswith("ec.")])
            if not ec_assemblies:
                self.logger.warning("No matching previous assembly found")
                return self._get_default_shipment(env="stage")
            previous_assembly = ec_assemblies[-1]

        # For other cases, inherit from previous assembly
        else:
            previous_assembly = f"{split[0]}{current_v - 1}"
            if previous_assembly not in self.releases_config.releases:
                self.logger.warning("No matching previous assembly found")
                return self._get_default_shipment(env="stage")

        # Return previous assembly shipment info
        previous_shipment = self.releases_config.releases[previous_assembly].assembly.group.shipment
        if previous_shipment is not Missing:
            return previous_shipment.primitive()
        else:
            return self._get_default_shipment(env="stage")

    def _get_shipment_info(self) -> dict:
        """
        For konflux, we need to add shipment information to the assembly definition.
        """
        if self.assembly_type in [AssemblyTypes.PREVIEW, AssemblyTypes.CANDIDATE]:
            shipment = self._get_previous_shipment_info()
            # If previous shipment has MR url, remove it
            # as it is not relevant for the new assembly.
            shipment.pop('url', None)
        elif self.assembly_type != AssemblyTypes.CUSTOM:
            shipment = self._get_default_shipment()
            advisories = shipment['advisories']
            # If shipment advisories already exist, reuse them
            if self.gen_assembly_name in self.releases_config.releases:
                release = self.releases_config.releases[self.gen_assembly_name]
                previous_advisories = {ad["kind"]: ad for ad in release.assembly.group.shipment.advisories.primitive()}
                if previous_advisories:
                    for advisory in advisories:
                        # preGA advisories (prerelease) associated with an assembly should not be reused
                        # they should be shipped or dropped if not shipping
                        if advisory["kind"] == "prerelease":
                            continue
                        # Reuse advisory if it exists in previous advisories
                        previous_ad = previous_advisories.get(advisory["kind"])
                        if not previous_ad:
                            continue
                        advisory.update(previous_ad)
        return shipment
