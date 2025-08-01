import asyncio
import hashlib
import json
import os
import sys
import traceback
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, NamedTuple, Optional, Set, Tuple, cast
from unittest.mock import MagicMock

import aiofiles
import click
import nest_asyncio
import openshift_client as oc
import yaml
from artcommonlib import exectools, rhcos
from artcommonlib.arch_util import brew_arch_for_go_arch, go_arch_for_brew_arch, go_suffix_for_arch
from artcommonlib.assembly import AssemblyIssue, AssemblyIssueCode, AssemblyTypes, assembly_basis
from artcommonlib.constants import KONFLUX_IMAGESTREAM_OVERRIDE_VERSIONS
from artcommonlib.exectools import manifest_tool
from artcommonlib.format_util import red_print
from artcommonlib.konflux.package_rpm_finder import PackageRpmFinder
from artcommonlib.model import Model
from artcommonlib.rhcos import RhcosMissingContainerException
from artcommonlib.rpm_utils import parse_nvr
from artcommonlib.telemetry import start_as_current_span_async
from artcommonlib.util import convert_remote_git_to_https
from elliottlib.util import chunk
from opentelemetry import trace
from tenacity import retry, stop_after_attempt, wait_fixed

from doozerlib.assembly_inspector import AssemblyInspector
from doozerlib.brew import KojiWrapperMetaReturn
from doozerlib.build_info import BuildRecordInspector, ImageInspector
from doozerlib.cli import cli, click_coroutine, pass_runtime
from doozerlib.exceptions import DoozerFatalError
from doozerlib.image import ImageMetadata
from doozerlib.rhcos import RHCOSBuildInspector
from doozerlib.runtime import Runtime
from doozerlib.util import (
    extract_version_fields,
    find_manifest_list_sha,
    isolate_nightly_name_components,
    what_is_in_master,
)

TRACER = trace.get_tracer(__name__)


class RepositoryType(Enum):
    PRIVATE = 0
    PUBLIC = 1


@cli.command("release:gen-payload", short_help="Mirror release images to quay and release-controller")
@click.option(
    "--is-name", metavar="NAME", required=False, help="ImageStream .metadata.name value. For example '4.2-art-latest'"
)
@click.option(
    "--is-namespace",
    metavar="NAMESPACE",
    required=False,
    help="ImageStream .metadata.namespace value. For example 'ocp'",
)
@click.option(
    "--organization",
    metavar="ORGANIZATION",
    required=False,
    default="openshift-release-dev",
    help="Quay ORGANIZATION to mirror into.\ndefault=openshift-release-dev",
)
@click.option(
    "--repository",
    metavar="REPO",
    required=False,
    default="ocp-v4.0-art-dev",
    help="Quay REPOSITORY in ORGANIZATION to mirror into.\ndefault=ocp-v4.0-art-dev",
)
@click.option(
    "--private-repository",
    metavar="REPO",
    required=False,
    default="ocp-v4.0-art-dev-priv",
    help="Private Quay REPOSITORY in ORGANIZATION to mirror into.\ndefault=ocp-v4.0-art-dev-priv",
)
@click.option(
    "--release-repository",
    metavar="REPO",
    required=False,
    default="ocp-release-nightly",
    help="Quay REPOSITORY in ORGANIZATION to push release payloads (used for multi-arch)\ndefault=ocp-release-nightly",
)
@click.option(
    "--output-dir",
    metavar="DIR",
    required=False,
    default=".",
    help="Directory into which the mirroring/imagestream artifacts should be written",
)
@click.option(
    "--skip-gc-tagging",
    default=False,
    is_flag=True,
    help="By default, for a named assembly, images will be tagged to prevent garbage collection",
)
@click.option(
    "--exclude-arch",
    metavar="ARCH",
    required=False,
    multiple=True,
    help="Architecture (brew nomenclature) to exclude from payload generation",
)
@click.option(
    "--emergency-ignore-issues",
    default=False,
    is_flag=True,
    help="If you must get this command to permit an assembly despite issues. Only supported for type: stream. Do not use without approval.",
)
@click.option("--apply", default=False, is_flag=True, help="Perform mirroring and imagestream updates.")
@click.option(
    "--apply-multi-arch",
    default=False,
    is_flag=True,
    help="Also create a release payload for multi-arch/heterogeneous clusters.",
)
@click.option(
    "--moist-run",
    default=False,
    is_flag=True,
    help="Mirror and determine tags but do not actually update imagestreams.",
)
@click.option(
    "--embargo-permit-ack",
    default=False,
    is_flag=True,
    help="Allow embargoed builds to sync publicly in named assemblies",
)
@click_coroutine
@pass_runtime
async def release_gen_payload(
    runtime: Runtime,
    is_name: str,
    is_namespace: str,
    organization: str,
    repository: str,
    private_repository: str,
    release_repository: str,
    output_dir: str,
    exclude_arch: Tuple[str, ...],
    skip_gc_tagging: bool,
    emergency_ignore_issues: bool,
    apply: bool,
    apply_multi_arch: bool,
    moist_run: bool,
    embargo_permit_ack: bool,
):
    """
Computes a set of imagestream tags which can be assembled into an OpenShift release for this
assembly. The tags may not be valid unless --apply or --moist-run triggers mirroring.

Applying the change will cause the OSBS images to be mirrored into the OpenShift release
repositories on quay.

Applying will also directly update the imagestreams relevant to assembly (e.g. updating
4.9-art-latest for 4.9's stream assembly).

You may provide the namespace and base name for the image streams, or defaults will be used.

The ORGANIZATION and REPOSITORY options are combined into ORGANIZATION/REPOSITORY when preparing for
mirroring.

Generate files for mirroring from registry-proxy (OSBS storage) to our quay registry:

\b
    $ doozer --group=openshift-4.12 release:gen-payload \\
        --is-name=4.12-art-latest

Note that if you use -i to include specific images, you should also include openshift-enterprise-pod
to supply the 'pod' tag. The 'pod' image is used automatically as a payload stand-in for images that
do not build on all arches.

## Validation ##

Additionally we want to check that the following conditions are true for each imagestream being
updated:

* For all architectures built, RHCOS builds must have matching versions of any unshipped RPM they
  include (per-entry os metadata - the set of RPMs may differ between arches, but versions should
  not).
* Any RPMs present in images (including RHCOS) from unshipped RPM builds included in one of our
  candidate tags must exactly version-match the latest RPM builds in those candidate tags (ONLY; we
  never flag what we don't directly ship.)

These checks (and likely more in the future) should run and any failures should
be listed in brief via a "release.openshift.io/inconsistency" annotation on the
relevant image istag (these are publicly visible; ref. https://bit.ly/37cseC1)
and in more detail in state.yaml. The release-controller, per ART-2195, will
read and propagate/expose this annotation in its display of the release image.
    """

    # This will let us make asyncio event loops reentrant; it is useful whenever we need to call asynchronous code
    # from a synchronous function, and there's a chance that another even loop is already running.
    # This has to be done once, so do it right after entering the CLI command
    nest_asyncio.apply()

    # Initialize group config: we need this to determine the canonical builders behavior
    runtime.initialize(config_only=True)

    if runtime.group_config.canonical_builders_from_upstream and runtime.build_system == 'brew':
        runtime.initialize(mode="both", clone_distgits=True, clone_source=False, prevent_cloning=False)
    else:
        runtime.initialize(mode="both", clone_distgits=False, clone_source=False, prevent_cloning=True)

    await GenPayloadCli(
        runtime,
        is_name or assembly_imagestream_base_name(runtime),
        is_namespace or default_imagestream_namespace_base_name(),
        organization,
        repository,
        private_repository,
        release_repository,
        output_dir,
        exclude_arch,
        skip_gc_tagging,
        emergency_ignore_issues,
        apply,
        apply_multi_arch,
        moist_run,
        embargo_permit_ack,
    ).run()


def default_imagestream_base_name(version: str, runtime: Runtime) -> str:
    return default_imagestream_base_name_generic(version, runtime.build_system)


def default_imagestream_base_name_generic(version: str, build_system) -> str:
    if version in KONFLUX_IMAGESTREAM_OVERRIDE_VERSIONS or build_system == 'brew':
        return f"{version}-art-latest"
    else:  # konflux
        return f"{version}-konflux-art-latest"


def assembly_imagestream_base_name(runtime: Runtime) -> str:
    version = runtime.get_minor_version()
    return assembly_imagestream_base_name_generic(
        version, runtime.assembly, runtime.assembly_type, runtime.build_system
    )


def assembly_imagestream_base_name_generic(version, assembly_name, assembly_type, build_system):
    if assembly_name == 'stream' and assembly_type is AssemblyTypes.STREAM:
        return default_imagestream_base_name_generic(version, build_system)
    elif version in KONFLUX_IMAGESTREAM_OVERRIDE_VERSIONS or build_system == 'brew':
        return f"{version}-art-assembly-{assembly_name}"
    else:  # konflux
        return f"{version}-konflux-art-assembly-{assembly_name}"


def default_imagestream_namespace_base_name() -> str:
    return "ocp"


def payload_imagestream_namespace_and_name(
    base_namespace: str, base_imagestream_name: str, brew_arch: str, private: bool
) -> Tuple[str, str]:
    """
    :return: Returns the imagestream name and namespace to which images
             for the specified CPU arch and privacy mode should be synced.
    """

    arch_suffix = go_suffix_for_arch(brew_arch)
    priv_suffix = "-priv" if private else ""
    namespace = f"{base_namespace}{arch_suffix}{priv_suffix}"
    name = f"{base_imagestream_name}{arch_suffix}{priv_suffix}"
    return namespace, name


async def modify_and_replace_api_object(
    api_obj: oc.APIObject, modifier_func: Callable[[oc.APIObject], Any], backup_file_path: Path, dry_run: bool
):
    """
    Receives an APIObject, archives the current state of that object, runs a modifying method on it,
    archives the new state of the object, and then tries to replace the object on the
    cluster API server.
    :param api_obj: The openshift client APIObject to work with.
    :param modifier_func: A function that will accept the api_obj as its first parameter and make
                          any desired change to that object.
    :param backup_file_path: A Path object that can be used to archive pre & post modification
                             states of the object before triggering the update.
    :param dry_run: Write archive files but do not actually update the imagestream.
    """

    filepath = backup_file_path.joinpath(
        f"replacing-{api_obj.kind()}.{api_obj.namespace()}.{api_obj.name()}.before-modify.json"
    )
    async with aiofiles.open(filepath, mode='w+') as backup_file:
        await backup_file.write(api_obj.as_json(indent=4))

    modifier_func(api_obj)
    api_obj_model = api_obj.model

    # Before replacing api objects on the server, make sure to remove aspects that can
    # confuse subsequent CLI interactions with the object.
    if api_obj_model.metadata.annotations["kubectl.kubernetes.io/last-applied-configuration"]:
        api_obj_model.metadata.annotations.pop("kubectl.kubernetes.io/last-applied-configuration")

    # If server-side metadata is being passed in, remove it before we try to replace the object.
    if api_obj_model.metadata:
        for md in ["creationTimestamp", "generation", "uid"]:
            api_obj_model.metadata.pop(md)

    api_obj_model.pop("status")

    filepath = backup_file_path.joinpath(
        f"replacing-{api_obj.kind()}.{api_obj.namespace()}.{api_obj.name()}.after-modify.json"
    )
    async with aiofiles.open(filepath, mode="w+") as backup_file:
        await backup_file.write(api_obj.as_json(indent=4))

    if not dry_run:
        api_obj.replace()


class PayloadEntry(NamedTuple):
    # Append any issues for the assembly
    issues: List[AssemblyIssue]

    # The final quay.io destination for the single-arch pullspec
    dest_pullspec: str

    # The final quay.io destination for the manifest list the single arch image
    # might belong to. Most images built in brew will have been part of a
    # manifest list, but not all release components (e.g. RHCOS)
    # will be. We reuse manifest lists where possible for heterogeneous
    # release payloads to save time vs building them ourselves.
    dest_manifest_list_pullspec: str = None

    # If the entry is for an image in this doozer group, these values will be set.
    # The image metadata which associated with the payload
    image_meta: Optional[ImageMetadata] = None
    # An inspector associated with the overall brew build (manifest list) found for the release
    build_record_inspector: Optional[BuildRecordInspector] = None
    # The brew build archive (arch specific image) that should be tagged into the payload
    image_inspector: Optional[ImageInspector] = None

    # If the entry is for RHCOS, this value will be set
    rhcos_build: Optional[RHCOSBuildInspector] = None


def exchange_pullspec_tag_for_shasum(tag_pullspec: str, digest: str) -> str:
    """
    Create pullspec with the repo from the (tag-specifying) pullspec plus the given digest.
    """

    # extract repo e.g. quay.io/openshift-release-dev/ocp-v4.0-art-dev:sha256-b056..84b-ml
    #                -> quay.io/openshift-release-dev/ocp-v4.0-art-dev
    output_pullspec: str = tag_pullspec.rsplit(":", 1)[0]
    # return a sha-based pullspec
    return output_pullspec + "@" + digest


class GenPayloadCli:
    """
    An object to encapsulate the CLI inputs, methods, and state for the release:gen-payload command.
    """

    def __init__(
        self,
        # leave these all optional to make testing easier
        runtime: Runtime = None,
        is_name: str = None,
        is_namespace: str = None,
        organization: str = None,
        repository: str = None,
        private_repository: str = None,
        release_repository: str = None,
        output_dir: str = None,
        exclude_arch: Tuple[str] = None,
        skip_gc_tagging: bool = False,
        emergency_ignore_issues: bool = False,
        apply: bool = False,
        apply_multi_arch: bool = False,
        moist_run: bool = False,
        embargo_permit_ack: bool = False,
    ):
        self.runtime = runtime
        self.package_rpm_finder = PackageRpmFinder(runtime)
        self.payload_generator = PayloadGenerator(runtime, self.package_rpm_finder)
        self.logger = runtime.logger if runtime else MagicMock()  # in tests, blackhole logs by default
        # release-controller IS to update (modified per arch, privacy)
        self.base_imagestream = (is_namespace, is_name)
        # where in the registry to publish/reference component images and release images
        self.component_repo = (organization, repository)
        self.private_component_repo = (organization, private_repository)
        self.release_repo = (organization, release_repository)

        if output_dir:  # where to output yaml report and backed up IS
            self.output_path = Path(output_dir).absolute()
            self.output_path.mkdir(parents=True, exist_ok=True)

        self.exclude_arch = exclude_arch
        self.skip_gc_tagging = skip_gc_tagging  # do not re-tag custom builds in brew
        self.emergency_ignore_issues = emergency_ignore_issues  # permit any inconsistency
        self.apply = apply  # actually update the IS
        self.apply_multi_arch = apply_multi_arch  # update the "multi" IS as well
        self.moist_run = moist_run  # mirror the images but do not update the IS

        # store generated payload entries: {arch -> dict of payload entries}
        self.payload_entries_for_arch: Dict[str, Dict[str, PayloadEntry]] = {}
        self.private_payload_entries_for_arch: Dict[str, Dict[str, PayloadEntry]] = {}
        # for gathering issues that are found while evaluating the payload:
        self.assembly_issues: List[AssemblyIssue] = list()
        # private releases (only nightlies) can reference private component builds
        self.privacy_modes: List[bool] = [False]
        # do we proceed with this payload after weighing issues against permits?
        self.payload_permitted = False
        # Allows embargoed builds to be released
        self.embargo_permit_ack = embargo_permit_ack

    @start_as_current_span_async(TRACER, "releases:gen-payload")
    async def run(self):
        """
        Main entry point once instantiated with CLI inputs.
        """
        rt = self.runtime
        span = trace.get_current_span()
        span.set_attributes(
            {
                "doozer.group": rt.group,
                "doozer.assembly": str(rt.assembly) if rt.assembly else "",
                "doozer.assembly_type": str(rt.assembly_type),
                "doozer.brew_event": int(rt.brew_event) if rt.brew_event else 0,
                "doozer.arches": sorted(rt.arches),
            }
        )

        self.validate_parameters()
        self.logger.info(f"Collecting latest information associated with the assembly: {rt.assembly}")
        with TRACER.start_as_current_span("Calls AssemblyInspector.__init__"):
            assembly_inspector = AssemblyInspector(rt, rt.build_retrying_koji_client())
            await assembly_inspector.initialize(lookup_mode='both')

        self.payload_entries_for_arch, self.private_payload_entries_for_arch = await self.generate_payload_entries(
            assembly_inspector
        )
        assembly_report: Dict = await self.generate_assembly_report(assembly_inspector)

        self.logger.info('\n%s', yaml.dump(assembly_report, default_flow_style=False, indent=2))
        with self.output_path.joinpath("assembly-report.yaml").open(mode="w") as report_file:
            yaml.dump(assembly_report, stream=report_file, default_flow_style=False, indent=2)
        span.set_attribute("doozer.result.report", assembly_report)

        self.assess_assembly_viability()
        await self.sync_payloads()  # even when not permitted, produce what we _would have_ synced

        if self.payload_permitted or self.emergency_ignore_issues:
            exit(0)

        red_print(
            "DO NOT PROCEED WITH THIS ASSEMBLY PAYLOAD -- not all detected issues are permitted.", file=sys.stderr
        )
        exit(1)

    def validate_parameters(self):
        """
        Sanity check the assembly requested and adjust state accordingly.
        """

        rt = self.runtime
        if rt.assembly not in {None, "stream", "test"} and rt.assembly not in rt.releases_config.releases:
            raise DoozerFatalError(f"Assembly '{rt.assembly}' is not explicitly defined.")

        if rt.assembly and rt.assembly != "stream" and "art-latest" in self.base_imagestream[1]:
            raise ValueError('"art-latest" imagestreams should only be used for the "stream" assembly')

        if rt.assembly_type is AssemblyTypes.STREAM:
            # Only nightlies have the concept of private and public payloads
            self.privacy_modes = [False, True]

        if rt.assembly_type is not AssemblyTypes.STREAM and self.emergency_ignore_issues:
            raise DoozerFatalError(
                "Assemblies of type other than 'stream' need to have their permits listed "
                "in the assembly definitions. `EMERGENCY_IGNORE_ISSUES` is not a valid option"
            )

        # check that we can produce a full multi nightly if requested
        if self.apply_multi_arch and (rt.images or rt.exclude or self.exclude_arch):
            raise DoozerFatalError(
                "Cannot create a multi nightly without including the full set of images. "
                "Either include all images/arches or omit --apply-multi-arch"
            )

    async def generate_assembly_report(self, assembly_inspector: AssemblyInspector) -> Dict:
        """
        Generate a status report of the search for inconsistencies across all payloads generated.
        """
        rt = self.runtime
        report = dict(
            non_release_images=[image_meta.distgit_key for image_meta in rt.get_non_release_image_metas()],
            release_images=[image_meta.distgit_key for image_meta in rt.get_for_release_image_metas()],
            missing_image_builds=[
                dgk for (dgk, ii) in assembly_inspector.get_group_release_images().items() if ii is None
            ],  # A list of metas where the assembly did not find a build
        )
        report["viable"], report["assembly_issues"] = await self.generate_assembly_issues_report(assembly_inspector)
        self.payload_permitted = report["viable"]

        return report

    @start_as_current_span_async(TRACER, "GenPayloadCli.generate_assembly_issues_report")
    async def generate_assembly_issues_report(self, assembly_inspector: AssemblyInspector) -> (bool, Dict[str, Dict]):
        """
        Populate self.assembly_issues and payload entries with inconsistencies found.
        """
        span = trace.get_current_span()
        rt = self.runtime
        self.logger.info("Checking assembly content for inconsistencies.")
        span.add_event("Checking assembly content for inconsistencies")
        self.detect_mismatched_siblings(assembly_inspector)

        if self.runtime.build_system == 'brew':
            with rt.shared_build_status_detector() as bsd:
                # Use the list of builds associated with the group/assembly to warm up BSD caches
                assembly_build_ids: Set[int] = self.collect_assembly_build_ids(assembly_inspector)
                assembly_brew_build_ids = list(filter(lambda build_id: str(build_id).isdigit(), assembly_build_ids))
                bsd.populate_archive_lists(assembly_brew_build_ids)
                bsd.find_shipped_builds(assembly_brew_build_ids)

        # check that RPMs belonging to this assembly/group are consistent with the assembly definition.
        for rpm_meta in rt.rpm_metas():
            self.assembly_issues.extend(assembly_inspector.check_group_rpm_package_consistency(rpm_meta))

        # check that RPMs belonging to this assembly/group are the latest
        if rt.assembly_type is AssemblyTypes.STREAM:
            await self.detect_non_latest_rpms(assembly_inspector)

        # check that images for this assembly/group are consistent with the assembly definition.
        self.detect_inconsistent_images(assembly_inspector)

        # check that images for this assembly/group have installed rpms that are not allowed to assemble.
        self.detect_installed_rpms_issues(assembly_inspector)

        # update issues found for payload images and check RPM consistency
        await self.detect_extend_payload_entry_issues(assembly_inspector)

        # If the assembly claims to have reference nightlies, assert that our payload matches them exactly.
        self.assembly_issues.extend(await self.payload_generator.check_nightlies_consistency(assembly_inspector))

        span.add_event("Summarizing assembly issue permits")
        return self.summarize_issue_permits(assembly_inspector)

    @TRACER.start_as_current_span("GenPayloadCli.detect_mismatched_siblings")
    def detect_mismatched_siblings(self, assembly_inspector: AssemblyInspector):
        """
        Mismatched siblings are built from the same repo but at a different commit
        """
        span = trace.get_current_span()
        self.logger.debug("Checking for mismatched sibling sources...")
        group_images: List = list(assembly_inspector.get_group_release_images().values())
        issues = []
        for mismatched, sibling in self.payload_generator.find_mismatched_siblings(group_images):
            issue = AssemblyIssue(
                f"{mismatched.get_nvr()} was built from a different upstream "
                f"source commit ({mismatched.get_source_git_commit()[:7]}) "
                f"than one of its siblings {sibling.get_nvr()} "
                f"from {sibling.get_source_git_commit()[:7]}",
                component=mismatched.get_image_meta().distgit_key,
                code=AssemblyIssueCode.MISMATCHED_SIBLINGS,
            )
            issues.append(issue)
        self.assembly_issues.extend(issues)
        span.set_attribute("doozer.result.issues", list(map(lambda it: it.to_dict(), issues)))

    @staticmethod
    def generate_id_tags_list(assembly_inspector: AssemblyInspector) -> List[Tuple[int, str]]:
        """
        Construct a list of builds and desired tags (we use "hotfix" tags for this)
        """

        return [  # (build_id, desired non-GC tag)
            (bbii.get_build_id(), bbii.get_image_meta().hotfix_brew_tag())
            for bbii in assembly_inspector.get_group_release_images().values()
            if bbii
        ]

    @TRACER.start_as_current_span("GenPayloadCli.collect_assembly_build_ids")
    def collect_assembly_build_ids(self, assembly_inspector: AssemblyInspector) -> Set[int]:
        """
        Collect a list of brew builds (images and RPMs) included in the assembly.

        To prevent garbage collection for custom assemblies (which don't have errata tool releases
        that normally prevent), we must tag these builds explicitly. We want to ensure these builds
        persist so that later we can build custom releases based on previous custom releases. If we
        lose images and builds for custom releases in brew due to GC, we will not be able to
        construct derivative release payloads.

        We do not, however, want to preserve every build destined for a nightly (which is why we do
        not tag for "stream" assemblies).
        """

        self.logger.debug("Finding all builds in the assembly...")
        id_tags: List[Tuple[int, str]] = self.generate_id_tags_list(assembly_inspector)

        # For each RHEL version targeted by any of our RPMs, find RPM
        # builds with respect to the group/assembly. (Even single RPMs
        # can build for multiple versions of RHEL.)
        rhel_version_seen: Set[int] = set()  # set of rhel versions we have processed
        for rpm_meta in self.runtime.rpm_metas():
            for el_ver in rpm_meta.determine_rhel_targets():
                if el_ver not in rhel_version_seen:
                    # not processed yet, query the assembly for this rhel version now.
                    rhel_version_seen.add(el_ver)
                    hotfix_tag = self.runtime.get_default_hotfix_brew_tag(el_target=el_ver)
                    id_tags.extend(
                        [
                            (rpm_build_dict["id"], hotfix_tag)
                            for rpm_build_dict in assembly_inspector.get_group_rpm_build_dicts(el_ver=el_ver).values()
                            if rpm_build_dict
                        ]
                    )

        # Tag builds for custom assemblies unless we have been told not to from the command line.
        if self.runtime.assembly_type != AssemblyTypes.STREAM and not self.skip_gc_tagging:
            self.tag_missing_gc_tags(id_tags)

        # we should now in good conscience be able to put these in a payload
        return set(build_id for build_id, _ in id_tags)

    def tag_missing_gc_tags(self, id_tags: Tuple[int, str]):
        """
        Ensure that each build is tagged in brew with its GC-preventing tag
        """

        self.logger.debug("ensuring assembly contents are tagged to avoid brew GC...")
        # first, find the tags that each build currently has.
        # construct a list of builds and brew listTags tasks (which run as a multicall at loop/context exit)
        id_tag_tasks: List[Tuple[int, str, KojiWrapperMetaReturn]] = list()
        # [(build_id, non-GC tag, multicall task to list tags)]
        with self.runtime.pooled_koji_client_session() as pooled_kcs:
            with pooled_kcs.multicall(strict=True) as m:
                for build_id, desired_tag in id_tags:
                    id_tag_tasks.append((build_id, desired_tag, m.listTags(build=build_id)))

        # Tasks should now contain tag list information for all builds associated with this assembly.
        # Now see if the hotfix tag we want is already present, and if not, add it.
        # Note: shared_koji_client_session authenticates by default (needed for tagging)
        with self.runtime.shared_koji_client_session() as koji_client:
            with koji_client.multicall() as m:
                for build_id, desired_tag, list_tag_task in id_tag_tasks:
                    current_tags = [tag_entry["name"] for tag_entry in list_tag_task.result]
                    if desired_tag not in current_tags:
                        # The hotfix tag is missing, so apply it.
                        self.logger.info(
                            'Adding tag %s to build: %s to prevent garbage collection.', desired_tag, build_id
                        )
                        m.tagBuild(desired_tag, build_id)

    @start_as_current_span_async(TRACER, "GenPayloadCli.detect_non_latest_rpms")
    async def detect_non_latest_rpms(self, assembly_inspector: AssemblyInspector):
        """
        If this is a stream assembly, images which are not using the latest rpm builds should not reach
        the release controller. Other assemblies may be deliberately constructed from non-latest.
        """

        self.logger.debug("detecting images with group RPMs installed that are not the latest builds...")
        for dgk, build_inspector in assembly_inspector.get_group_release_images().items():
            if build_inspector:
                image_meta = build_inspector.get_image_meta()
                if self.runtime.build_system == 'brew':
                    non_latest_rpms = await build_inspector.find_non_latest_rpms()
                else:  # konflux
                    non_latest_rpms = await build_inspector.find_non_latest_rpms(self.package_rpm_finder)

                for arch, non_latest_rpms in non_latest_rpms.items():
                    # This could indicate an issue with scan-sources or that an image is no longer successfully building
                    # It could also mean that images are pinning content, which may be expected, so allow permits.
                    for installed_nevra, newest_nevra, repo in non_latest_rpms:
                        nevr, _ = installed_nevra.rsplit(".", maxsplit=1)
                        rpm_name = parse_nvr(nevr)['name']
                        is_exempt, pattern = image_meta.is_rpm_exempt(rpm_name)
                        if is_exempt:
                            self.logger.warning(
                                "%s is exempt from rpm change detection by '%s'", installed_nevra, pattern
                            )
                            continue
                        self.assembly_issues.append(
                            AssemblyIssue(
                                f"Found outdated RPM ({installed_nevra}) installed in {build_inspector.get_nvr()} ({arch})"
                                f" when {newest_nevra} was available in repo {repo}",
                                component=dgk,
                                code=AssemblyIssueCode.OUTDATED_RPMS_IN_STREAM_BUILD,
                            )
                        )

    @TRACER.start_as_current_span("GenPayloadCli.detect_inconsistent_images")
    def detect_inconsistent_images(self, assembly_inspector: AssemblyInspector):
        """
        Create issues for image builds selected by this assembly/group
        that are inconsistent with the assembly definition.
        """

        self.logger.debug("detecting images inconsistent with the assembly definition ...")
        for _, bbii in assembly_inspector.get_group_release_images().items():
            if bbii:
                self.assembly_issues.extend(
                    assembly_inspector.check_group_image_consistency(bbii, self.package_rpm_finder)
                )

    @TRACER.start_as_current_span("GenPayloadCli.detect_installed_rpms_issues")
    def detect_installed_rpms_issues(self, assembly_inspector: AssemblyInspector):
        """
        Create issues for image builds with installed rpms
        """
        self.logger.debug("Detecting issues with installed rpms...")
        for dg_key, bbii in assembly_inspector.get_group_release_images().items():
            if bbii:
                self.assembly_issues.extend(
                    assembly_inspector.check_installed_rpms_in_image(dg_key, bbii, self.package_rpm_finder)
                )

    def full_component_repo(self, repo_type: RepositoryType = RepositoryType.PUBLIC) -> str:
        """
        Full pullspec for the component repo
        """
        org, repo = self.private_component_repo if repo_type == RepositoryType.PRIVATE else self.component_repo

        return f"quay.io/{org}/{repo}"

    @TRACER.start_as_current_span("GenPayloadCli.generate_payload_entries")
    async def generate_payload_entries(
        self, assembly_inspector: AssemblyInspector
    ) -> (Dict[str, Dict[str, PayloadEntry]], Dict[str, Dict[str, PayloadEntry]]):
        """
        Generate single-arch PayloadEntries for the assembly payload.
        Payload generation may uncover assembly issues, which are added to the assembly_issues list.
        Returns a dict of dicts, keyed by architecture, then by payload component name.
        """

        public_entries_for_arch: Dict[str, Dict[str, PayloadEntry]] = dict()  # arch => img tag => PayloadEntry
        private_entries_for_arch: Dict[str, Dict[str, PayloadEntry]] = dict()  # arch => img tag => PayloadEntry

        arches = (
            self.runtime.group_config.konflux.arches if self.runtime.build_system == 'konflux' else self.runtime.arches
        )

        for arch in arches:
            if arch in self.exclude_arch:
                self.logger.info(f"Excluding payload files architecture: {arch}")
                continue
            # No adjustment for private or public; the assembly's canonical payload content is the same.

            entries: Dict[str, PayloadEntry]  # Key of this dict is release payload tag name
            payload_issues: List[AssemblyIssue]
            public_repo = self.full_component_repo(repo_type=RepositoryType.PUBLIC)
            entries, payload_issues = self.payload_generator.find_payload_entries(assembly_inspector, arch, public_repo)

            public_entries: Dict[str, PayloadEntry] = dict()
            for k, v in entries.items():
                if not v.build_record_inspector:
                    # Its RHCOS, since it doesn't have a build inspector. Put it in for now, but change once RHCOS
                    # supports private nightlies
                    public_entries[k] = v
                    continue

                if v.build_record_inspector.is_under_embargo() and self.runtime.assembly_type == AssemblyTypes.STREAM:
                    if self.runtime.build_system == 'brew':
                        public_build = v.image_meta.get_latest_brew_build(
                            default=None, el_target=v.image_meta.branch_el_target(), extra_pattern='*.p0.*'
                        )
                    else:
                        public_build = await v.image_meta.get_latest_konflux_build(
                            default=None, el_target=v.image_meta.branch_el_target(), embargoed=False
                        )

                    if not public_build:
                        raise IOError(f'Unable to find last public build for {v.image_meta.distgit_key}')

                    public_bbi = BuildRecordInspector.get_build_record_inspector(
                        runtime=self.runtime, build_obj=public_build
                    )
                    public_image_inspector = public_bbi.get_image_inspector(arch)

                    dest_pullspec = self.payload_generator.get_mirroring_destination(
                        public_image_inspector.get_digest(), public_repo
                    )
                    dest_manifest_list_pullspec = self.payload_generator.get_mirroring_destination(
                        public_image_inspector.get_manifest_list_digest(), public_repo
                    )
                    public_entry = PayloadEntry(
                        image_meta=v.image_meta,
                        build_record_inspector=public_bbi,
                        image_inspector=public_image_inspector,
                        dest_pullspec=dest_pullspec,
                        dest_manifest_list_pullspec=dest_manifest_list_pullspec,
                        issues=list(),
                    )

                    self.logger.info(
                        f'Replacing embargoed image {v.build_record_inspector.get_nvr()} with public image {public_bbi.get_nvr()} for public imagestream'
                    )
                    public_entries[k] = public_entry
                    # It's an embargoed build. Filter it out if its stream
                    continue

                public_entries[k] = v

            public_entries_for_arch[arch] = public_entries
            self.assembly_issues.extend(payload_issues)

            # Report issues for any embargoed content being made public.
            # If releasing after embargo lift, these can be permitted using 'EMBARGOED_CONTENT' code
            embargo_issues = self.payload_generator.embargo_issues_for_payload(public_entries, arch)
            self.assembly_issues.extend(embargo_issues)

            private_entries, private_payload_issues = self.payload_generator.find_payload_entries(
                assembly_inspector, arch, self.full_component_repo(repo_type=RepositoryType.PRIVATE)
            )
            private_entries_for_arch[arch] = private_entries

            # Check to see if there are private only issues, and add them to the list of assembly issues
            private_only_issues = set(private_payload_issues) - set(payload_issues)

            self.assembly_issues.extend(list(private_only_issues))

        return public_entries_for_arch, private_entries_for_arch

    @start_as_current_span_async(TRACER, "GenPayloadCli.detect_extend_payload_entry_issues")
    async def detect_extend_payload_entry_issues(self, assembly_inspector: AssemblyInspector):
        """
        Associate assembly issues with related payload entries if any.
        Also look for additional issues related to RHCOS container(s) across
        the single-arch payloads.
        """

        primary_container_name = rhcos.get_primary_container_name(self.runtime)
        cross_payload_requirements = self.runtime.group_config.rhcos.require_consistency
        if not cross_payload_requirements:
            self.runtime.logger.debug("No cross-payload consistency requirements defined in group.yml")
        # Structure to record rhcos builds we use so that they can be analyzed for inconsistencies
        targeted_rhcos_builds: Dict[bool, List[RHCOSBuildInspector]] = {
            False: [],
            True: [],
        }  # privacy mode: list of BuildInspector
        for arch, entries in self.payload_entries_for_arch.items():
            for tag, payload_entry in entries.items():
                if payload_entry.image_meta:
                    # Record the issues previously found for this image in corresponding payload_entry
                    payload_entry.issues.extend(
                        ai for ai in self.assembly_issues if ai.component == payload_entry.image_meta.distgit_key
                    )
                elif payload_entry.rhcos_build:
                    if tag != primary_container_name:
                        continue  # RHCOS is one build, only analyze once (for primary container)

                    await self.detect_rhcos_issues(payload_entry, assembly_inspector)
                    # Record the build to enable later consistency checks between all RHCOS builds.
                    # There are presently no private RHCOS builds, so add only to private_mode=False.
                    targeted_rhcos_builds[False].append(payload_entry.rhcos_build)

                    if cross_payload_requirements:
                        self.assembly_issues.extend(
                            self.payload_generator.find_rhcos_payload_rpm_inconsistencies(
                                payload_entry.rhcos_build,
                                assembly_inspector.get_group_release_images(),
                                cross_payload_requirements,
                                self.package_rpm_finder,
                            ),
                        )
                else:
                    raise DoozerFatalError(f"Unsupported PayloadEntry: {payload_entry}")

        self.detect_rhcos_kernel_inconsistencies(targeted_rhcos_builds)
        self.detect_rhcos_inconsistent_rpms(targeted_rhcos_builds)  # across all arches

    async def detect_rhcos_issues(self, payload_entry: PayloadEntry, assembly_inspector: AssemblyInspector):
        """
        Associate relevant assembly issues with an RHCOS PayloadEntry.
        Use assembly inspector to detect assembly issues like RPMs installed in
        the PayloadEntry that are not what the assembly specifies.
        """

        # record issues found in assembly list and per payload entry
        rhcos_build = cast(RHCOSBuildInspector, payload_entry.rhcos_build)
        self.assembly_issues.extend(await assembly_inspector.check_rhcos_issues(rhcos_build))
        payload_entry.issues.extend(ai for ai in self.assembly_issues if ai.component == "rhcos")

    def detect_rhcos_inconsistent_rpms(self, targeted_rhcos_builds: Dict[bool, List[RHCOSBuildInspector]]):
        """
        Generate assembly issue(s) if RHCOS builds do not contain consistent RPMs across arches.
        """

        for privacy_mode in self.privacy_modes:  # only for relevant modes
            rhcos_builds = targeted_rhcos_builds[privacy_mode]
            rhcos_inconsistencies: Dict[str, List[str]] = self.payload_generator.find_rhcos_build_rpm_inconsistencies(
                rhcos_builds
            )
            if rhcos_inconsistencies:
                self.assembly_issues.append(
                    AssemblyIssue(
                        f"Found RHCOS inconsistencies in builds {rhcos_builds} "
                        f"(private={privacy_mode}): {rhcos_inconsistencies}",
                        component="rhcos",
                        code=AssemblyIssueCode.INCONSISTENT_RHCOS_RPMS,
                    )
                )

    def detect_rhcos_kernel_inconsistencies(self, targeted_rhcos_builds: Dict[bool, List[RHCOSBuildInspector]]):
        for privacy_mode in self.privacy_modes:  # only for relevant modes
            rhcos_builds = targeted_rhcos_builds[privacy_mode]
            for rhcos_build in rhcos_builds:
                inconsistencies = self.payload_generator.find_rhcos_build_kernel_inconsistencies(rhcos_build)
                if inconsistencies:
                    self.assembly_issues.append(
                        AssemblyIssue(
                            f"Found kernel inconsistencies in RHCOS build {rhcos_build} "
                            f"(private={privacy_mode}): {inconsistencies}",
                            component="rhcos",
                            code=AssemblyIssueCode.FAILED_CROSS_RPM_VERSIONS_REQUIREMENT,
                        )
                    )

    def summarize_issue_permits(self, assembly_inspector: AssemblyInspector) -> (bool, Dict[str, Dict]):
        """
        Check whether the issues found are permitted,
        and collect issues per component into a serializable report
        """
        permitted_non_acknowledged_embargoed_issues = []
        payload_permitted = True
        assembly_issues_report: Dict[str, List[Dict]] = dict()
        for ai in self.assembly_issues:
            permitted = assembly_inspector.does_permit(ai)

            if (
                self.runtime.assembly_type is not AssemblyTypes.STREAM
                and ai.code == AssemblyIssueCode.EMBARGOED_CONTENT
                and permitted
            ):
                # If this is not a STREAM run, check to see if there are any permitted embargoed content
                # If there is, --embargo-permit-ack must be specified in build-sync params, as the second and final gate
                # before letting embargoed builds from being synced to public quay
                if not self.embargo_permit_ack:
                    permitted_non_acknowledged_embargoed_issues.append(ai.component)

            payload_permitted &= permitted  # If anything not permitted, payload not permitted
            assembly_issues_report.setdefault(ai.component, []).append(
                dict(
                    code=ai.code.name,
                    msg=ai.msg,
                    permitted=permitted,
                )
            )

        if permitted_non_acknowledged_embargoed_issues:
            raise IOError(
                f"Embargoed components found: {set(permitted_non_acknowledged_embargoed_issues)}; "
                "must specify --embargo-permit-ack flag to publish private content"
            )

        return payload_permitted, assembly_issues_report

    @TRACER.start_as_current_span("GenPayloadCli.assess_assembly_viability")
    def assess_assembly_viability(self):
        """
        Adjust assembly viability if needed per emergency_ignore_issues.
        """

        if not self.payload_permitted:
            if self.emergency_ignore_issues:
                self.logger.warning("Permitting issues only because --emergency-ignore-issues was specified")
                self.payload_permitted = True
            else:
                self.logger.error("Assembly is not permitted. Will not apply changes to imagestreams.")
                self.apply = False
                self.apply_multi_arch = False

        span = trace.get_current_span()
        span.set_attributes(
            {
                "doozer.param.emergency_ignore_issues": self.emergency_ignore_issues,
                "doozer.result.viability": self.payload_permitted,
                "doozer.result.apply": self.apply,
                "doozer.result.apply_multi_arch": self.apply_multi_arch,
            }
        )

    @start_as_current_span_async(TRACER, "GenPayloadCli.sync_payloads")
    async def sync_payloads(self):
        """
        Use the payload entries that have been generated across the arches to mirror the images
        out to quay and create the imagestream definitions that will update the release-controller.

        Also re-organize the entries into a hierarchy suitable for creating the heterogeneous
        "multi" payloads, and create those.
        """

        # When we are building a heterogeneous / multiarch payload, we need to
        # keep track of images that are going into each single-arch
        # imagestream. Maps [is_private] -> [tag_name] -> [arch] -> PayloadEntry
        multi_specs: Dict[bool, Dict[str, Dict[str, PayloadEntry]]] = {
            True: dict(),
            False: dict(),
        }

        # Ensure that all payload images have been mirrored before updating
        # the imagestream. Otherwise, the imagestream will fail to import the
        # image.
        tasks = []
        for arch, payload_entries in self.private_payload_entries_for_arch.items():
            tasks.append(self.mirror_payload_content(arch, payload_entries, True))
        for arch, payload_entries in self.payload_entries_for_arch.items():
            tasks.append(self.mirror_payload_content(arch, payload_entries))
        await asyncio.gather(*tasks)

        await asyncio.sleep(120)

        # Updating public and private image streams
        for private_mode, payload_entries_for_each_arch_iter in [
            (False, self.payload_entries_for_arch),
            (True, self.private_payload_entries_for_arch),
        ]:
            tasks = []
            for arch, payload_entries in payload_entries_for_each_arch_iter.items():
                self.logger.info(f"Building payload files for architecture: {arch}; private: {private_mode}")
                tasks.append(
                    self.generate_specific_payload_imagestreams(arch, private_mode, payload_entries, multi_specs)
                )
            await asyncio.gather(*tasks)

        if self.apply_multi_arch:
            if self.runtime.group_config.multi_arch.enabled:
                await self.sync_heterogeneous_payloads(multi_specs)
            else:
                self.logger.info(
                    "--apply-multi-arch is enabled but the group config / assembly does "
                    "not have group.multi_arch.enabled==true"
                )

    async def mirror_payload_content(self, arch: str, payload_entries: Dict[str, PayloadEntry], private: bool = False):
        """
        Ensure an arch's payload entries are synced out for the public to access.
        """

        # Prevents writing the same destination twice (not supported by oc if in the same mirroring file):
        mirror_src_for_dest: Dict[str, str] = dict()

        # Login to the konflux registry
        if self.runtime.build_system == 'konflux':
            cmd = [
                'oc',
                'registry',
                'login',
                '--registry',
                'quay.io/redhat-user-workloads',
                f'--registry-config={os.getenv("KONFLUX_ART_IMAGES_AUTH_FILE")}',
            ]
            await exectools.cmd_assert_async(cmd)

        for payload_entry in payload_entries.values():
            if not payload_entry.image_inspector:
                continue  # Nothing to mirror (e.g. RHCOS)
            mirror_src_for_dest[payload_entry.dest_pullspec] = payload_entry.image_inspector.get_pullspec()
            if payload_entry.dest_manifest_list_pullspec:
                # For heterogeneous release payloads, if a component builds for all arches
                # (without using -alt images), we can use the manifest list for the images directly from OSBS.
                # This saves a significant amount of time compared to building the manifest list again.
                mirror_src_for_dest[payload_entry.dest_manifest_list_pullspec] = (
                    payload_entry.build_record_inspector.get_build_pullspec()
                )

        @exectools.limit_concurrency(500)
        @retry(reraise=True, stop=stop_after_attempt(10), wait=wait_fixed(60))
        async def _mirror(file_path: Path, dest_src_pullspecs: List[Tuple[str, str]]):
            # Save the default SRC=DEST input to a file for syncing by 'oc image mirror'
            async with aiofiles.open(file_path, mode="w+", encoding="utf-8") as out_file:
                for dest_pullspec, src_pullspec in dest_src_pullspecs:
                    await out_file.write(f"{src_pullspec}={dest_pullspec}\n")

            if self.apply or self.apply_multi_arch:
                self.logger.info(f"Mirroring images from {str(src_dest_path)}")
                cmd = [
                    'oc',
                    'image',
                    'mirror',
                    '--keep-manifest-list',
                    '--continue-on-error',
                    f'--filename={str(src_dest_path)}',
                ]
                if self.runtime.build_system == 'konflux':
                    cmd.append(f'--registry-config={os.getenv("KONFLUX_ART_IMAGES_AUTH_FILE")}')
                await asyncio.wait_for(exectools.cmd_assert_async(cmd), timeout=7200)

        # Mirror the images in chunks to avoid erroring out due to possible registry issues
        image_chunk_size = 50
        i = 0
        for pullspec_pair_chunk in chunk(list(mirror_src_for_dest.items()), image_chunk_size):
            src_dest_path = self.output_path.joinpath(f"src_dest.{arch}-{'private' if private else 'public'}-{i}.txt")
            await _mirror(src_dest_path, pullspec_pair_chunk)
            i += 1

    async def generate_specific_payload_imagestreams(
        self,
        arch: str,
        private_mode: bool,
        payload_entries: Dict[str, PayloadEntry],
        # Map [is_private] -> [tag_name] -> [arch] -> PayloadEntry
        multi_specs: Dict[bool, Dict[str, Dict[str, PayloadEntry]]],
    ):
        """
        For the specific arch, -priv, and payload entries, generate the imagestreams.
        Populate multi_specs with the single-arch images that need composing into the multi-arch imagestream.
        """

        # Generate arch/privacy-specific imagestream tags for the payload
        istags: List[Dict] = []
        # Typically for a stream update, we want to prune unused imagestream
        # tags. But setting this flag indicates we don't expect to have all the
        # tags for a payload, so tags will only be updated, and none removed.
        incomplete_payload_update: bool = False

        if self.runtime.images or self.runtime.exclude:
            # If images are being explicitly included or excluded, assume we will not be
            # performing a full replacement of the imagestream content.
            incomplete_payload_update = True

        for payload_tag_name, payload_entry in payload_entries.items():
            multi_specs[private_mode].setdefault(payload_tag_name, dict())

            if (
                private_mode is False
                and payload_entry.build_record_inspector
                and payload_entry.build_record_inspector.is_under_embargo()
                and self.runtime.assembly_type == AssemblyTypes.STREAM
            ):
                # No embargoed images for assembly stream
                # should go to the public release controller, so we will not have
                # a complete payload.
                incomplete_payload_update = True
                continue

            istags.append(self.payload_generator.build_payload_istag(payload_tag_name, payload_entry))
            multi_specs[private_mode][payload_tag_name][arch] = payload_entry

        imagestream_namespace, imagestream_name = payload_imagestream_namespace_and_name(
            *self.base_imagestream, arch, private_mode
        )

        await self.write_imagestream_artifact_file(
            imagestream_namespace, imagestream_name, istags, incomplete_payload_update
        )
        if self.apply:
            await self.apply_arch_imagestream(
                imagestream_namespace, imagestream_name, istags, incomplete_payload_update
            )

    async def write_imagestream_artifact_file(
        self, imagestream_namespace: str, imagestream_name: str, istags: List[Dict], incomplete_payload_update
    ):
        """
        Write out an artifact showing the entries we expect to add/update in the target
        integration imagestream.
        """

        filename = (
            f"updated-tags-for.{imagestream_namespace}.{imagestream_name}"
            f"{'-partial' if incomplete_payload_update else ''}.yaml"
        )
        async with aiofiles.open(self.output_path.joinpath(filename), mode="w+", encoding="utf-8") as out_file:
            istream_spec = self.payload_generator.build_payload_imagestream(
                imagestream_name,
                imagestream_namespace,
                istags,
                self.assembly_issues,
            )
            await out_file.write(yaml.safe_dump(istream_spec, indent=2, default_flow_style=False))

    async def apply_arch_imagestream(
        self, imagestream_namespace: str, imagestream_name: str, istags: List[Dict], incomplete_payload_update: bool
    ):
        """
        Orchestrate the update and tag removal for one arch imagestream in the OCP cluster.
        """

        with oc.project(imagestream_namespace):
            istream_apiobj = self.ensure_imagestream_apiobj(imagestream_name)

            if self.is_imagestream_locked(istream_apiobj):
                self.logger.warning(f'The {imagestream_name} imagestream is currently locked by TRT. Skipping updates.')
                return

            pruning_tags, adding_tags = await self.apply_imagestream_update(
                istream_apiobj, istags, incomplete_payload_update
            )

            if pruning_tags:
                self.logger.warning(
                    'The following tag names are no longer part of the release and will be pruned in %s:%s: %s',
                    imagestream_namespace,
                    imagestream_name,
                    pruning_tags,
                )

                if not self.moist_run:
                    for old_tag in pruning_tags:
                        # Even though we have replaced the .spec on the imagestream, the old tag will still
                        # be reflected in .status. The release controller considers this a legit declaration,
                        # so we must remove it explicitly using `oc delete istag`
                        try:
                            oc.selector(f"istag/{imagestream_name}:{old_tag}").delete()
                        except Exception:
                            # This is not a fatal error, but failure to delete may leave issues being
                            # displayed on the release controller page.
                            self.logger.error(
                                'Unable to delete %s tag fully from %s imagestream in %s:\n%s',
                                old_tag,
                                imagestream_name,
                                imagestream_namespace,
                                traceback.format_exc(),
                            )

            if adding_tags:
                self.logger.warning(
                    'The following tag names are net new to %s:%s: %s',
                    imagestream_namespace,
                    imagestream_name,
                    adding_tags,
                )

    @staticmethod
    def ensure_imagestream_apiobj(imagestream_name) -> oc.APIObject:
        """
        Create the imagestream if it does not exist, and return the api object.
        """

        istream_apiobj = oc.selector(f"imagestream/{imagestream_name}").object(ignore_not_found=True)
        if istream_apiobj:
            return istream_apiobj

        # The imagestream has not been bootstrapped; create it.
        oc.create(
            {
                "apiVersion": "image.openshift.io/v1",
                "kind": "ImageStream",
                "metadata": {
                    "name": imagestream_name,
                },
            }
        )
        return oc.selector(f"imagestream/{imagestream_name}").object()

    @staticmethod
    def is_imagestream_locked(imagestream_obj: oc.APIObject) -> bool:
        """
        Via the CRT "release-tool", TRT can "lock" nightly imagestream, which requests
        that ART stop making updates to the stream for period of time, until the lock
        is removed.
        https://github.com/openshift/release-controller/blob/master/hack/release-tool.py
        """
        imagestream_mode = imagestream_obj.get_annotation('release.openshift.io/mode')
        return imagestream_mode == 'locked'

    async def apply_imagestream_update(
        self, istream_apiobj, istags: List[Dict], incomplete_payload_update: bool
    ) -> Tuple[Set[str], Set[str]]:
        """
        Apply changes for one integration imagestream object on the app.ci cluster.
        """

        # gather diffs between old and new, indicating removal or addition
        pruning_tags: Set[str] = set()
        adding_tags: Set[str] = set()

        # create a closure to be applied below against the api object
        def update_single_arch_istags(apiobj: oc.APIObject):
            nonlocal pruning_tags
            nonlocal adding_tags

            new_annotations = dict()
            if apiobj.model.metadata.annotations is not oc.Missing:
                # We must preserve annotations as they contain release controller configuration information
                new_annotations = apiobj.model.metadata.annotations._primitive()

                # Remove old inconsistency information if it exists
                new_annotations.pop("release.openshift.io/inconsistency", None)

            new_annotations.update(self.payload_generator.build_imagestream_annotations(self.assembly_issues))

            apiobj.model.metadata["annotations"] = new_annotations

            incoming_tag_lookup = {istag["name"]: istag for istag in istags}
            incoming_tag_names = set(incoming_tag_lookup.keys())
            existing_tag_names = set([istag["name"] for istag in apiobj.model.spec.tags])
            adding_tags = incoming_tag_names - existing_tag_names

            for existing_istag in apiobj.model.spec.tags:
                # When TRT reverts an image using release-tool (https://github.com/openshift/release-controller/blob/master/hack/release-tool.py)
                # it means they are setting a tag pointing to image X to use an older image Y.
                # Since there is something wrong with X, they don't want to unstick
                # from Y until a new image Z supersedes X.
                # To accomplish this, the 'revert' verb adds an annotation to the
                # istag being reverted with the value "reverted-from: X".
                # Thus, we should not update the annotated tag UNTIL our
                # target image is something other than X.
                # Note that existing_istag.annotations may be None (vs model.Missing) if
                # "annotations: null" is specified in the istag, so take care before
                # assuming it is dict-like.
                if existing_istag.annotations and 'reverted-from' in existing_istag.annotations:
                    revereted_tag_name = existing_istag.name
                    reverted_from_image = existing_istag.annotations['reverted-from']
                    reverted_to_image = existing_istag['from'].name
                    if revereted_tag_name not in incoming_tag_lookup:
                        if not incomplete_payload_update:
                            self.logger.warning(
                                f'The tag {revereted_tag_name} was reverted by TRT from {reverted_from_image} to {reverted_to_image} HOWEVER, the reverted tag is not in the incoming tags (which suggests it should be pruned). That is an unlikely series of events.'
                            )
                    else:
                        target_image = incoming_tag_lookup[revereted_tag_name]['from'].name
                        self.logger.warning(
                            f'The tag {revereted_tag_name} was reverted by TRT from {reverted_from_image} to {reverted_to_image}. Incoming update wants to set target image {target_image}.'
                        )
                        if target_image == reverted_from_image:
                            self.logger.warning(
                                f'The target image matches the reverted image; ART will persist the image TRT reverted to: {reverted_to_image}'
                            )
                            incoming_tag_lookup[revereted_tag_name]['from'].name = reverted_to_image
                            # We must also preserve the annotation to persist the revert for the next update
                            incoming_tag_lookup[revereted_tag_name]['annotations'] = (
                                existing_istag.annotations.primitive()
                            )
                        else:
                            self.logger.warning(
                                f'The target image DOES NOT match the reverted image; ART will remove the revert and update to {target_image}'
                            )

            new_istags = list(istags)  # copy, don't update/embed list parameter
            if incomplete_payload_update:
                # If our incoming `istags` don't necessarily include everything in the release,
                # we need to preserve existing tag values that were not updated.
                for istag in apiobj.model.spec.tags:
                    if istag.name not in incoming_tag_names:
                        new_istags.append(istag)
            else:
                # Else, we believe the assembled tags are canonical. Declare
                # old tags to prune.
                pruning_tags = existing_tag_names - incoming_tag_names

            apiobj.model.spec.tags = new_istags

        await modify_and_replace_api_object(istream_apiobj, update_single_arch_istags, self.output_path, self.moist_run)
        return pruning_tags, adding_tags

    async def sync_heterogeneous_payloads(self, multi_specs: Dict[bool, Dict[str, Dict[str, PayloadEntry]]]):
        """
        We now generate the artifacts to create heterogeneous release payloads (suitable for
        clusters with multiple arches present). A heterogeneous or 'multi' release payload is a
        manifest list (i.e. it consists of N release payload manifests, one for each arch).

        The release payload images referenced in the multi-release payload manifest list are
        themselves somewhat standard release payloads (i.e. they are based on CVO images for their
        arch) BUT, each component image they reference is a manifest list. For example, the `cli`
        image in the s390x release payload will point to a manifest list composed of cli image
        manifests for each architecture.

        So the top-level release payload pullspec is a manifest list, referencing release payload
        images for each arch, and these arch specific payload images reference manifest list based
        components pullspecs.

        The outcome is that this method creates/updates:
        1. A component imagestream containing a tag per image component, similar to what we do for
           single-arch assemblies, but written to a file rather than applied to the cluster.
        2. For each arch, a payload image based on its CVO, built with references to the (multi-arch)
           components in this imagestream and stored in
           * quay.io/openshift-release-dev/ocp-release-nightly for nightlies or
           * quay.io/openshift-release-dev/ocp-release for standard/custom releases.
        3. A multi payload image, which is a manifest list built from the arch-specific release
           payload images and stored in the same quay.io repos as above.
        4. A _release_ imagestream per assembly to record tag(s) for multi-arch payload image(s)
           created in step 3, e.g. `4.11-art-latest-multi` for `stream` assembly or
           `4.11-art-assembly-4.11.4-multi` for other assemblies.
        """

        for private_mode in self.privacy_modes:
            if private_mode:
                # The CI image registry does not support manifest lists. Thus, we need to publish
                # our nightly release payloads to quay.io. As of this writing, we don't have a
                # private quay repository into which we could push embargoed release heterogeneous
                # release payloads.
                red_print(
                    "PRIVATE MODE MULTI PAYLOADS ARE CURRENTLY DISABLED. "
                    "WE NEED A PRIVATE QUAY REPO FOR PRIVATE MULTI RELEASE PAYLOADS"
                )
                continue

            imagestream_namespace, imagestream_name = payload_imagestream_namespace_and_name(
                *self.base_imagestream, "multi", private_mode
            )

            multi_release_istag: str  # The tag to record the multi release payload image
            multi_release_manifest_list_tag: str  # The quay.io tag to preserve the multi payload
            multi_release_istag, multi_release_manifest_list_tag = self.get_multi_release_names(private_mode)

            tasks = []
            for tag_name, arch_to_payload_entry in multi_specs[private_mode].items():
                tasks.append(self.build_multi_istag(tag_name, arch_to_payload_entry, imagestream_namespace))
            multi_istags: List[Dict] = await asyncio.gather(*tasks)

            # now multi_istags contains istags which all point to component manifest lists. We must
            # run oc adm release new on this set of tags -- once for each arch - to create the arch
            # specific release payloads.
            multi_release_is = self.payload_generator.build_payload_imagestream(
                imagestream_name,
                imagestream_namespace,
                multi_istags,
                assembly_wide_inconsistencies=self.assembly_issues,
            )

            # We will then stitch those arch specific payload images together into a release payload
            # manifest list.
            multi_release_dest: str = f"quay.io/{'/'.join(self.release_repo)}:{multi_release_manifest_list_tag}"
            final_multi_pullspec: str = await self.create_multi_release_image(
                imagestream_name, multi_release_is, multi_release_dest, multi_release_istag, multi_specs, private_mode
            )
            self.logger.info(f"The final pull_spec for the multi release payload is: {final_multi_pullspec}")

            with oc.project(imagestream_namespace):
                await self.apply_multi_imagestream_update(final_multi_pullspec, imagestream_name, multi_release_istag)

    def get_multi_release_names(self, private_mode: bool) -> Tuple[str, str]:
        """
        Determine a unique name for the multi release (recorded in the imagestream for the assembly) and a
        quay.io tag to prevent garbage collection of the image -- as of 2022-09-09, both tags are
        the same.
        """

        multi_ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d-%H%M%S")
        if self.runtime.assembly_type is AssemblyTypes.STREAM:
            # We are publicizing a nightly. Unlike single-arch payloads, the release controller does
            # not react to 4.x-art-latest updates and create a timestamp-based name. We create the
            # nightly name in doozer.
            multi_release_istag = (
                f"{self.runtime.get_minor_version()}.0-0.nightly{go_suffix_for_arch('multi', private_mode)}-{multi_ts}"
            )
            # Tag the release image with same name as release displayed in the release controller
            multi_release_manifest_list_tag = multi_release_istag
        else:
            # Tag the release image in quay with anything unique; we just don't want it garbage
            # collected. It will not show up in the release controller. The only purpose of this
            # image is to provide inputs to the promotion job, which looks at the imagestream
            # and not for this tag.
            multi_release_manifest_list_tag = (
                f"{self.runtime.get_minor_version()}.0-0.art-assembly-"
                f"{self.runtime.assembly}{go_suffix_for_arch('multi', private_mode)}-"
                f"{multi_ts}"
            )
            # This will be the singular tag we create in an imagestream on app.ci. The actual name
            # does not matter, because it will not be visible in the release controller and will not
            # be the ultimate name used to promote the release. It must be unique, however, because
            # Cincinnati chokes if multiple images exist in the repo with the same release name.
            multi_release_istag = multi_release_manifest_list_tag
        return multi_release_istag, multi_release_manifest_list_tag

    async def build_multi_istag(
        self, tag_name: str, arch_to_payload_entry: Dict[str, PayloadEntry], imagestream_namespace: str
    ) -> Dict:
        """
        Build a single imagestream tag for a component in a multi-arch payload.
        """

        # There are two flows:
        # 1. The images for ALL arches were part of the same brew built manifest list. In this case,
        #    we want to reuse the manifest list (it was already mirrored during the mirroring step).
        # 2. At least one arch for this component does not have the same manifest list as the
        #    other images. This will always be true for RHCOS, but also applies
        #    to -alt images. In this case, we must stitch a manifest list together ourselves.
        entries: List[PayloadEntry] = list(arch_to_payload_entry.values())
        # Determine which flow applies by checking whether all entries have the same manifest list
        manifest_list_dests: Set[str] = set(entry.dest_manifest_list_pullspec for entry in entries)
        if len(manifest_list_dests) == 1 and set(manifest_list_dests).pop():  # only one, and not None
            # Flow 1: Just reuse the manifest list built in brew and synced to a tag in quay.
            output_digest_pullspec = exchange_pullspec_tag_for_shasum(
                manifest_list_dests.pop(), entries[0].image_inspector.get_manifest_list_digest()
            )
            self.logger.info(f"Reusing brew manifest-list {output_digest_pullspec} for component {tag_name}")
        else:
            # Flow 2: Build a new manifest list and push it to quay.
            output_digest_pullspec = await self.create_multi_manifest_list(
                tag_name, arch_to_payload_entry, imagestream_namespace
            )

        issues = list(
            issue  # collect issues from each payload entry.
            for payload_entry in entries
            for issue in payload_entry.issues or []
        )
        return self.payload_generator.build_payload_istag(
            tag_name,
            PayloadEntry(
                dest_pullspec=output_digest_pullspec,
                issues=issues,
            ),
        )

    async def create_multi_manifest_list(
        self, tag_name: str, arch_to_payload_entry: Dict[str, PayloadEntry], imagestream_namespace: str
    ) -> str:
        """
        Create and publish a manifest list for a component in a multi-arch payload.
        """

        # podman on rhel7.9 (like buildvm) does not support manifest lists. Instead we use a tool
        # named manifest-list which is available through epel for rhel7 and can be installed
        # directly on fedora. The format for input is https://github.com/estesp/manifest-tool
        # Let's create some yaml input files.
        component_manifest_path: Path = self.output_path.joinpath(
            f"{imagestream_namespace}.{tag_name}.manifest-list.yaml"
        )
        self.logger.info(f"Stitching {component_manifest_path} manifest-list spec for component {tag_name}")
        manifests: List[Dict] = []
        # Ensure we create a new tag for each manifest list. Unlike images, if we push a manifest
        # list that seems to contain the same content (i.e. references the exact same manifests), it
        # will still have a different digest. This means pushing a seemingly identical manifest list
        # to the same tag will cause the original to lose the tag and be garbage collected.
        manifest_list_hash = hashlib.sha256(self.runtime.uuid.encode("utf-8"))
        for arch, payload_entry in arch_to_payload_entry.items():
            manifests.append(
                {
                    "image": payload_entry.dest_pullspec,
                    "platform": {
                        "os": "linux",
                        "architecture": go_arch_for_brew_arch(arch),
                    },
                }
            )
            manifest_list_hash.update(payload_entry.dest_pullspec.encode("utf-8"))

        # We need a unique tag for the manifest list image so that it does not get garbage collected.
        # To calculate a tag that will vary depending on the individual manifests being added,
        # we've calculated a sha256 of all the manifests being added.
        output_pullspec: str = f"{self.full_component_repo()}:sha256-{manifest_list_hash.hexdigest()}"

        # write the manifest list to a file and push it to the registry.
        async with aiofiles.open(component_manifest_path, mode="w+") as ml:
            await ml.write(yaml.safe_dump(dict(image=output_pullspec, manifests=manifests), default_flow_style=False))
        await manifest_tool(f'push from-spec {str(component_manifest_path)}')

        # we are pushing a new manifest list, so return its sha256 based pullspec
        sha = await find_manifest_list_sha(output_pullspec)
        return exchange_pullspec_tag_for_shasum(output_pullspec, sha)

    async def create_multi_release_image(
        self,
        imagestream_name: str,
        multi_release_is: Dict,
        multi_release_dest: str,
        multi_release_name: str,
        multi_specs: Dict[bool, Dict[str, Dict[str, PayloadEntry]]],
        private_mode: bool,
    ) -> str:
        """
        Create and publish a "multi" release image for each arch. These all have the same content,
        just the release image itself is arch-specific (based on arch CVO). Then stitch them
        together into a manifest list.
        Returns: a pullspec for the manifest list of release images.
        """

        # Write the imagestream to a file ("oc adm release new" can read from a file instead of
        # openshift cluster API)
        multi_release_is_path: Path = self.output_path.joinpath(f"{imagestream_name}-release-imagestream.yaml")
        async with aiofiles.open(multi_release_is_path, mode="w+") as mf:
            await mf.write(yaml.safe_dump(multi_release_is))

        @retry(reraise=True, stop=stop_after_attempt(10), wait=wait_fixed(60))
        async def _run(to_image, to_image_base):
            return await exectools.cmd_assert_async(
                [
                    "oc",
                    "adm",
                    "release",
                    "new",
                    f"--name={multi_release_name}",
                    "--reference-mode=source",
                    "--keep-manifest-list",
                    f"--from-image-stream-file={str(multi_release_is_path)}",
                    f"--to-image-base={to_image_base}",
                    f"--to-image={to_image}",
                    "--metadata",
                    json.dumps({"release.openshift.io/architecture": "multi"}),
                ]
            )

        # This will map arch names to a release payload pullspec we create for that arch
        # (i.e. based on the arch's CVO image)
        arch_release_dests: Dict[str, str] = dict()
        for arch, cvo_entry in multi_specs[private_mode]["cluster-version-operator"].items():
            arch_release_dests[arch] = f"{multi_release_dest}-{arch}"
            # Create the arch specific release payload containing tags pointing to manifest list
            # component images.
            # Note: Do not use asyncio.gather here because it can result in a large number of
            # concurrent requests to the registry, which can cause unexpected EOFs/fails
            await _run(to_image=arch_release_dests[arch], to_image_base=cvo_entry.dest_pullspec)

        return await self.create_multi_release_manifest_list(arch_release_dests, imagestream_name, multi_release_dest)

    async def create_multi_release_manifest_list(
        self, arch_release_dests: Dict[str, str], imagestream_name: str, multi_release_dest: str
    ) -> str:
        """
        Create a manifest list spec containing references to the arch specific release payloads
        created. Return sha-based pullspec.
        """

        ml_dict = {
            "image": f"{multi_release_dest}",
            "manifests": [
                {
                    "image": arch_release_payload,
                    "platform": {
                        "os": "linux",
                        "architecture": go_arch_for_brew_arch(arch),
                    },
                }
                for arch, arch_release_payload in arch_release_dests.items()
            ],
        }

        release_payload_ml_path = self.output_path.joinpath(f"{imagestream_name}.manifest-list.yaml")
        async with aiofiles.open(release_payload_ml_path, mode="w+") as ml:
            await ml.write(yaml.safe_dump(ml_dict, default_flow_style=False))

        await manifest_tool(f'push from-spec {str(release_payload_ml_path)}')

        # if we are actually pushing a manifest list, then we should derive a sha256 based pullspec
        sha = await find_manifest_list_sha(multi_release_dest)
        return exchange_pullspec_tag_for_shasum(multi_release_dest, sha)

    async def apply_multi_imagestream_update(
        self, final_multi_pullspec: str, imagestream_name: str, multi_release_istag: str
    ):
        """
        If running with assembly==stream, updates release imagestream with a new imagestream tag for the nightly. Older
        nightlies are pruned from the release imagestream.
        When running for non-stream, creates 4.x-art-assembly-$name with the pullspec to contain the newly created
        pullspec.
        """

        multi_art_latest_is = self.ensure_imagestream_apiobj(imagestream_name)

        if self.is_imagestream_locked(multi_art_latest_is):
            self.logger.warning(f'The {imagestream_name} imagestream is currently locked by TRT. Skipping updates.')
            return

        # For nightlies, these will be set as annotations on the release imagestream tag.
        # For non-nightlies, the new 4.x-art-assembly-$name the annotations will also be
        # applied at the top level annotations for the imagestream.
        pipeline_metadata_annotations = {
            'release.openshift.io/build-url': os.getenv('BUILD_URL', ''),
            'release.openshift.io/runtime-brew-event': str(self.runtime.brew_event),
        }

        def add_multi_nightly_release(obj: oc.APIObject):
            obj_model = obj.model
            if obj_model.spec.tags is oc.Missing:
                obj_model.spec["tags"] = oc.ListModel([])

            if self.runtime.assembly_type is AssemblyTypes.STREAM:
                # For normal 4.x-art-latest, we update the imagestream with individual component images
                # and the release controller formulates the nightly. For multi-arch, this is not
                # possible (notably, the CI internal registry does not support manifest lists). Instead,
                # in the ocp-multi namespace, the 4.x-art-latest imagestreams are configured
                # `as: Stable`: https://github.com/openshift/release/pull/24130
                # This means the release controller treats entries in these imagestreams the same way it
                # treats it when ART tags into is/release; i.e. it treats it as an official release.
                # With this comes the responsibility to prune nightlies ourselves.
                # The goal is to keep the list short, but to ensure that at least two accepted nightlies
                # remain in the list, if there are at least two to begin with. Having at least one
                # accepted nightly in the list is necessary to trigger upgrade tests from an old accepted
                # nightly to a new one.
                def is_accepted(tag):
                    # Returns true if the imagestream tag has been accepted.
                    return tag.get('annotations', dict()).get('release.openshift.io/phase', 'Unknown') == 'Accepted'

                release_tags: List = obj_model.spec["tags"]._primitive()
                new_release_tags = release_tags[-5:]  # Preserve the most recent five

                latest_accepted = list(filter(is_accepted, new_release_tags))
                if len(latest_accepted) < 2:
                    remaining_tags = release_tags[:-5]
                    remaining_accepted = list(filter(is_accepted, remaining_tags))
                    new_release_tags = (
                        remaining_accepted[-2:] + new_release_tags
                    )  # Keep the newest accepted of the payloads we were going to otherwise prune

                if release_tags:
                    last_nightly_tagname = release_tags[-1].get('name', None)
                    if last_nightly_tagname:
                        date_str = last_nightly_tagname.split('-multi-')[
                            -1
                        ]  # 4.18.0-0.nightly-multi-2024-10-10-163835 => "2024-10-10-163835"
                        last_nightly_time = datetime.strptime(date_str, "%Y-%m-%d-%H%M%S")
                        current_time = datetime.utcnow()

                        major = self.runtime.group_config.vars['MAJOR']
                        minor = self.runtime.group_config.vars['MINOR']

                        master_major, master_minor = extract_version_fields(what_is_in_master(), at_least=2)
                        if major != master_major or minor != master_minor:
                            next_nightly_delay = timedelta(hours=6)
                        else:
                            # More frequent nightlies for master
                            next_nightly_delay = timedelta(hours=6)

                        if current_time < last_nightly_time + next_nightly_delay:
                            self.logger.info(
                                f'The last nightly {last_nightly_tagname} is less than {next_nightly_delay}h old; skipping release controller update'
                            )
                            return

                obj_model.spec["tags"] = new_release_tags

                # When spec tags are removed, their entry under the imagestream.status field should also
                # be removed by the imagestream controller. The release controller will delete the
                # mirror imagestream that was created for the payload.

            else:
                # For non-stream 4.x-art-assembly-$name, old imagestreamtags should be removed.
                obj_model.spec["tags"].clear()
                obj_model.metadata.annotations = pipeline_metadata_annotations

            # Now append a tag for our new nightly.
            obj_model.spec["tags"].append(
                {
                    "from": {
                        "kind": "DockerImage",
                        "name": final_multi_pullspec,
                    },
                    "referencePolicy": {
                        "type": "Source",
                    },
                    "importPolicy": {
                        "importMode": "PreserveOriginal",
                    },
                    "name": multi_release_istag,
                    "annotations": dict(
                        **{
                            # Prevents the release controller from trying to create a local registry release payload
                            # with oc adm release new.
                            "release.openshift.io/rewrite": "false",
                        },
                        **pipeline_metadata_annotations,
                    ),
                }
            )
            return True

        await modify_and_replace_api_object(
            multi_art_latest_is, add_multi_nightly_release, self.output_path, self.moist_run
        )


class PayloadGenerator:
    def __init__(self, runtime: Runtime = None, package_rpm_finder=None):
        self.runtime = runtime
        self.package_rpm_finder = package_rpm_finder

    @staticmethod
    def find_mismatched_siblings(
        build_record_inspectors: Iterable[Optional[BuildRecordInspector]],
    ) -> List[Tuple[BuildRecordInspector, BuildRecordInspector]]:
        """
        Sibling images are those built from the same repository. We need to throw an error
        if there are sibling built from different commits.
        :return: Returns a list of (BuildRecordInspector,BuildRecordInspector)
                 where the first item is a mismatched sibling of the second
        """

        class RepoBuildRecord(NamedTuple):
            build_record_inspector: BuildRecordInspector
            source_git_commit: str

        # Maps SOURCE_GIT_URL -> RepoBuildRecord(SOURCE_GIT_COMMIT, DISTGIT_KEY, NVR).
        # Where the Tuple is the first build encountered claiming it is sourced from the SOURCE_GIT_URL
        repo_builds: Dict[str, RepoBuildRecord] = dict()

        mismatched_siblings: List[Tuple[BuildRecordInspector, BuildRecordInspector]] = []
        for build_record_inspector in build_record_inspectors:
            if not build_record_inspector:
                # No build for this component at present.
                continue

            # Here we check the raw config - before it is affected by assembly overrides. Why?
            # If an artist overrides one sibling's git url, but not another, the following
            # scan would not be able to detect that they were siblings. Instead, we rely on the
            # original image metadata to determine sibling-ness.
            source_url = build_record_inspector.get_image_meta().raw_config.content.source.git.url

            source_git_commit = build_record_inspector.get_source_git_commit()
            if not source_url or not source_git_commit:
                # This is true for distgit only components.
                continue

            # Make sure URLs are comparable regardless of git: or https:
            source_url = convert_remote_git_to_https(source_url)

            potential_conflict: RepoBuildRecord = repo_builds.get(source_url, None)
            if potential_conflict:
                # Another component has build from this repo before. Make
                # sure it built from the same commit.
                if potential_conflict.source_git_commit != source_git_commit:
                    mismatched_siblings.append((build_record_inspector, potential_conflict.build_record_inspector))
                    red_print(
                        "The following NVRs are siblings but built from different commits: "
                        f"{potential_conflict.build_record_inspector.get_nvr()} and "
                        f"{build_record_inspector.get_nvr()}",
                        file=sys.stderr,
                    )
            else:
                # No conflict, so this is our first encounter for this repo; add it to our tracking dict.
                repo_builds[source_url] = RepoBuildRecord(
                    build_record_inspector=build_record_inspector, source_git_commit=source_git_commit
                )

        return mismatched_siblings

    @staticmethod
    def find_rhcos_build_rpm_inconsistencies(rhcos_builds: List[RHCOSBuildInspector]) -> Dict[str, List[str]]:
        """
        Looks through a set of RHCOS builds and finds if any of those builds contains a package version that
        is inconsistent with the same package in another RHCOS build.
        :return: Returns Dict[inconsistent_rpm_name] -> [inconsistent_nvrs, ...]. The Dictionary will be empty
                 if there are no inconsistencies detected.
        """

        rpm_uses: Dict[str, Set[str]] = {}

        for rhcos_build in rhcos_builds:
            for nvr in rhcos_build.get_rpm_nvrs():
                rpm_name = parse_nvr(nvr)["name"]
                if rpm_name not in rpm_uses:
                    rpm_uses[rpm_name] = dict()
                if nvr not in rpm_uses[rpm_name]:
                    rpm_uses[rpm_name][nvr] = []
                rpm_uses[rpm_name][nvr].append(rhcos_build.brew_arch)

        # Report back rpm name keys which were associated with more than one NVR in the set of RHCOS builds.
        return {rpm_name: nvr_dict for rpm_name, nvr_dict in rpm_uses.items() if len(nvr_dict) > 1}

    @staticmethod
    def find_rhcos_build_kernel_inconsistencies(rhcos_build: RHCOSBuildInspector) -> List[Dict[str, str]]:
        """
        Looks through a RHCOS build and finds if any of those builds contains a kernel-rt version that
        is inconsistent with kernel.

        e.g. kernel-4.18.0-372.43.1.el8_6 and kernel-rt-4.18.0-372.43.1.rt7.200.el8_6 are consistent,
        while kernel-4.18.0-372.43.1.el8_6 and kernel-rt-4.18.0-372.41.1.rt7.198.el8_6 are not.
        :return: Returns List[Dict[inconsistent_rpm_name, rpm_nvra]] The List will be empty
                 if there are no inconsistencies detected.
        """
        inconsistencies = []
        # rpm_list will be a list of rpms.
        # Each entry is another list in the format of [name, epoch, version, release, arch].
        rpm_list = rhcos_build.get_os_metadata_rpm_list()
        rpms_dict = {entry[0]: entry for entry in rpm_list}

        def _to_nvr(nevra):
            return f'{nevra[0]}-{nevra[2]}-{nevra[3]}'

        if {"kernel-core", "kernel-rt-core"} <= rpms_dict.keys():
            # ["kernel-core", 0, "4.18.0", "372.43.1.el8_6", "x86_64"] => ["4.18.0", "372.43.1.el8_6"]
            kernel_v, kernel_r = rpms_dict["kernel-core"][2:4]
            # ["kernel-rt-core", 0, "4.18.0", "4.18.0-372.41.1.rt7.198.el8_6", "x86_64"] => ["4.18.0", "4.18.0-372.41.1.rt7.198.el8_6"]
            kernel_rt_v, kernel_rt_r = rpms_dict["kernel-rt-core"][2:4]
            inconsistency = False
            if kernel_v != kernel_rt_v:
                inconsistency = True
            elif kernel_r != kernel_rt_r:
                # 372.43.1.el8_6 => 372.43.1
                kernel_r = kernel_r.rsplit('.', 1)[0]
                # 372.41.1.rt7.198.el8_6 => 372.41.1
                kernel_rt_r = kernel_rt_r.rsplit('.', 3)[0]
                if kernel_r != kernel_rt_r:
                    inconsistency = True
            if inconsistency:
                inconsistencies.append(
                    {
                        "kernel-core": _to_nvr(rpms_dict["kernel-core"]),
                        "kernel-rt-core": _to_nvr(rpms_dict["kernel-rt-core"]),
                    }
                )
        return inconsistencies

    def find_rhcos_payload_rpm_inconsistencies(
        self,
        primary_rhcos_build: RHCOSBuildInspector,
        payload_bri: Dict[str, BuildRecordInspector],
        # payload tag -> [pkg_name1, ...]
        payload_consistency_config: Dict[str, List[str]],
        package_rpm_finder=None,
    ) -> List[AssemblyIssue]:
        """
        Compares designated brew packages installed in designated payload members with the RPMs
        in an RHCOS build, ensuring that both have the same version installed.

        This is a little more tricky with RHCOS than other payload content because RHCOS metadata
        supplies only the installed RPMs, not the package NVRs that brew metadata supplies for
        containers, and the containers may install different subsets of the RPMs a package build
        provides. So we have to do a little more work to find all RPMs for the NVRs installed in the
        member and compare.

        :return: Returns a list of AssemblyIssue objects describing any inconsistencies found.
        """
        issues: List[AssemblyIssue] = []

        # index by name the RPMs installed in the RHCOS build
        rhcos_rpm_vrs: Dict[str, str] = {}  # name -> version-release
        for rpm in primary_rhcos_build.get_os_metadata_rpm_list():
            name, _, version, release, _ = rpm
            rhcos_rpm_vrs[name] = f"{version}-{release}"

        # check that each member consistency condition is met
        primary_rhcos_build.runtime.logger.debug(f"Running payload consistency checks against {primary_rhcos_build}")
        for payload_tag, consistent_pkgs in payload_consistency_config.items():
            bbii = payload_bri.get(payload_tag)
            if not bbii:
                issues.append(
                    AssemblyIssue(
                        f"RHCOS consistency configuration specifies a payload tag '{payload_tag}' that does not exist",
                        payload_tag,
                        AssemblyIssueCode.IMPERMISSIBLE,
                    )
                )
                continue

            # check that each specified package in the member is consistent with the RHCOS build
            for pkg in consistent_pkgs:
                issues.append(
                    self.validate_pkg_consistency_req(
                        payload_tag, pkg, bbii, rhcos_rpm_vrs, str(primary_rhcos_build), package_rpm_finder
                    )
                )

        return [issue for issue in issues if issue]

    def validate_pkg_consistency_req(
        self,
        payload_tag: str,
        pkg: str,
        bri: BuildRecordInspector,
        rhcos_rpm_vrs: Dict[str, str],
        rhcos_build_id: str,
        package_rpm_finder=None,
    ) -> Optional[AssemblyIssue]:
        """check that the specified package in the member is consistent with the RHCOS build"""
        logger = bri.runtime.logger
        payload_tag_nvr: str = bri.get_nvr()
        logger.debug(f"Checking consistency of {pkg} for {payload_tag_nvr} against {rhcos_build_id}")
        member_nvrs: Dict[str, Dict] = bri.get_all_installed_package_build_dicts(package_rpm_finder)  # by name
        try:
            build = member_nvrs[pkg]
        except KeyError:
            return AssemblyIssue(
                f"RHCOS consistency configuration specifies that payload tag '{payload_tag}' "
                f"should install package '{pkg}', but it does not",
                payload_tag,
                AssemblyIssueCode.FAILED_CONSISTENCY_REQUIREMENT,
            )

        # get names of all the actual RPMs included in this package build, because that's what we
        # have for comparison in the RHCOS metadata (not the package name).
        # For Konflux we're tracking package build NVRs as they come from the SBOM
        if self.runtime.build_system == 'brew':
            rpm_names = set(rpm["name"] for rpm in bri.get_rpms_in_pkg_build(build["build_id"]))
        else:
            rpm_names = set(rpm["name"] for rpm in bri.get_rpms_in_pkg_build(build["nvr"], self.package_rpm_finder))

        # find package RPM names in the RHCOS build and check that they have the same version
        required_vr = "-".join([build["version"], build["release"]])
        logger.debug(f"{payload_tag_nvr} {pkg} has RPMs {rpm_names} at version {required_vr}")
        for name in rpm_names:
            vr = rhcos_rpm_vrs.get(name)
            if vr:
                logger.debug(f"RHCOS RPM {name} version is {vr}")
            if vr and vr != required_vr:
                return AssemblyIssue(
                    f"RHCOS and '{payload_tag}' should use the same build of "
                    f"package '{pkg}', but {rhcos_build_id} has {name}-{vr} and "
                    f"{payload_tag_nvr} has {build['nvr']}",
                    payload_tag,
                    AssemblyIssueCode.FAILED_CONSISTENCY_REQUIREMENT,
                )
                # no need to check other RPMs from this package build, one is enough

        return None

    @staticmethod
    def get_mirroring_destination(sha256: str, dest_repo: str) -> str:
        """
        :param sha256: The digest for the image (e.g. "sha256:6084e70110ef....70676c8ca8ee5bd5e891e74")
        :param dest_repo: A pullspec to mirror to, except for the tag. This include registry, organization, and repo.
        :return: Returns the external (quay) image location to which this image should be mirrored in order
                 to be included in an nightly release payload. These tags are meant to leak no information
                 to users watching the quay repo. The image must have a tag or it will be garbage collected.
        """

        tag = sha256.replace(":", "-")  # sha256:abcdef -> sha256-abcdef
        return f"{dest_repo}:{tag}"

    def find_payload_entries(
        self, assembly_inspector: AssemblyInspector, arch: str, dest_repo: str
    ) -> (Dict[str, PayloadEntry], List[AssemblyIssue]):
        """
        Returns a list of images which should be included in the architecture specific release payload.
        This includes images for our group's image metadata as well as RHCOS.
        :param assembly_inspector: An analyzer for the assembly to generate entries for.
        :param arch: The brew architecture name to create the list for.
        :param dest_repo: The registry/org/repo into which the image should be mirrored.
        :return: Map[payload_tag_name] -> PayloadEntry.
        """

        members: Dict[str, PayloadEntry] = self._find_initial_payload_entries(assembly_inspector, arch, dest_repo)
        members = self._replace_missing_payload_entries(members, arch)
        rhcos_members, issues = self._find_rhcos_payload_entries(assembly_inspector, arch)
        members.update(rhcos_members)
        return members, issues

    def _find_initial_payload_entries(
        self, assembly_inspector: AssemblyInspector, arch: str, dest_repo: str
    ) -> Dict[str, PayloadEntry]:
        # Maps release payload tag name to the PayloadEntry for the image.
        members: Dict[str, Optional[PayloadEntry]] = dict()
        for payload_tag, image_inspector in self.get_group_payload_tag_mapping(assembly_inspector, arch).items():
            if not image_inspector:
                # There is no build for this payload tag for this CPU arch. This
                # will be filled in later in this method for the final list.
                members[payload_tag] = None
                continue

            members[payload_tag] = PayloadEntry(
                image_meta=image_inspector.get_image_meta(),
                build_record_inspector=image_inspector.get_build_inspector(),
                image_inspector=image_inspector,
                dest_pullspec=PayloadGenerator.get_mirroring_destination(image_inspector.get_digest(), dest_repo),
                dest_manifest_list_pullspec=PayloadGenerator.get_mirroring_destination(
                    image_inspector.get_manifest_list_digest(), dest_repo
                ),
                issues=list(),
            )
        return members

    @staticmethod
    def _replace_missing_payload_entries(members: Dict[str, PayloadEntry], arch: str) -> Dict[str, PayloadEntry]:
        """
        Members contains a complete map of payload tag keys, but some values may be None, indicating that the image
        does not build for this architecture. However, all architecture-specific release payloads must contain the
        full set of tags or 'oc adm release new' will fail; while a tag may not be logically necessary on e.g. s390x,
        we still need to populate that tag with something for metadata references to resolve.

        To do this, we replace missing images with the 'pod' image for the architecture. This should
        be available for every CPU architecture. As such, we must find 'pod' to proceed.
        """

        pod_entry = members.get("pod", None)
        if not pod_entry:
            raise IOError(f"Unable to find 'pod' image archive for architecture: {arch}; unable to construct payload")

        return {tag_name: entry or pod_entry for tag_name, entry in members.items()}

    @staticmethod
    @TRACER.start_as_current_span("PayloadGenerator._find_rhcos_payload_entries")
    def _find_rhcos_payload_entries(
        assembly_inspector: AssemblyInspector, arch: str
    ) -> Tuple[Dict[str, PayloadEntry], List[AssemblyIssue]]:
        span = trace.get_current_span()
        span.set_attributes(
            {
                "doozer.param.arch": arch,
            }
        )
        members: Dict[str, PayloadEntry] = dict()
        issues: List[AssemblyIssue] = list()
        rhcos_build: RHCOSBuildInspector = assembly_inspector.get_rhcos_build(arch)
        for container_config in rhcos_build.get_container_configs():
            try:
                members[container_config.name] = PayloadEntry(
                    dest_pullspec=rhcos_build.get_container_pullspec(container_config),
                    rhcos_build=rhcos_build,
                    issues=list(),
                )
            except RhcosMissingContainerException as ex:
                if container_config.primary:
                    # Impermissible, need to be sure of having the primary container in the payload
                    issues.append(
                        AssemblyIssue(
                            f"RHCOS build {rhcos_build} metadata lacks entry for primary container "
                            f"{container_config.name}: {ex}",
                            component=container_config.name,
                        )
                    )

                else:
                    issues.append(
                        AssemblyIssue(
                            f"RHCOS build {rhcos_build} metadata lacks entry for non-primary container "
                            f"{container_config.name}: {ex}",
                            component=container_config.name,
                            code=AssemblyIssueCode.MISSING_RHCOS_CONTAINER,
                        )
                    )

        return members, issues

    @staticmethod
    def build_payload_istag(payload_tag_name: str, payload_entry: PayloadEntry) -> Dict:
        """
        :param payload_tag_name: The name of the payload tag for which to create an istag.
        :param payload_entry: The payload entry to serialize into an imagestreamtag.
        :return: Returns an imagestreamtag dict for a release payload imagestream.
        """

        return {
            "annotations": PayloadGenerator.build_inconsistency_annotations(payload_entry.issues),
            "name": payload_tag_name,
            "from": {
                "kind": "DockerImage",
                "name": payload_entry.dest_pullspec,
            },
            "importPolicy": {
                "importMode": "PreserveOriginal",
            },
        }

    def build_payload_imagestream(
        self,
        imagestream_name: str,
        imagestream_namespace: str,
        payload_istags: Iterable[Dict],
        assembly_wide_inconsistencies: Iterable[AssemblyIssue],
    ) -> Dict:
        """
        Builds a definition for a release payload imagestream from a set of payload istags.
        :param runtime: The doozer Runtime.
        :param imagestream_name: The name of the imagstream to generate.
        :param imagestream_namespace: The nemspace in which the imagestream should be created.
        :param payload_istags: A list of istags generated by build_payload_istag.
        :param assembly_wide_inconsistencies: Any inconsistency information to embed in the imagestream.
        :return: Returns a definition for an imagestream for the release payload.
        """

        istream_obj = {
            "kind": "ImageStream",
            "apiVersion": "image.openshift.io/v1",
            "metadata": {
                "name": imagestream_name,
                "namespace": imagestream_namespace,
                "annotations": self.build_imagestream_annotations(assembly_wide_inconsistencies),
            },
            "spec": {
                "tags": list(payload_istags),
            },
        }

        return istream_obj

    def build_pipeline_metadata_annotations(self) -> Dict:
        """
        :return: If the metadata is available, include information like the Jenkins job URL
                 in a nightly annotation.
                 Returns a dict of pipeline metadata annotations.
        """

        pipeline_metadata = {
            'release.openshift.io/build-url': os.getenv('BUILD_URL', ''),
            'release.openshift.io/runtime-brew-event': str(self.runtime.brew_event),
        }

        return pipeline_metadata

    @staticmethod
    def build_inconsistency_annotations(inconsistencies: Iterable[AssemblyIssue]) -> Dict:
        """
        :param inconsistencies: A list of strings to report as inconsistencies within an annotation.
        :return: Returns a dict containing an inconsistency annotation out of the specified issues list.
                 Returns emtpy {} if there are no inconsistencies in the parameter.
        """

        # given a list of strings, build the annotation for inconsistencies
        if not inconsistencies:
            return {}

        # Some codes aren't critical to be marked as a blocking inconsistency for payloads
        exclude_assembly_issues = [AssemblyIssueCode.OUTDATED_RPMS_IN_STREAM_BUILD]

        msgs = sorted([i.msg for i in inconsistencies if i.code not in exclude_assembly_issues])

        if not msgs:
            return {}

        if len(msgs) > 5:
            # an exhaustive list of the problems may be too large; that goes in the state file.
            msgs[5:] = ["(...and more)"]
        return {"release.openshift.io/inconsistency": json.dumps(msgs)}

    def build_imagestream_annotations(self, inconsistencies: Iterable[AssemblyIssue]) -> Dict:
        annotations = {}
        annotations.update(self.build_inconsistency_annotations(inconsistencies))
        annotations.update(self.build_pipeline_metadata_annotations())
        return annotations

    def get_group_payload_tag_mapping(
        self, assembly_inspector: AssemblyInspector, arch: str
    ) -> Dict[str, Optional[ImageInspector]]:
        """
        Each payload tag name used to map exactly to one release imagemeta. With the advent of '-alt' images,
        we need some logic to determine which images map to which payload tags for a given architecture.
        :return: Returns a map[payload_tag_name] -> ArchiveImageInspector containing an image for the payload.
                 The value may be None if there is no arch specific build for the tag. This does not include RHCOS
                 since that is not a member of the group.
        """

        brew_arch = brew_arch_for_go_arch(arch)  # Make certain this is brew arch nomenclature
        members: Dict[str, Optional[ImageInspector]] = (
            dict()
        )  # Maps release payload tag name to the archive which should populate it
        for dgk, build_inspector in assembly_inspector.get_group_release_images().items():
            if build_inspector is None:
                # There was no build for this image found associated with the assembly.
                # In this case, don't put the tag_name into the imagestream. This is not good,
                # so be verbose.
                red_print(
                    f"Unable to find build for {dgk} for {assembly_inspector.get_assembly_name()}", file=sys.stderr
                )
                continue

            image_meta: ImageMetadata = assembly_inspector.runtime.image_map[dgk]

            if not image_meta.is_payload:
                # Nothing to do for images which are not in the payload
                continue

            # The tag that will be used in the imagestreams and whether it was explicitly declared.
            tag_name, explicit = image_meta.get_payload_tag_info()

            if arch not in image_meta.get_arches():
                # If this image is not meant for this architecture
                if tag_name not in members:
                    members[tag_name] = None  # We still need a placeholder in the tag mapping
                continue

            if members.get(tag_name, None) and not explicit:
                # If we have already found an entry, there is a precedence we honor for
                # "-alt" images. Specifically, if a imagemeta declares its payload tag
                # name explicitly, it will take precedence over any other entries
                # https://issues.redhat.com/browse/ART-2823
                # This was tag not explicitly declared, so ignore the duplicate image.
                continue

            try:
                image_inspector = build_inspector.get_image_inspector(brew_arch)
            except ChildProcessError:  # raised by Konflux in case oc image info fails
                image_inspector = None

            if not image_inspector:
                # There is no build for this CPU architecture for this image_meta/build. This finding
                # conflicts with the `arch not in image_meta.get_arches()` check above.
                # Best to fail.
                raise IOError(
                    f"{dgk} claims to be built for {image_meta.get_arches()} "
                    f"but did not find {brew_arch} build for {build_inspector.get_build_webpage_url()}"
                )

            members[tag_name] = image_inspector

        return members

    async def _check_nightly_consistency(
        self, assembly_inspector: AssemblyInspector, nightly: str, arch: str
    ) -> List[AssemblyIssue]:
        runtime = assembly_inspector.runtime

        def terminal_issue(msg: str) -> List[AssemblyIssue]:
            return [AssemblyIssue(msg, component="reference-releases")]

        issues: List[str]
        runtime.logger.info(f"Processing nightly: {nightly}")
        major_minor, brew_cpu_arch, priv = isolate_nightly_name_components(nightly)

        if major_minor != runtime.get_minor_version():
            return terminal_issue(f"Specified nightly {nightly} does not match group major.minor")

        # For 4.20, remove the -konflux suffix as Konflux builds are being mirrored to standard imagestreams
        if runtime.build_system == 'brew' or major_minor in KONFLUX_IMAGESTREAM_OVERRIDE_VERSIONS:
            release_suffix = 'release'
        else:
            release_suffix = 'konflux-release'

        rc_suffix = go_suffix_for_arch(brew_cpu_arch, priv)

        retries: int = 3
        release_json_str = ""
        rc = -1
        pullspec = f"registry.ci.openshift.org/ocp{rc_suffix}/{release_suffix}{rc_suffix}:{nightly}"
        while retries > 0:
            rc, release_json_str, err = await exectools.cmd_gather_async(
                f"oc adm release info {pullspec} -o=json", check=False
            )
            if rc == 0:
                break
            runtime.logger.warn(f"Error accessing nightly release info for {pullspec}:  {err}")
            retries -= 1

        if rc != 0:
            return terminal_issue(f"Unable to gather nightly release info details: {pullspec}; garbage collected?")

        release_info = Model(dict_to_model=json.loads(release_json_str))
        if not release_info.references.spec.tags:
            return terminal_issue(f"Could not find tags in nightly {nightly}")

        payload_entries: Dict[str, PayloadEntry]
        issues: List[AssemblyIssue]
        payload_entries, issues = self.find_payload_entries(assembly_inspector, arch, "")
        rhcos_container_configs = {tag.name: tag for tag in rhcos.get_container_configs(runtime)}
        for component_tag in release_info.references.spec.tags:  # For each tag in the imagestream
            payload_tag_name: str = component_tag.name  # e.g. "aws-ebs-csi-driver"
            payload_tag_pullspec: str = component_tag["from"].name  # quay pullspec
            if "@" not in payload_tag_pullspec:
                # This speaks to an invalid nightly, so raise and exception
                raise IOError(
                    f"Expected pullspec in {nightly}:{payload_tag_name} to be sha digest "
                    f"but found invalid: {payload_tag_pullspec}"
                )

            pullspec_sha = payload_tag_pullspec.rsplit("@", 1)[-1]
            entry = payload_entries.get(payload_tag_name, None)

            if not entry:
                raise IOError(f"Did not find {nightly} payload tag {payload_tag_name} in computed assembly payload")

            if entry.image_inspector:
                if entry.image_inspector.get_digest() != pullspec_sha:
                    # Impermissible because the artist should remove
                    # the reference nightlies from the assembly definition
                    issues.append(
                        AssemblyIssue(
                            f"{nightly} contains {payload_tag_name} sha {pullspec_sha} but assembly computed archive: "
                            f"{entry.image_inspector.get_pullspec()}",
                            component="reference-releases",
                        )
                    )

            elif entry.rhcos_build:
                actual_digest = entry.rhcos_build.get_container_digest(rhcos_container_configs[payload_tag_name])
                if actual_digest != pullspec_sha:
                    # Impermissible because the artist should remove the reference nightlies
                    # from the assembly definition
                    issues.append(
                        AssemblyIssue(
                            f'{nightly} contains {payload_tag_name} sha {pullspec_sha} but assembly computed rhcos:'
                            f' {entry.rhcos_build} and {actual_digest}',
                            component='reference-releases',
                        )
                    )
            else:
                raise IOError(f"Unsupported payload entry {entry}")

        return issues

    @staticmethod
    @start_as_current_span_async(TRACER, "GenPayloadCli.check_nightlies_consistency")
    async def check_nightlies_consistency(assembly_inspector: AssemblyInspector) -> List[AssemblyIssue]:
        """
        If this assembly has reference-releases, check whether the current images selected by the
        assembly are an exact match for the nightly contents.
        """

        basis = assembly_basis(assembly_inspector.runtime.get_releases_config(), assembly_inspector.runtime.assembly)
        if not basis or not basis.reference_releases:
            return []

        issues: List[AssemblyIssue] = []
        tasks = []
        generator = PayloadGenerator()
        for arch, nightly in basis.reference_releases.primitive().items():
            tasks.append(generator._check_nightly_consistency(assembly_inspector, nightly, arch))
        results = await asyncio.gather(*tasks)
        issues.extend([issue for result in results for issue in result])
        return issues

    @staticmethod
    def embargo_issues_for_payload(entries: Dict[str, PayloadEntry], arch: str) -> List[AssemblyIssue]:
        """
        Check to see if there are any embargoed builds in public image stream tags
        """
        issues = []
        for payload_tag, entry in entries.items():
            if not entry.build_record_inspector:
                # Probably RHCOS
                continue

            if entry.build_record_inspector.is_under_embargo():
                issues.append(
                    AssemblyIssue(
                        f"Found embargoed build {entry.build_record_inspector.get_nvr()} in payload entries for arch {arch}",
                        component=entry.image_meta.name,
                        code=AssemblyIssueCode.EMBARGOED_CONTENT,
                    )
                )
        return issues
