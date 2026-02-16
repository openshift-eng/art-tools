import asyncio
import hashlib
import json
import pathlib
import re
from collections import OrderedDict
from copy import copy
from functools import lru_cache
from multiprocessing import Event
from typing import Any, Dict, List, Optional, Set, Tuple, cast

from artcommonlib import util as artlib_util
from artcommonlib.konflux.konflux_build_record import ArtifactType, Engine, KonfluxBuildOutcome, KonfluxBuildRecord
from artcommonlib.model import Missing, Model
from artcommonlib.pushd import Dir
from artcommonlib.release_util import isolate_el_version_in_release
from artcommonlib.rpm_utils import parse_nvr, to_nevra
from artcommonlib.util import deep_merge
from dockerfile_parse import DockerfileParser

import doozerlib
from doozerlib import brew, coverity, util
from doozerlib.build_info import BrewBuildRecordInspector
from doozerlib.distgit import pull_image
from doozerlib.metadata import Metadata, RebuildHint, RebuildHintCode
from doozerlib.source_resolver import SourceResolver


@lru_cache(maxsize=256)
def extract_builder_info_from_pullspec(pullspec: str) -> tuple[int | None, tuple[int, int] | None]:
    """
    Extract RHEL version and golang version from a container image pullspec.

    This function is cached to avoid repeated image inspections for the same pullspec.

    First attempts to extract versions from the pullspec tag, then falls back to
    inspecting the image's 'release' and 'version' labels.

    Args:
        pullspec: Full image pullspec (e.g., registry.ci.openshift.org/ocp/builder:rhel-9-golang-1.21)

    Returns:
        Tuple of (rhel_version, golang_version) where:
        - rhel_version: int or None (e.g., 9)
        - golang_version: tuple of (major, minor) or None (e.g., (1, 21))
    """
    rhel_version = None
    golang_version = None

    try:
        # Try to extract versions from the tag (fast path)
        if ':' in pullspec:
            tag = pullspec.split(':')[-1]
            # Extract RHEL version from patterns like rhel-9, rhel9, ubi-9, ubi9, centos-9, centos9
            match = re.search(r'(?:rhel|ubi|centos)-?(\d+)', tag)
            if match:
                rhel_version = int(match.group(1))
                # Try to extract golang version from tag (e.g., golang-1.21 or golang-1.21.3)
                golang_match = re.search(r'golang-?(\d+)\.(\d+)', tag)
                if golang_match:
                    golang_version = (int(golang_match.group(1)), int(golang_match.group(2)))
                    # Got both values from tag parsing - return immediately
                    return rhel_version, golang_version

        # If the tag didn't contain a RHEL version, try matching against the full pullspec.
        # This handles pullspecs like registry.redhat.io/rhel9-eus/rhel-9.6-bootc:9.6
        # where the RHEL version appears in the registry path rather than the tag.
        if not rhel_version:
            match = re.search(r'(?:rhel|ubi|centos)-?(\d+)', pullspec)
            if match:
                rhel_version = int(match.group(1))

        # At this point, we're missing at least one value - fall back to inspecting image labels
        image_info = cast(Dict, util.oc_image_info_for_arch__caching(pullspec))
        labels = image_info['config']['config']['Labels']

        # Extract RHEL version from release label if we don't have it yet
        if not rhel_version:
            release_label = labels.get('release')
            if release_label:
                rhel_version = isolate_el_version_in_release(release_label)

        # Extract golang version from version label if we don't have it yet
        if not golang_version:
            version_label = labels.get('version')
            if version_label:
                version_fields = util.extract_version_fields(version_label, at_least=2)
                golang_version = (version_fields[0], version_fields[1])

    except Exception:
        # Return whatever we managed to extract before the error
        pass

    return rhel_version, golang_version


class ImageMetadata(Metadata):
    def __init__(
        self,
        runtime: "doozerlib.Runtime",
        data_obj: Dict,
        commitish: Optional[str] = None,
        clone_source: Optional[bool] = False,
        prevent_cloning: Optional[bool] = False,
        process_dependents: Optional[bool] = True,
    ):
        super(ImageMetadata, self).__init__('image', runtime, data_obj, commitish, prevent_cloning=prevent_cloning)
        self.required = self.config.get('required', False)
        self.parent = None
        self.children = []  # list of ImageMetadata which use this image as a parent.
        self.dependencies: Set[str] = set()
        dependents = self.config.get('dependents', [])

        # Only process dependents if this image is being added to image_map for building.
        # When loading an image just to query its latest build (process_dependents=False),
        # we should not add its dependents to image_map as a side effect.
        # This prevents unwanted circular dependencies and incorrect CSV image references.
        if process_dependents:
            for d in dependents:
                dependent = self.runtime.late_resolve_image(d, add=True, required=False)
                if not dependent:
                    continue
                dependent.dependencies.add(self.distgit_key)
                self.children.append(dependent)
        self.rebase_event = Event()
        """ Event that is set when this image is being rebased. """
        self.rebase_status = False
        """ True if this image has been successfully rebased. """
        self.build_event = Event()
        """ Event that is set when this image is being built. """
        self.build_status = False
        """ True if this image has been successfully built. """

        # Apply alternative upstream config if canonical builders is enabled
        # This must happen early so config is correct for all subsequent operations
        if self.canonical_builders_enabled:
            if prevent_cloning:
                self.logger.warning(
                    '[%s] canonical_builders_from_upstream is enabled but prevent_cloning=True. '
                    'Skipping upstream RHEL version detection; alternative_upstream config will not be applied.',
                    self.distgit_key,
                )
            else:
                if not clone_source:
                    self.logger.warning(
                        '[%s] canonical_builders_from_upstream is enabled but clone_source=False. '
                        'Source will be cloned anyway to determine upstream RHEL version.',
                        self.distgit_key,
                    )
                # When canonical_builders_from_upstream is enabled, we need to determine
                # the upstream RHEL version and merge alternative_upstream config if needed.
                # This will clone source as part of the process.
                self._apply_alternative_upstream_config()
        elif clone_source:
            # Normal case: clone source if requested (and canonical builders not enabled)
            runtime.source_resolver.resolve_source(self)

        self.installed_rpms = None

    @property
    def image_name(self):
        return self.config.name

    @property
    def image_name_short(self):
        return self.config.name.split('/')[-1]

    @property
    def is_olm_operator(self):
        """Returns whether this image is an OLM operator."""
        return self.config['update-csv'] is not Missing

    def get_olm_bundle_brew_component_name(self):
        """Returns the Brew component name of the OLM bundle for this OLM operator.

        :return: The Brew component name.
        :raises IOError: If the image is not an OLM operator.
        """
        if not self.is_olm_operator:
            raise IOError(f"[{self.distgit_key}] No update-csv config found in the image's metadata")
        return str(
            self.config.distgit.bundle_component
            or self.get_component_name().replace('-container', '-metadata-container')
        )

    def get_olm_bundle_short_name(self):
        """Returns the short name of the OLM bundle for this OLM operator.

        :return: The short name of the OLM bundle for this OLM operator.
        :raises IOError: If the image is not an OLM operator.
        """
        if not self.is_olm_operator:
            raise IOError(f"[{self.distgit_key}] No update-csv config found in the image's metadata")
        return f"{self.distgit_key}-bundle"

    def get_olm_bundle_image_name(self):
        """Returns the image name of the OLM bundle for this OLM operator.
        This is to be used in the "name" label of the OLM bundle.

        :return: The image name of the OLM bundle for this OLM operator.
        :raises IOError: If the image is not an OLM operator.
        """
        short_name = self.get_olm_bundle_short_name()

        # Use product name as the namespace prefix
        product = self.runtime.group_config.product if self.runtime.group_config.product else "openshift"

        # Only add "ose-" prefix for OpenShift products
        if product == "openshift" and not short_name.startswith('ose-'):
            short_name = 'ose-' + short_name

        return f"{product}/{short_name}"

    def get_olm_bundle_delivery_repo_name(self):
        """Returns the delivery repository name for the OLM bundle of this OLM operator.

        :return: The delivery repository name for the OLM bundle.
        :raises IOError: If the image is not an OLM operator.
        """
        if not self.is_olm_operator:
            raise IOError(f"[{self.distgit_key}] No update-csv config found in the image's metadata")
        repo_name = self.config.delivery.bundle_delivery_repo_name
        if repo_name is Missing:
            raise IOError(
                f"[{self.distgit_key}] No delivery.bundle_delivery_repo_name config found in the image's metadata"
            )
        return cast(str, repo_name)

    def get_assembly_rpm_package_dependencies(self, el_ver: int) -> Tuple[Dict[str, str], Dict[str, str]]:
        """
        An assembly can define RPMs which should be installed into a given member
        image. Those dependencies can be specified at either the individual member
        (higher priority) or at the group level. This method computes a
        Dict[package_name] -> nvr for any package that should be installed due to
        these overrides.
        :param el_ver: Which version of RHEL to check
        :return: Returns a tuple with two Dict[package_name] -> nvr for any assembly induced overrides.
                 The first entry in the Tuple are dependencies directly specified in the member.
                 The second entry are dependencies specified in the group and member.
        """
        direct_member_deps: Dict[str, str] = dict()  # Map package_name -> nvr
        aggregate_deps: Dict[str, str] = dict()  # Map package_name -> nvr
        eltag = f'el{el_ver}'
        group_deps = self.runtime.get_group_config().dependencies.rpms or []
        member_deps = self.config.dependencies.rpms or []

        for rpm_entry in group_deps:
            if eltag in rpm_entry:  # This entry has something for the requested RHEL version
                nvr = rpm_entry[eltag]
                package_name = parse_nvr(nvr)['name']
                aggregate_deps[package_name] = nvr

        # Perform the same process, but only for dependencies directly listed for the member
        for rpm_entry in member_deps:
            if eltag in rpm_entry:  # This entry has something for the requested RHEL version
                nvr = rpm_entry[eltag]
                package_name = parse_nvr(nvr)['name']
                direct_member_deps[package_name] = nvr
                aggregate_deps[package_name] = nvr  # Override anything at the group level

        return direct_member_deps, aggregate_deps

    def is_ancestor(self, image):
        """
        :param image: A Metadata object or a distgit_key to check.
        :return: Returns whether the specified image is an ancestor of this image (eg. a parent, or parent's parent...)
        """
        if isinstance(image, Metadata):
            image = image.distgit_key

        parent = self.parent
        while parent:
            if parent.distgit_key == image:
                return True
            parent = parent.parent

        return False

    def get_descendants(self):
        """
        :return: Returns a set of children imagemetadata and their children, ..etc.
        """
        descendants = set()
        for child in self.children:
            descendants.add(child)
            descendants.update(child.get_descendants())

        return descendants

    def resolve_parent(self):
        """
        :return: Resolves and returns imagemeta.parent attribute for this image's parent image OR None if no parent is defined.
        """
        if 'from' in self.config:
            if 'member' in self.config['from']:
                base = self.config['from']['member']
                try:
                    self.parent = self.runtime.resolve_image(base)
                except:
                    self.parent = None

                if self.parent:
                    self.parent.add_child(self)
        return self.parent

    def add_child(self, child):
        """
        Adds a child imagemetadata to this list of children for this image.
        :param child:
        :return:
        """
        self.children.append(child)

    @property
    def base_only(self):
        """
        Some images are marked base-only.  Return the flag from the config file
        if present.
        """
        return self.config.base_only

    @property
    def is_payload(self):
        val = self.config.get('for_payload', False)
        if val and self.base_only:
            raise ValueError(f'{self.distgit_key} claims to be for_payload and base_only')
        return val

    @property
    def for_release(self):
        val = self.config.get('for_release', True)
        if val and self.base_only:
            raise ValueError(f'{self.distgit_key} claims to be for_release and base_only')
        return val

    def get_payload_tag_info(self) -> Tuple[str, bool]:
        """
        If this image is destined for the OpenShift release payload, it will have a tag name
        associated within it within the release payload's originating imagestream.
        :return: Returns the tag name to use for this image in an
                OpenShift release imagestream and whether the payload name was explicitly included in the
                image metadata. See https://issues.redhat.com/browse/ART-2823 . i.e. (tag_name, explicitly_declared)
        """
        if not self.is_payload:
            raise ValueError('Attempted to derive payload name for non-payload image: ' + self.distgit_key)

        payload_name = self.config.get("payload_name")
        if payload_name:
            return payload_name, True
        else:
            payload_name = (
                self.image_name_short[4:] if self.image_name_short.startswith("ose-") else self.image_name_short
            )  # it _should_ but... to be safe
            return payload_name, False

    def get_brew_image_name_short(self):
        # Get image name in the Brew pullspec. e.g. openshift3/ose-ansible --> openshift3-ose-ansible
        return self.image_name.replace("/", "-")

    def pull_url(self):
        """
        Get the pullspec for the latest build of this image.

        Returns the pullspec from Konflux if build_system is 'konflux',
        otherwise returns the Brew pullspec.
        """
        if self.runtime.build_system == 'konflux':
            # Bind Konflux DB if not already bound
            if not hasattr(self.runtime, '_konflux_db_bound'):
                self.runtime.konflux_db.bind(KonfluxBuildRecord)
                self.runtime._konflux_db_bound = True

            # Get the latest Konflux build synchronously
            async def get_latest_konflux_build():
                return await self.runtime.konflux_db.get_latest_build(
                    name=self.distgit_key,
                    group=self.runtime.group,
                    assembly=self.runtime.assembly,
                    artifact_type=ArtifactType.IMAGE,
                    engine=Engine.KONFLUX,
                    outcome=KonfluxBuildOutcome.SUCCESS,
                    exclude_large_columns=True,
                )

            build = asyncio.run(get_latest_konflux_build())
            if not build:
                raise IOError(f'No Konflux build found for {self.distgit_key} in group {self.runtime.group}')
            return build.image_pullspec
        else:
            # Brew build system (existing behavior)
            # Don't trust what is the Dockerfile for version & release. This field may not even be present.
            # Query brew to find the most recently built release for this component version.
            _, version, release = self.get_latest_build_info(el_target=self.branch_el_target())

            # we need to pull images from proxy if 'brew_image_namespace' is enabled:
            # https://source.redhat.com/groups/public/container-build-system/container_build_system_wiki/pulling_pre_quay_switch_over_osbs_built_container_images_using_the_osbs_registry_proxy
            if self.runtime.group_config.urls.brew_image_namespace is not Missing:
                name = self.runtime.group_config.urls.brew_image_namespace + '/' + self.config.name.replace('/', '-')
            else:
                name = self.config.name

            return "{host}/{name}:{version}-{release}".format(
                host=self.runtime.group_config.urls.brew_image_host, name=name, version=version, release=release
            )

    def pull_image(self):
        pull_image(self.pull_url())

    def get_default_push_tags(self, version, release):
        push_tags = [
            "%s-%s" % (version, release),  # e.g. "v3.7.0-0.114.0.0"
            "%s" % version,  # e.g. "v3.7.0"
        ]

        # it's possible but rare that an image will have an alternate
        # tags along with the regular ones
        # append those to the tag list.
        if self.config.push.additional_tags is not Missing:
            push_tags.extend(self.config.push.additional_tags)

        # In v3.7, we use the last .0 in the release as a bump field to differentiate
        # image refreshes. Strip this off since OCP will have no knowledge of it when reaching
        # out for its node image.
        if "." in release:
            # Strip off the last field; "0.114.0.0" -> "0.114.0"
            push_tags.append("%s-%s" % (version, release.rsplit(".", 1)[0]))

        # Push as v3.X; "v3.7.0" -> "v3.7"
        push_tags.append("%s" % (version.rsplit(".", 1)[0]))
        return push_tags

    def get_default_repos(self):
        """
        :return: Returns a list of ['ns/repo', 'ns/repo'] found in the image config yaml specified for default pushes.
        """
        # Repos default to just the name of the image (e.g. 'openshift3/node')
        default_repos = [self.config.name]

        # Unless overridden in the config.yml
        if self.config.push.repos is not Missing:
            default_repos = self.config.push.repos.primitive()

        return default_repos

    def get_default_push_names(self):
        """
        :return: Returns a list of push names that should be pushed to for registries defined in
        group.yml and for additional repos defined in image config yaml.
        (e.g. ['registry/ns/repo', 'registry/ns/repo', ...]).
        """

        # Will be built to include a list of 'registry/ns/repo'
        push_names = []

        default_repos = self.get_default_repos()  # Get a list of [ ns/repo, ns/repo, ...]

        default_registries = []
        if self.runtime.group_config.push.registries is not Missing:
            default_registries = self.runtime.group_config.push.registries.primitive()

        for registry in default_registries:
            registry = registry.rstrip("/")  # Remove any trailing slash to avoid mistaking it for a namespace
            for repo in default_repos:
                namespace, repo_name = repo.split('/')
                if '/' in registry:  # If registry overrides namespace
                    registry, namespace = registry.split('/')
                push_names.append('{}/{}/{}'.format(registry, namespace, repo_name))

        # image config can contain fully qualified image names to push to (registry/ns/repo)
        if self.config.push.also is not Missing:
            push_names.extend(self.config.push.also)

        return push_names

    def get_additional_push_names(self, additional_registries):
        """
        :return: Returns a list of push names based on a list of additional registries that
        need to be pushed to (e.g. ['registry/ns/repo', 'registry/ns/repo', ...]).
        """

        if not additional_registries:
            return []

        # Will be built to include a list of 'registry/ns/repo'
        push_names = []

        default_repos = self.get_default_repos()  # Get a list of [ ns/repo, ns/repo, ...]

        for registry in additional_registries:
            registry = registry.rstrip("/")  # Remove any trailing slash to avoid mistaking it for a namespace
            for repo in default_repos:
                namespace, repo_name = repo.split('/')
                if '/' in registry:  # If registry overrides namespace
                    registry, namespace = registry.split('/')
                push_names.append('{}/{}/{}'.format(registry, namespace, repo_name))

        return push_names

    # Class methods to speed up computation of does_image_need_change if multiple
    # images will be assessed.
    # Mapping of brew pullspec => most recent brew build dict.
    builder_image_builds = dict()

    async def does_image_need_change(
        self, changing_rpm_packages=None, buildroot_tag=None, newest_image_event_ts=None, oldest_image_event_ts=None
    ) -> Tuple[Metadata, RebuildHint]:
        """
        Answers the question of whether the latest built image needs to be rebuilt based on
        the packages (and therefore RPMs) it is dependent on might have changed in tags
        relevant to the image. A check is also made if the image depends on a package
        we know is changing because we are about to rebuild it.
        :param changing_rpm_packages: A list of package names that are about to change.
        :param buildroot_tag: The build root for this image
        :param newest_image_event_ts: The build timestamp of the most recently built image in this group.
        :param oldest_image_event_ts: The build timestamp of the oldest build in this group from getLatestBuild of each component.
        :return: (meta, RebuildHint).
        """

        if not changing_rpm_packages:
            changing_rpm_packages = []

        dgk = self.distgit_key
        runtime = self.runtime

        with runtime.pooled_koji_client_session() as koji_api:
            image_build = self.get_latest_brew_build(default='')
            if not image_build:
                # Seems this have never been built. Mark it as needing change.
                return self, RebuildHint(RebuildHintCode.NO_LATEST_BUILD, 'Image has never been built before')

            image_nvr = image_build['nvr']
            self.logger.debug(f'Image {dgk} latest is {image_nvr}')

            image_build_event_id = image_build['creation_event_id']  # the brew event that created this build
            self.logger.info(f'Running a change assessment on {image_nvr} built at event {image_build_event_id}')

            # Very rarely, an image might need to pull a package that is not actually installed in the
            # builder image or in the final image.
            # e.g. https://github.com/openshift/ironic-ipa-downloader/blob/999c80f17472d5dbbd4775d901e1be026b239652/Dockerfile.ocp#L11-L14
            # This is programmatically undetectable through koji queries. So we allow extra scan-sources hints to
            # be placed in the image metadata.
            if self.config.scan_sources.extra_packages is not Missing:
                for package_details in self.config.scan_sources.extra_packages:
                    extra_package_name = package_details.name
                    extra_package_brew_tag = package_details.tag
                    # Example of queryHistory: https://gist.github.com/jupierce/943b845c07defe784522fd9fd76f4ab0
                    extra_latest_tagging_infos = koji_api.queryHistory(
                        table='tag_listing', tag=extra_package_brew_tag, package=extra_package_name, active=True
                    )['tag_listing']

                    if extra_latest_tagging_infos:
                        extra_latest_tagging_infos.sort(key=lambda event: event['create_event'])
                        # We have information about the most recent time this package was tagged into the
                        # relevant tag. Why the tagging event and not the build time? Well, the build could have been
                        # made long ago, but only tagged into the relevant tag recently.
                        extra_latest_tagging_event = extra_latest_tagging_infos[-1]['create_event']
                        self.logger.debug(
                            f'Checking image creation time against extra_packages {extra_package_name} in tag {extra_package_brew_tag} @ tagging event {extra_latest_tagging_event}'
                        )
                        if extra_latest_tagging_event > image_build_event_id:
                            return self, RebuildHint(
                                RebuildHintCode.PACKAGE_CHANGE,
                                f'Image {dgk} is sensitive to extra_packages {extra_package_name} which changed at event {extra_latest_tagging_event}',
                            )
                    else:
                        self.logger.warning(
                            f'{dgk} unable to find tagging event for for extra_packages {extra_package_name} in tag {extra_package_brew_tag} ; Possible metadata error.'
                        )

            # Collect build times from any parent/builder images used to create this image
            builders = list(self.config['from'].builder) or []
            builders.append(self.config['from'])  # Add the parent image to the builders
            for builder in builders:
                if builder.member:
                    # We can't determine if images are about to change. Defer to scan-sources.
                    continue

                if builder.image:
                    builder_image_name = builder.image
                elif builder.stream:
                    builder_image_name = runtime.resolve_stream(builder.stream).image
                else:
                    raise IOError(f'Unable to determine builder or parent image pullspec from {builder}')

                if "." in builder_image_name.split('/', 2)[0]:
                    # looks like full pullspec with domain name; e.g. "registry.redhat.io/ubi8/nodejs-12:1-45"
                    builder_image_url = builder_image_name
                else:
                    # Assume this is a org/repo name relative to brew; e.g. "openshift/ose-base:ubi8"
                    builder_image_url = self.runtime.resolve_brew_image_url(builder_image_name)

                builder_brew_build = ImageMetadata.builder_image_builds.get(builder_image_url, None)

                if not builder_brew_build:
                    latest_builder_image_info = Model(util.oc_image_info_for_arch__caching(builder_image_url))
                    builder_info_labels = latest_builder_image_info.config.config.Labels
                    builder_nvr_list = [
                        builder_info_labels['com.redhat.component'],
                        builder_info_labels['version'],
                        builder_info_labels['release'],
                    ]

                    if not all(builder_nvr_list):
                        raise IOError(f'Unable to find nvr in {builder_info_labels}')

                    builder_image_nvr = '-'.join(builder_nvr_list)
                    builder_brew_build = koji_api.getBuild(builder_image_nvr)
                    ImageMetadata.builder_image_builds[builder_image_url] = builder_brew_build
                    self.logger.debug(
                        f'Found that builder or parent image {builder_image_url} has event {builder_brew_build["creation_event_id"]}'
                    )

                if image_build_event_id < builder_brew_build['creation_event_id']:
                    self.logger.info(f'will be rebuilt because a builder or parent image changed: {builder_image_name}')
                    return self, RebuildHint(
                        RebuildHintCode.BUILDER_CHANGING,
                        f'A builder or parent image {builder_image_name} has changed since {image_nvr} was built',
                    )

            self.logger.info("Getting RPMs contained in %s", image_nvr)
            bbii = BrewBuildRecordInspector(self.runtime, image_build)

            arch_archives = bbii.get_arch_archives()
            build_arches = set(arch_archives.keys())
            target_arches = set(self.get_arches())
            if target_arches != build_arches:
                # The latest brew build does not exactly match the required arches as specified in group.yml
                return self, RebuildHint(
                    RebuildHintCode.ARCHES_CHANGE,
                    f'Arches of {image_nvr}: ({build_arches}) does not match target arches {target_arches}',
                )

            # Build up a map of RPMs built by this group. It is the 'latest' builds of these RPMs
            # relative to the current assembly that matter in the forthcoming search -- not
            # necessarily the true-latest. i.e. if we are checking for changes in a 'stream' image,
            # we do not want to react because of an RPM build in the 'test' assembly.
            group_rpm_builds_nvrs: Dict[str, str] = dict()  # Maps package name to latest brew build nvr
            for rpm_meta in self.runtime.rpm_metas():
                latest_rpm_build = rpm_meta.get_latest_brew_build(default=None)
                if latest_rpm_build:
                    group_rpm_builds_nvrs[rpm_meta.get_package_name()] = latest_rpm_build['nvr']

            # Populate all rpms contained in the image build
            arch_rpms: Dict[str, List[Dict]] = {}  # arch => list of rpm dicts to check for updates in configured repos
            for arch, archive_inspector in arch_archives.items():
                # Example results of listing RPMs in an given imageID:
                # https://gist.github.com/jupierce/a8798858104dcf6dfa4bd1d6dd99d2d8
                rpm_entries = archive_inspector.get_installed_rpm_dicts()
                for rpm_entry in rpm_entries:
                    is_exempt, pattern = self.is_rpm_exempt(rpm_entry["name"])
                    if is_exempt:
                        self.logger.info(f"{to_nevra(rpm_entry)} is exempt from rpm change detection by '{pattern}'")
                        continue

                    build_id = rpm_entry['build_id']
                    build = koji_api.getBuild(build_id, brew.KojiWrapperOpts(caching=True))
                    package_name = build['package_name']
                    if package_name in changing_rpm_packages:
                        return self, RebuildHint(
                            RebuildHintCode.PACKAGE_CHANGE,
                            f'Image includes {package_name} which is also about to change',
                        )

                    latest_assembly_build_nvr = group_rpm_builds_nvrs.get(package_name, None)
                    if latest_assembly_build_nvr and latest_assembly_build_nvr == build['nvr']:
                        # The latest RPM build for this assembly is already installed and we know the RPM
                        # is not about to change. Ignore the installed package.
                        self.logger.debug(
                            f'Found latest assembly specific build ({latest_assembly_build_nvr}) for group package {package_name} is already installed in {dgk} archive; no tagging change search will occur'
                        )
                        continue
                    # Add this rpm_entry to arch_rpms in order to chech whether it is latest in repos
                    if arch not in arch_rpms:
                        arch_rpms[arch] = []
                    arch_rpms[arch].append(rpm_entry)

        if arch_rpms:
            self.logger.info('Checking whether any of the installed rpms is outdated')
            non_latest_rpms = await bbii.find_non_latest_rpms(arch_rpms)
            rebuild_hints = [
                f"Outdated RPM {installed_rpm} installed in {image_nvr} ({arch}) when {latest_rpm} was available in repo {repo}"
                for arch, non_latest in non_latest_rpms.items()
                for installed_rpm, latest_rpm, repo in non_latest
            ]
            if rebuild_hints:
                return self, RebuildHint(
                    RebuildHintCode.PACKAGE_CHANGE,
                    ";\n".join(rebuild_hints),
                )
        return self, RebuildHint(RebuildHintCode.BUILD_IS_UP_TO_DATE, 'No change detected')

    def covscan(self, cc: coverity.CoverityContext) -> bool:
        self.logger.info('Setting up for coverity scan')
        dgr = self.distgit_repo()
        with Dir(dgr.distgit_dir):
            if coverity.run_covscan(cc):
                cc.mark_results_done()
                return True
            else:
                self.logger.error('Error computing coverity results for this image')
                return False

    def calculate_config_digest(self, group_config, streams):
        ignore_keys = [
            "owners",
            "okd",
            "scan_sources",
            "content.source.ci_alignment",
            "content.source.git",
            "external_scanners",
            "delivery",
        ]  # list of keys that shouldn't be involved in config digest calculation
        image_config = copy(self.config)
        # If there is a konflux stanza in the image config, merge it with the main config
        if image_config.konflux is not Missing:
            image_config = Model(deep_merge(image_config.primitive(), image_config.konflux.primitive()))
        image_config: Dict[str, Any] = image_config.primitive()

        group_config: Dict[str, Any] = group_config.primitive()
        streams: Dict[str, Any] = streams.primitive()

        # Remove image_config fields specified in ignore_keys
        for key in ignore_keys:
            c = image_config
            seg = None
            p = None
            for seg in key.split("."):
                if seg not in c:
                    p = None
                    break
                p = c
                c = c[seg]
            if p and seg:
                del p[seg]

        message = {
            "config": image_config,
        }

        repos = set(image_config.get("enabled_repos", []) + image_config.get("non_shipping_repos", []))
        if repos:
            # Use runtime.repos which handles both old-style and new-style configurations
            message["repos"] = {repo: self.runtime.repos[repo].to_dict() for repo in repos}

        builders = image_config.get("from", {}).get("builder", [])
        from_stream = image_config.get("from", {}).get("stream")
        referred_streams = {builder.get("stream") for builder in builders if builder.get("stream")}
        if from_stream:
            referred_streams.add(from_stream)
        if referred_streams:
            message["streams"] = {
                stream: self.runtime.resolve_stream(stream).get('image') for stream in referred_streams
            }

        # Avoid non serializable objects. Known to occur for PosixPath objects in content.source.modifications.
        def default(o):
            return f"<<non-serializable: {type(o).__qualname__}>>"

        digest = hashlib.sha256(json.dumps(message, sort_keys=True, default=default).encode("utf-8")).hexdigest()
        return "sha256:" + digest

    @property
    def canonical_builders_enabled(self) -> bool:
        """Returns whether canonical_builders is enabled for this image."""
        # canonical_builders_from_upstream can be overridden by every single image; if it's not, use the global one
        if self.config.canonical_builders_from_upstream is not Missing:
            canonical_builders_from_upstream = self.config.canonical_builders_from_upstream
        else:
            canonical_builders_from_upstream = self.runtime.group_config.canonical_builders_from_upstream

        if not isinstance(canonical_builders_from_upstream, bool):
            self.logger.warning(
                'Invalid value provided for "canonical_builders_from_upstream": %s, defaulting to False',
                canonical_builders_from_upstream,
            )
            return False
        return canonical_builders_from_upstream

    def _apply_alternative_upstream_config(self) -> None:
        """
        When canonical_builders_enabled, merge alternative_upstream config based on upstream RHEL version.
        This must happen early in initialization so config is correct for all subsequent operations.

        This method determines both ART and upstream intended RHEL versions, then merges the appropriate
        alternative_upstream config stanza if the versions differ. If versions match, no merge is needed.

        Raises:
            IOError: If image doesn't have upstream source
            IOError: If source is not available or upstream RHEL version cannot be determined
            IOError: If RHEL versions differ but no matching alternative_upstream config is found
        """
        # Check if this image has upstream source
        if not self.has_source():
            raise IOError(
                f'[{self.distgit_key}] canonical_builders_from_upstream is enabled but image does not have '
                'upstream source. Cannot determine upstream RHEL version.'
            )

        # Determine ART intended RHEL version using branch_el_target()
        art_intended_el_version = self.branch_el_target()

        # Get the source resolution
        source_resolution = self.runtime.source_resolver.resolve_source(self)
        if not source_resolution:
            raise IOError(
                f'[{self.distgit_key}] canonical_builders_from_upstream is enabled but source could not be resolved. '
                'Cannot determine upstream RHEL version.'
            )

        # Get the source directory
        source_dir = SourceResolver.get_source_dir(source_resolution, self)

        # Determine upstream intended RHEL version and golang version from upstream Dockerfile
        upstream_intended_el_version, _ = self._determine_upstream_builder_info(source_dir)

        if not upstream_intended_el_version:
            raise IOError(
                f'[{self.distgit_key}] Could not determine upstream RHEL version. '
                'Cannot apply alternative_upstream config.'
            )

        # If ART and upstream RHEL versions match, no config merge needed
        if upstream_intended_el_version == art_intended_el_version:
            self.logger.info(
                '[%s] ART and upstream intended RHEL versions match (el%s). No alternative config merge needed.',
                self.distgit_key,
                art_intended_el_version,
            )
            return

        # RHEL versions differ - look for matching alternative_upstream config
        alt_configs = self.config.alternative_upstream
        matched = False
        for alt_config in alt_configs or []:
            if alt_config['when'] == f'el{upstream_intended_el_version}':
                self.logger.info(
                    '[%s] Merging rhel%s alternative_upstream config to match upstream',
                    self.distgit_key,
                    upstream_intended_el_version,
                )
                # Merge the alternative config
                self.config = Model(deep_merge(self.config.primitive(), alt_config.primitive()))
                matched = True
                break

        if not matched:
            raise IOError(
                f'[{self.distgit_key}] Upstream uses el{upstream_intended_el_version} but ART uses '
                f'el{art_intended_el_version}, and no matching alternative_upstream config was found. '
                f'Please add an alternative_upstream config with "when: el{upstream_intended_el_version}".'
            )

        # Update targets after config merge
        self.targets = self.determine_targets()

    @staticmethod
    def _is_pullspec_unresolved(pullspec: str | None) -> bool:
        """
        Check whether a Dockerfile FROM pullspec is missing or contains unresolved
        ARG references.

        A pullspec is considered unresolved if it is None/empty, still contains "${"
        (an unexpanded ARG substitution), or equals ":" (the result of
        FROM ${IMAGE}:${TAG} when both ARGs have no defaults).

        Arg(s):
            pullspec (str | None): The image pullspec string from a Dockerfile FROM directive.

        Return Value(s):
            bool: True if the pullspec is unresolved, False otherwise.
        """
        return not pullspec or "${" in pullspec or pullspec == ":"

    def _determine_upstream_builder_info(
        self, source_dir: pathlib.Path
    ) -> Tuple[Optional[int], Optional[Tuple[int, int]]]:
        """
        Determine the upstream intended RHEL version and golang version from the last build layer in the upstream Dockerfile.

        Delegates to extract_builder_info_from_pullspec() for the actual extraction logic.

        If the upstream Dockerfile uses ARG substitutions without defaults (e.g. FROM ${BASE_IMAGE_URL}:${BASE_IMAGE_TAG}),
        the pullspec will be unresolvable (e.g. ":"). In that case, apply the content.source.modifications (replace actions)
        to the Dockerfile content before re-parsing, as the modifications may provide the ARG default values needed to
        resolve the FROM directive.

        Arg(s):
            source_dir: Path to the upstream source directory (already includes content.source.path if applicable)

        Return Value(s):
            Tuple of (rhel_version, golang_version) where:
            - rhel_version: int or None (e.g., 9)
            - golang_version: tuple of (major, minor) or None (e.g., (1, 21))
        """
        df_name = self.config.content.source.dockerfile
        if not df_name:
            df_name = 'Dockerfile'

        df_path = source_dir.joinpath(df_name)
        rhel_version = None
        golang_version = None

        try:
            with open(df_path) as f:
                df_content = f.read()

            dfp = DockerfileParser()
            dfp.content = df_content
            parent_images = dfp.parent_images

            # We will infer the versions from the last build layer in the upstream Dockerfile
            last_layer_pullspec = parent_images[-1]

            # If the pullspec contains unresolved ARG references, apply content.source.modifications
            # (replace actions) to the Dockerfile content and re-parse.
            if self._is_pullspec_unresolved(last_layer_pullspec):
                self.logger.info(
                    '[%s] Upstream Dockerfile last FROM has unresolved ARG references ("%s"), '
                    'applying content.source.modifications and re-parsing',
                    self.distgit_key,
                    last_layer_pullspec,
                )
                last_layer_pullspec = self._resolve_pullspec_with_modifications(df_content)

            if last_layer_pullspec:
                # Use the cached function to extract builder info
                rhel_version, golang_version = extract_builder_info_from_pullspec(last_layer_pullspec)

        except Exception as e:
            # Log the exception but don't raise - caller will decide how to handle None return
            self.logger.warning('[%s] Failed determining upstream builder info: %s', self.distgit_key, e)

        if not rhel_version:
            self.logger.warning('[%s] Could not determine rhel version from upstream', self.distgit_key)
        if not golang_version:
            self.logger.debug('[%s] Could not determine golang version from upstream', self.distgit_key)

        return rhel_version, golang_version

    def _resolve_pullspec_with_modifications(self, df_content: str) -> str | None:
        """
        Apply content.source.modifications (replace actions) to the Dockerfile content
        and re-parse to extract the last parent image pullspec.

        Arg(s):
            df_content: The raw Dockerfile content string.

        Return Value(s):
            The resolved pullspec string, or None if it cannot be resolved.
        """
        modifications = self.config.content.source.modifications
        if not modifications:
            self.logger.warning(
                '[%s] No content.source.modifications defined; cannot resolve unresolved ARG references',
                self.distgit_key,
            )
            return None

        # Apply replace modifications to the Dockerfile content
        modified_content = df_content
        for mod in modifications:
            if mod.get('action') == 'replace' and mod.get('match') and mod.get('replacement') is not None:
                modified_content = modified_content.replace(mod['match'], mod['replacement'])

        # Re-parse the modified Dockerfile
        dfp = DockerfileParser()
        dfp.content = modified_content
        parent_images = dfp.parent_images

        if not parent_images:
            self.logger.warning('[%s] No parent images found after applying modifications', self.distgit_key)
            return None

        pullspec = parent_images[-1]
        if self._is_pullspec_unresolved(pullspec):
            self.logger.warning(
                '[%s] Pullspec still unresolved after applying modifications: "%s"',
                self.distgit_key,
                pullspec,
            )
            return None

        self.logger.info(
            '[%s] Resolved pullspec after applying modifications: "%s"',
            self.distgit_key,
            pullspec,
        )
        return pullspec

    def _default_brew_target(self):
        """Returns derived brew target name from the distgit branch name"""
        return f"{self.branch()}-containers-candidate"

    @property
    def image_build_method(self):
        return self.config.image_build_method or self.runtime.group_config.default_image_build_method or "osbs2"

    def get_parent_members(self) -> "OrderedDict[str, Optional[ImageMetadata]]":
        """Get the dict of parent members for the given image metadata.
        If a parent is not loaded, its value will be None.
        """
        parent_members = OrderedDict()
        image_from = self.config.get('from', {})
        for parent in image_from.get("builder", []) + [image_from]:
            member_name = parent.get("member")
            if not member_name:
                continue
            member = self.runtime.resolve_image(member_name, required=False)
            if member is None:
                self.logger.warning(f"Parent member {member_name} not loaded for {self.distgit_key}")
            parent_members[member_name] = member
        return parent_members

    def is_cachi2_enabled(self):
        """
        Determine if cachi2 is enabled or not
        image config override > group config override > fallback to cachito config
        """

        cachi2_config_override = self.config.konflux.cachi2.enabled
        cachi2_group_override = self.runtime.group_config.konflux.cachi2.enabled

        if cachi2_config_override not in [Missing, None]:
            # If cachi2 override is defined in image metadata
            cachi2_enabled = cachi2_config_override
            source = "metadata config"

        elif cachi2_group_override not in [Missing, None]:
            # If cachi2 override is defined in group metadata
            cachi2_enabled = cachi2_group_override
            source = "group config"

        else:
            # Enable cachi2 based on cachito config
            cachi2_enabled = artlib_util.is_cachito_enabled(
                metadata=self, group_config=self.runtime.group_config, logger=self.logger
            )
            source = "cachito config"

        self.logger.info("cachi2 %s from %s", "enabled" if cachi2_enabled else "disabled", source)
        return cachi2_enabled

    def is_lockfile_generation_enabled(self) -> bool:
        """
        Determines whether lockfile generation is enabled for the current image configuration.

        The method checks preconditions in the following order:
        1. Cachi2 feature must be enabled
        2. enabled_repos must be defined and not empty
        3. Lockfile generation overrides:
           - Image metadata configuration (`self.config.konflux.cachi2.lockfile.enabled`)
           - Group configuration (`self.runtime.group_config.konflux.cachi2.lockfile.enabled`)
        If no override is set, lockfile generation defaults to enabled.

        Returns:
            bool: True if lockfile generation is enabled, False otherwise.
        """
        lockfile_enabled = True

        # First check: cachi2 must be enabled
        cachi2_enabled = self.is_cachi2_enabled()
        if not cachi2_enabled:
            return False

        # Second check: enabled_repos must exist and not be empty
        enabled_repos = self.config.get("enabled_repos", [])
        if not enabled_repos:
            self.logger.info("Lockfile generation disabled: enabled_repos is empty or not defined")
            return False

        # Third check: lockfile-specific overrides
        source = None
        lockfile_config_override = self.config.konflux.cachi2.lockfile.enabled

        if lockfile_config_override not in [Missing, None]:
            lockfile_enabled = bool(lockfile_config_override)
            source = "metadata config"

        elif (lockfile_group_override := self.runtime.group_config.konflux.cachi2.lockfile.enabled) not in [
            Missing,
            None,
        ]:
            lockfile_enabled = bool(lockfile_group_override)
            source = "group config"

        self.logger.info(f"Lockfile generation set from {source} {lockfile_enabled}")

        return lockfile_enabled

    def is_dnf_modules_enable_enabled(self) -> bool:
        """
        Determines whether DNF module enablement command injection is enabled.

        Checks configuration in the following order:
        1. Image metadata configuration (konflux.cachi2.lockfile.dnf_modules_enable)
        2. Group configuration (runtime.group_config.konflux.cachi2.lockfile.dnf_modules_enable)

        If neither is set, DNF modules enablement defaults to enabled (True).

        Returns:
            bool: True if DNF module enablement injection is enabled, False otherwise.
        """
        dnf_modules_enable_config_override = self.config.konflux.cachi2.lockfile.dnf_modules_enable
        if dnf_modules_enable_config_override not in [Missing, None]:
            dnf_modules_enable = bool(dnf_modules_enable_config_override)
            self.logger.info(f"DNF modules enablement set from metadata config: {dnf_modules_enable}")
            return dnf_modules_enable

        dnf_modules_enable_group_override = self.runtime.group_config.konflux.cachi2.lockfile.dnf_modules_enable
        if dnf_modules_enable_group_override not in [Missing, None]:
            dnf_modules_enable = bool(dnf_modules_enable_group_override)
            self.logger.info(f"DNF modules enablement set from group config: {dnf_modules_enable}")
            return dnf_modules_enable

        return True

    def is_cross_arch_enabled(self) -> bool:
        """
        Determines whether cross-architecture lockfile inclusion is enabled.

        Checks configuration in the following order:
        1. Image metadata configuration (konflux.cachi2.lockfile.cross_arch)
        2. Group configuration (runtime.group_config.konflux.cachi2.lockfile.cross_arch)

        If neither is set, cross-architecture inclusion defaults to disabled (False).

        Returns:
            bool: True if cross-architecture lockfile inclusion is enabled, False otherwise.
        """
        cross_arch_config_override = self.config.konflux.cachi2.lockfile.cross_arch
        if cross_arch_config_override not in [Missing, None]:
            cross_arch = bool(cross_arch_config_override)
            self.logger.info(f"Cross-architecture lockfile inclusion set from metadata config: {cross_arch}")
            return cross_arch

        cross_arch_group_override = self.runtime.group_config.konflux.cachi2.lockfile.cross_arch
        if cross_arch_group_override not in [Missing, None]:
            cross_arch = bool(cross_arch_group_override)
            self.logger.info(f"Cross-architecture lockfile inclusion set from group config: {cross_arch}")
            return cross_arch

        return False

    async def fetch_rpms_from_build(self) -> set[str]:
        """
        Fetch RPM packages from database installed_rpms field.

        Returns either the full image RPM set or difference from parent packages,
        based on the inspect_parent configuration. Caches result in installed_rpms attribute.

        Returns:
            set[str]: Source RPM package names - full set if inspect_parent=False,
                     otherwise difference between this image's packages and parent packages
        """
        if hasattr(self, 'installed_rpms') and self.installed_rpms is not None:
            self.logger.debug(f"Using cached installed_rpms for {self.distgit_key}: {len(self.installed_rpms)} RPMs")
            return set(self.installed_rpms)

        try:
            base_search_params = {
                'group': self.runtime.group,
                'outcome': "success",
                'engine': self.runtime.build_system,
            }

            base_search_params['name'] = self.distgit_key
            # Need installed_rpms column for this operation, so don't exclude any columns
            build = await self.runtime.konflux_db.get_latest_build(**base_search_params)
            if not build:
                self.logger.debug(f"No build record found for {self.distgit_key}/{self.runtime.group}")
                self.installed_rpms = []
                return set()

            rpms = set(build.installed_rpms or [])

            if not rpms:
                self.logger.debug(
                    f"Build record for {self.distgit_key} has no installed_rpms, skipping parent calculation"
                )
                self.installed_rpms = []
                return set()

            self.installed_rpms = list(rpms)
            return rpms

        except Exception as e:
            self.logger.error(f"Failed to fetch RPMs for {self.distgit_key}/{self.runtime.group}: {e}")
            self.installed_rpms = []
            return set()

    async def get_lockfile_rpms_to_install(self) -> set[str]:
        """
        Get the union of RPMs from build and lockfile configuration for lockfile generation.

        This method returns the union of RPMs from two sources:
        1. RPMs from the build database (via fetch_rpms_from_build)
        2. RPMs defined in meta.config.konflux.cachi2.lockfile.rpms

        The method follows the same pattern as other lockfile-related methods,
        checking if lockfile generation is enabled before proceeding.

        Returns:
            set[str]: Union of RPM package names from both sources, or empty set if
                     lockfile generation is not enabled
        """

        # Get RPMs from build
        rpms_from_build = await self.fetch_rpms_from_build()

        # Get RPMs from lockfile config
        rpms_from_config = set()
        lockfile_rpms = self.config.konflux.cachi2.lockfile.get('rpms', [])
        if lockfile_rpms not in [Missing, None]:
            rpms_from_config = set(lockfile_rpms)
            if rpms_from_config:  # Only log if there are actually RPMs
                self.logger.info(f'{self.distgit_key} adding {len(rpms_from_config)} RPMs from lockfile config')

        # Return union of both sources
        return rpms_from_build.union(rpms_from_config)

    def get_lockfile_modules_to_install(self) -> set[str]:
        """
        Get module names for lockfile generation from configuration.

        Follows the same pattern as get_lockfile_rpms_to_install() for consistency.
        Configuration path: konflux.cachi2.lockfile.modules

        Returns:
            set[str]: Module names specified in lockfile configuration
        """
        modules_from_config = set()
        lockfile_modules = self.config.konflux.cachi2.lockfile.get('modules', [])

        if lockfile_modules not in [Missing, None]:
            modules_from_config = set(lockfile_modules)
            if modules_from_config:
                self.logger.info(f'{self.distgit_key} adding {len(modules_from_config)} modules from lockfile config')

        return modules_from_config

    def get_enabled_repos(self) -> set[str]:
        """
        Get enabled repositories for lockfile generation.

        A repo is considered enabled only if it is BOTH:
        1. Enabled in group.yml (Repo.enabled == True)
        2. Listed in this image's enabled_repos config

        Returns:
            set[str]: Repository names enabled for this image
        """
        image_enabled_repos = set(self.config.get("enabled_repos", []))
        if not image_enabled_repos:
            return set()

        # Get globally enabled repos from group config
        globally_enabled = {r.name for r in self.runtime.repos.values() if r.enabled}

        # Return intersection - repos must be enabled in BOTH places
        return globally_enabled & image_enabled_repos

    def is_artifact_lockfile_enabled(self) -> bool:
        """
        Determines whether artifact lockfile generation is enabled for the current image configuration.

        The method checks preconditions in the following order:
        1. Cachi2 feature must be enabled
        2. Artifact resources must be defined and not empty
        3. Artifact lockfile generation overrides:
           - Image metadata configuration (`self.config.konflux.cachi2.artifact_lockfile.enabled`)
           - Group configuration (`self.runtime.group_config.konflux.cachi2.artifact_lockfile.enabled`)
        If no override is set, artifact lockfile generation defaults to enabled.

        Returns:
            bool: True if artifact lockfile generation is enabled, False otherwise.
        """
        artifact_lockfile_enabled = True

        # First check: cachi2 must be enabled
        cachi2_enabled = self.is_cachi2_enabled()
        if not cachi2_enabled:
            return False

        # Second check: artifact resources must exist and not be empty
        artifact_resources = self.config.konflux.cachi2.artifact_lockfile.resources
        if artifact_resources in [Missing, None] or not artifact_resources:
            self.logger.info("Artifact lockfile generation disabled: artifact resources not defined")
            return False

        # Third check: artifact lockfile-specific overrides
        artifact_lockfile_config_override = self.config.konflux.cachi2.artifact_lockfile.enabled
        if artifact_lockfile_config_override not in [Missing, None]:
            artifact_lockfile_enabled = bool(artifact_lockfile_config_override)
            self.logger.info(f"Artifact lockfile generation set from metadata config {artifact_lockfile_enabled}")
        else:
            artifact_lockfile_group_override = self.runtime.group_config.konflux.cachi2.artifact_lockfile.enabled
            if artifact_lockfile_group_override not in [Missing, None]:
                artifact_lockfile_enabled = bool(artifact_lockfile_group_override)
                self.logger.info(f"Artifact lockfile generation set from group config {artifact_lockfile_enabled}")

        return artifact_lockfile_enabled

    def get_required_artifacts(self) -> list:
        """Get list of required artifact URLs from image config."""
        if not self.is_artifact_lockfile_enabled():
            return []

        resource_urls = self.config.konflux.cachi2.artifact_lockfile.resources
        if resource_urls in [Missing, None]:
            return []

        return resource_urls  # Direct URL list
