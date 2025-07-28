import hashlib
import json
from collections import OrderedDict
from copy import copy
from multiprocessing import Event
from typing import Any, Dict, List, Optional, Set, Tuple, cast

from artcommonlib import util as artlib_util
from artcommonlib.model import Missing, Model
from artcommonlib.pushd import Dir
from artcommonlib.rpm_utils import parse_nvr, to_nevra
from artcommonlib.util import deep_merge

import doozerlib
from doozerlib import brew, coverity, util
from doozerlib.build_info import BrewBuildRecordInspector
from doozerlib.distgit import pull_image
from doozerlib.metadata import Metadata, RebuildHint, RebuildHintCode


class ImageMetadata(Metadata):
    def __init__(
        self,
        runtime: "doozerlib.Runtime",
        data_obj: Dict,
        commitish: Optional[str] = None,
        clone_source: Optional[bool] = False,
        prevent_cloning: Optional[bool] = False,
    ):
        super(ImageMetadata, self).__init__('image', runtime, data_obj, commitish, prevent_cloning=prevent_cloning)
        self.required = self.config.get('required', False)
        self.parent = None
        self.children = []  # list of ImageMetadata which use this image as a parent.
        self.dependencies: Set[str] = set()
        dependents = self.config.get('dependents', [])
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
        if clone_source:
            runtime.source_resolver.resolve_source(self)

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
        if not short_name.startswith('ose-'):
            short_name = 'ose-' + short_name
        return f"openshift/{short_name}"

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
            message["repos"] = {repo: group_config["repos"][repo] for repo in repos}

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
            self.logger.info("cachi2 enabled from metadata config")
        elif cachi2_group_override not in [Missing, None]:
            # If cachi2 override is defined in group metadata
            cachi2_enabled = cachi2_group_override
            self.logger.info("cachi2 enabled from group config")
        else:
            # Enable cachi2 based on cachito config
            self.logger.info("cachi2 override not found. fallback to use cachito config")
            cachi2_enabled = artlib_util.is_cachito_enabled(
                metadata=self, group_config=self.runtime.group_config, logger=self.logger
            )

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
        lockfile_config_override = self.config.konflux.cachi2.lockfile.enabled
        if lockfile_config_override not in [Missing, None]:
            lockfile_enabled = bool(lockfile_config_override)
            self.logger.info(f"Lockfile generation set from metadata config {lockfile_enabled}")
        else:
            lockfile_group_override = self.runtime.group_config.konflux.cachi2.lockfile.enabled
            if lockfile_group_override not in [Missing, None]:
                lockfile_enabled = bool(lockfile_group_override)
                self.logger.info(f"Lockfile generation set from group config {lockfile_enabled}")

        return lockfile_enabled

    def is_lockfile_force_enabled(self) -> bool:
        """
        Determines whether lockfile force generation is enabled for the current image configuration.

        The method checks configuration in the following order:
        1. Image metadata configuration (`self.config.konflux.cachi2.lockfile.force`)
        2. Group configuration (`self.runtime.group_config.konflux.cachi2.lockfile.force`)

        If neither is set, force generation defaults to disabled.

        Returns:
            bool: True if lockfile force generation is enabled, False otherwise.
        """
        lockfile_force_config_override = self.config.konflux.cachi2.lockfile.force
        if lockfile_force_config_override not in [Missing, None]:
            lockfile_force = bool(lockfile_force_config_override)
            self.logger.info(f"Lockfile force generation set from metadata config: {lockfile_force}")
            return lockfile_force

        lockfile_force_group_override = self.runtime.group_config.konflux.cachi2.lockfile.force
        if lockfile_force_group_override not in [Missing, None]:
            lockfile_force = bool(lockfile_force_group_override)
            self.logger.info(f"Lockfile force generation set from group config: {lockfile_force}")
            return lockfile_force

        return False

    def is_lockfile_parent_inspect_enabled(self) -> bool:
        """
        Determines whether lockfile parent inspection is enabled for the current image configuration.

        The method checks configuration in the following order:
        1. Image metadata configuration (`self.config.konflux.cachi2.lockfile.inspect_parent`)
        2. Group configuration (`self.runtime.group_config.konflux.cachi2.lockfile.inspect_parent`)

        If neither is set, parent inspection defaults to enabled (True).

        Returns:
            bool: True if lockfile parent inspection is enabled, False otherwise.
        """
        lockfile_inspect_parent_config_override = self.config.konflux.cachi2.lockfile.inspect_parent
        if lockfile_inspect_parent_config_override not in [Missing, None]:
            lockfile_inspect_parent = bool(lockfile_inspect_parent_config_override)
            self.logger.info(f"Lockfile parent inspection set from metadata config: {lockfile_inspect_parent}")
            return lockfile_inspect_parent

        lockfile_inspect_parent_group_override = self.runtime.group_config.konflux.cachi2.lockfile.inspect_parent
        if lockfile_inspect_parent_group_override not in [Missing, None]:
            lockfile_inspect_parent = bool(lockfile_inspect_parent_group_override)
            self.logger.info(f"Lockfile parent inspection set from group config: {lockfile_inspect_parent}")
            return lockfile_inspect_parent

        return True

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

            # Check if we should skip parent inspection
            if not self.is_lockfile_parent_inspect_enabled():
                # Skip parent inspection: return full image RPM list
                self.installed_rpms = list(rpms)
                return rpms

            parents = self.get_parent_members()
            if not parents:
                self.logger.warning(f'No parent found for {self.distgit_key}; using full RPM set')
                self.installed_rpms = list(rpms)
                return rpms

            parent_name = next(iter(parents))
            try:
                base_search_params['name'] = parent_name
                parent_build = await self.runtime.konflux_db.get_latest_build(**base_search_params)
                parent_rpms = set(parent_build.installed_rpms or []) if parent_build else set()

                diff_rpms = rpms - parent_rpms
                self.installed_rpms = list(diff_rpms)
                return diff_rpms
            except Exception as e:
                self.logger.error(f"Failed to fetch parent RPMs for {parent_name}/{self.runtime.group}: {e}")
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

    def get_enabled_repos(self) -> set[str]:
        """
        Get enabled repositories for lockfile generation.

        Returns:
            set[str]: Repository names enabled for this image
        """
        return set(self.config.get("enabled_repos", []))
