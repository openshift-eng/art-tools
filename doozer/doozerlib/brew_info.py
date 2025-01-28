
import asyncio
import json
import logging
from typing import (Awaitable, Dict, List, Optional, Tuple, Union,
                    cast)

import doozerlib
from artcommonlib.arch_util import brew_arch_for_go_arch, go_arch_for_brew_arch
from artcommonlib.release_util import isolate_el_version_in_release
from doozerlib import brew, util
from doozerlib.constants import BREWWEB_URL
from doozerlib.repodata import OutdatedRPMFinder, Repodata


class ArchiveImageInspector:
    """
    Represents and returns information about an archive image associated with a brew build.
    """

    def __init__(self, runtime: "doozerlib.Runtime", archive: Dict, brew_build_inspector: 'BrewBuildImageInspector' = None):
        """
        :param runtime: The brew build inspector associated with the build that created this archive.
        :param archive: The raw archive dict from brew.
        :param brew_build_inspector: If the BrewBuildImageInspector is known, pass it in.
        """
        self.runtime = runtime
        self._archive = archive
        self._cache = {}
        self.brew_build_inspector = brew_build_inspector

        if self.brew_build_inspector:
            assert (self.brew_build_inspector.get_brew_build_id() == self.get_brew_build_id())

    def image_arch(self) -> str:
        """
        Returns the CPU architecture (brew build nomenclature) for which this archive was created.
        """
        return self._archive['extra']['image']['arch']

    def get_archive_id(self) -> int:
        """
        :return: Returns this archive's unique brew ID.
        """
        return self._archive['id']

    def get_image_envs(self) -> Dict[str, Optional[str]]:
        """
        :return: Returns a dictionary of environment variables set for this image.
        """
        env_list: List[str] = self._archive['extra']['docker']['config']['config']['Env']
        envs_dict: Dict[str, Optional[str]] = dict()
        for env_entry in env_list:
            if '=' in env_entry:
                components = env_entry.split('=', 1)
                envs_dict[components[0]] = components[1]
            else:
                # Something odd about entry, so include it by don't try to parse
                envs_dict[env_entry] = None
        return envs_dict

    def get_image_labels(self) -> Dict[str, str]:
        """
        :return: Returns a dictionary of labels set for this image.
        """
        return dict(self._archive['extra']['docker']['config']['config']['Labels'])

    def get_archive_dict(self) -> Dict:
        """
        :return: Returns the raw brew archive object associated with this object.
                 listArchives output: https://gist.github.com/jupierce/a28a53e4057b550b3c8e5d6a8ac5198c. This method returns a single entry.
        """
        return self._archive

    def get_brew_build_id(self) -> int:
        """
        :return: Returns the brew build id for the build which created this archive.
        """
        return self._archive['build_id']

    def get_brew_build_inspector(self):
        """
        :return: Returns a brew build inspector for the build which created this archive.
        """
        if not self.brew_build_inspector:
            self.brew_build_inspector = BrewBuildImageInspector(self.runtime, self.get_brew_build_id())
        return self.brew_build_inspector

    def get_image_meta(self):  # -> "ImageMetadata":
        """
        :return: Returns the imagemeta associated with this archive's component if there is one. None if no imagemeta represents it.
        """
        return self.get_brew_build_inspector().get_image_meta()

    async def find_non_latest_rpms(self, rpms_to_check: Optional[List[Dict]] = None) -> List[Tuple[str, str, str]]:
        """
        If the packages installed in this image archive overlap packages in the configured YUM repositories,
        return NVRs of the latest avaiable rpms that are not also installed in this archive.
        This indicates that the image has not picked up the latest from configured repos.

        Note that this is completely normal for non-STREAM assemblies. In fact, it is
        normal for any assembly other than the assembly used for nightlies. In the age of
        a thorough config:scan-sources, if this method returns anything, scan-sources is
        likely broken.

        :param rpms_to_check: If set, narrow the rpms to check
        :return: Returns a list of (installed_rpm, latest_rpm, repo_name)
        """
        meta = self.get_image_meta()
        logger = meta.logger or self.runtime.logger
        arch = self.image_arch()

        # Get enabled repos for the image
        group_repos = self.runtime.repos
        enabled_repos = sorted({r.name for r in group_repos.values() if r.enabled} | set(meta.config.get("enabled_repos", [])))
        if not enabled_repos:  # no enabled repos
            logger.warning("Skipping non-latest rpms check for image %s because it doesn't have enabled_repos configured.", meta.distgit_key)
            return []
        logger.info("Fetching repodatas for enabled repos %s", ", ".join(f"{repo_name}-{arch}" for repo_name in enabled_repos))
        repodatas: List[Repodata] = await asyncio.gather(*(group_repos[repo_name].get_repodata_threadsafe(arch) for repo_name in enabled_repos))

        # Get all installed rpms in the image
        rpms_to_check = rpms_to_check or self.get_installed_rpm_dicts()

        logger.info("Determining outdated rpms...")
        results = OutdatedRPMFinder().find_non_latest_rpms(rpms_to_check, repodatas, logger=cast(logging.Logger, logger))
        return results

    def get_installed_rpm_dicts(self) -> List[Dict]:
        """
        :return: Returns listRPMs for this archive
        (e.g. https://gist.github.com/jupierce/a8798858104dcf6dfa4bd1d6dd99d2d8)
        IMPORTANT: these are different from brew
        build records; use get_installed_build_dicts for those.
        """
        cn = 'get_installed_rpms'
        if cn not in self._cache:
            with self.runtime.pooled_koji_client_session() as koji_api:
                rpm_entries = koji_api.listRPMs(brew.KojiWrapperOpts(caching=True), imageID=self.get_archive_id())
                self._cache[cn] = rpm_entries
        return self._cache[cn]

    def get_installed_package_build_dicts(self) -> Dict[str, Dict]:
        """
        :return: Returns a Dict containing records for package builds corresponding to
                 RPMs used by this archive. A single package can build multiple RPMs.
                 Dict[package_name] -> raw brew build dict for package.
        """
        cn = 'get_installed_package_build_dicts'

        if cn not in self._cache:
            aggregate: Dict[str, Dict] = dict()
            with self.runtime.pooled_koji_client_session() as koji_api:
                # Example results of listing RPMs in an given imageID:
                # https://gist.github.com/jupierce/a8798858104dcf6dfa4bd1d6dd99d2d8
                for rpm_entry in self.get_installed_rpm_dicts():
                    rpm_build_id = rpm_entry['build_id']
                    # Now retrieve records for the package build which created this RPM. Turn caching
                    # on as the archives (and other images being analyzed) likely reference the
                    # exact same builds.
                    package_build = koji_api.getBuild(rpm_build_id, brew.KojiWrapperOpts(caching=True))
                    package_name = package_build['package_name']
                    aggregate[package_name] = package_build

            self._cache[cn] = aggregate

        return self._cache[cn]

    def get_archive_pullspec(self):
        """
        :return: Returns an internal pullspec for a specific archive image.
                 e.g. 'registry-proxy.engineering.redhat.com/rh-osbs/openshift-ose-openshift-controller-manager:rhaos-4.6-rhel-8-containers-candidate-53809-20210722091236-x86_64'
        """
        return self._archive['extra']['docker']['repositories'][0]

    def get_archive_digest(self):
        """
        :return Returns the archive image's sha digest (e.g. 'sha256:1f3ebef02669eca018dbfd2c5a65575a21e4920ebe6a5328029a5000127aaa4b')
        """
        digest = self._archive["extra"]['docker']['digests']['application/vnd.docker.distribution.manifest.v2+json']
        if not digest.startswith("sha256:"):  # It should start with sha256: for now. Let's raise an error if this changes.
            raise ValueError(f"Received unrecognized digest {digest} for archive {self.get_archive_id()}")
        return digest


class BrewBuildImageInspector:
    """
    Provides an API for common queries we perform against brew built images.
    """

    def __init__(self, runtime: "doozerlib.Runtime", build: Union[str, int, Dict]):
        """
        :param runtime: The koji client session to use.
        :param build: A pullspec to the brew image (it is fine if this is a manifest list OR a single archive image), a brew build id, an NVR, or a brew build dict.
        """
        self.runtime = runtime

        with self.runtime.pooled_koji_client_session() as koji_api:
            self._nvr: Optional[str] = None  # Will be resolved to the NVR for the image/manifest list
            self._brew_build_obj: Optional[Dict] = None  # Will be populated with a brew build dict for the NVR

            self._cache = dict()

            if isinstance(build, Dict):
                # Treat as a brew build dict
                self._brew_build_obj = build
                self._nvr = self._brew_build_obj['nvr']
            elif '/' not in str(build):
                # Treat the parameter as an NVR or build_id.
                self._brew_build_obj = koji_api.getBuild(build, strict=True)
                self._nvr = self._brew_build_obj['nvr']
            else:
                # Treat as a full pullspec
                self._build_pullspec = build  # This will be reset to the official brew pullspec, but use it for now
                image_info = self.get_image_info()  # We need to find the brew build, so extract image info
                image_labels = image_info['config']['config']['Labels']
                self._nvr = image_labels['com.redhat.component'] + '-' + image_labels['version'] + '-' + image_labels['release']
                self._brew_build_obj = koji_api.getBuild(self._nvr, strict=True)

            self._build_pullspec = self._brew_build_obj['extra']['image']['index']['pull'][0]
            self._brew_build_id = self._brew_build_obj['id']

    def get_manifest_list_digest(self) -> str:
        """
        :return: Returns  'sha256:....' for the manifest list associated with this brew build.
        """
        return self._brew_build_obj['extra']['image']['index']['digests']['application/vnd.docker.distribution.manifest.list.v2+json']

    def get_brew_build_id(self) -> int:
        """
        :return: Returns the koji build id for this image.
        """
        return self._brew_build_id

    def get_brew_build_webpage_url(self):
        """
        :return: Returns a link for humans to go look at details for this brew build.
        """
        return f'{BREWWEB_URL}/buildinfo?buildID={self._brew_build_id}'

    def get_brew_build_dict(self) -> Dict:
        """
        :return: Returns the koji getBuild dictionary for this iamge.
        """
        return self._brew_build_obj

    def get_nvr(self) -> str:
        return self._nvr

    def __str__(self):
        return f'BrewBuild:{self.get_brew_build_id()}:{self.get_nvr()}'

    def __repr__(self):
        return f'BrewBuild:{self.get_brew_build_id()}:{self.get_nvr()}'

    def get_image_info(self, arch='amd64') -> Dict:
        """
        :return Returns the parsed output of oc image info for the specified arch.
        """
        go_arch = go_arch_for_brew_arch(arch)  # Ensure it is a go arch
        return util.oc_image_info_for_arch__caching(self._build_pullspec, go_arch)

    def get_labels(self, arch='amd64') -> Dict[str, str]:
        """
        :return: Returns a dictionary of labels associated with the image. If the image is a manifest list,
                 these will be the amd64 labels.
        """
        return self.get_image_archive_inspector(arch).get_image_labels()

    def get_envs(self, arch='amd64') -> Dict[str, str]:
        """
        :param arch: The image architecture to check.
        :return: Returns a dictionary of environment variables set for the image.
        """
        return self.get_image_archive_inspector(arch).get_image_envs()

    def get_component_name(self) -> str:
        return self.get_labels()['com.redhat.component']

    def get_package_name(self) -> str:
        return self.get_component_name()

    def get_image_meta(self):  # -> Optional["ImageMetadata"]
        """
        :return: Returns the ImageMetadata object associated with this component. Returns None if the component is not in ocp-build-data.
        """
        return self.runtime.component_map.get(self.get_component_name(), None)

    def get_version(self) -> str:
        """
        :return: Returns the 'version' field of this image's NVR.
        """
        return self._brew_build_obj['version']

    def get_release(self) -> str:
        """
        :return: Returns the 'release' field of this image's NVR.
        """
        return self._brew_build_obj['release']

    def get_rhel_base_version(self) -> Optional[int]:
        """
        Determines whether this image is based on RHEL 8, 9, ... May return None if no
        RPMS are installed (e.g. FROM scratch)
        """
        # OS metadata has changed a bit over time (i.e. there may be newer/cleaner ways
        # to determine this), but one thing that seems backwards compatible
        # is finding 'el' information in RPM list.
        for brew_dict in self.get_all_installed_rpm_dicts():
            nvr = brew_dict['nvr']
            el_ver = isolate_el_version_in_release(nvr)
            if el_ver:
                return el_ver

        raise None

    def get_source_git_url(self) -> Optional[str]:
        """
        :return: Returns SOURCE_GIT_URL from the image environment. This is a URL for the
                public source a customer should be able to find the source of the component.
                If the component does not have a ART-style SOURCE_GIT_URL, None is returned.
        """
        return self.get_envs().get('SOURCE_GIT_URL', None)

    def get_source_git_commit(self) -> Optional[str]:
        """
        :return: Returns SOURCE_GIT_COMMIT from the image environment. This is a URL for the
                public source a customer should be able to find the source of the component.
                If the component does not have a ART-style SOURCE_GIT_COMMIT, None is returned.
        """
        return self.get_envs().get('SOURCE_GIT_COMMIT', None)

    def get_arch_archives(self) -> Dict[str, ArchiveImageInspector]:
        """
        :return: Returns a map of architectures -> brew archive Dict  within this brew build.
        """
        return {a.image_arch(): a for a in self.get_image_archive_inspectors()}

    def get_build_pullspec(self) -> str:
        """
        :return: Returns an internal pullspec for the overall build. Usually this would be a manifest list with architecture specific archives.
                    To get achive pullspecs, use get_archive_pullspec.
        """
        return self._build_pullspec

    def get_all_archive_dicts(self) -> List[Dict]:
        """
        :return: Returns all archives associated with the build. This includes entries for
        things like cachito.
        Example listArchives output: https://gist.github.com/jupierce/a28a53e4057b550b3c8e5d6a8ac5198c
        """
        cn = 'get_all_archives'  # cache entry name
        if cn not in self._cache:
            with self.runtime.pooled_koji_client_session() as koji_api:
                self._cache[cn] = koji_api.listArchives(self._brew_build_id)
        return self._cache[cn]

    def get_image_archive_dicts(self) -> List[Dict]:
        """
        :return: Returns only raw brew archives for images within the build.
        """
        return list(filter(lambda a: a['btype'] == 'image', self.get_all_archive_dicts()))

    def get_image_archive_inspectors(self) -> List[ArchiveImageInspector]:
        """
        Example listArchives output: https://gist.github.com/jupierce/a28a53e4057b550b3c8e5d6a8ac5198c
        :return: Returns only image archives from the build.
        """
        cn = 'get_image_archives'
        if cn not in self._cache:
            image_archive_dicts = self.get_image_archive_dicts()
            inspectors = [ArchiveImageInspector(self.runtime, archive, brew_build_inspector=self) for archive in image_archive_dicts]
            self._cache[cn] = inspectors

        return self._cache[cn]

    def get_rpms_in_pkg_build(self, build_id: int) -> List[Dict]:
        """
        :return: Returns a list of brew RPM records from a single package build.
        """
        with self.runtime.pooled_koji_client_session() as koji_api:
            return koji_api.listRPMs(buildID=build_id)

    def get_all_installed_rpm_dicts(self) -> List[Dict]:
        """
        :return: Returns an aggregate set of all brew rpm definitions
        for RPMs installed on ALL architectures
        of this image build. IMPORTANT: these are different from brew
        build records; use get_installed_build_dicts for those.
        """
        cn = 'get_all_installed_rpm_dicts'
        if cn not in self._cache:
            dedupe: Dict[str, Dict] = dict()  # Maps nvr to rpm definition. This is because most archives will have similar RPMS installed.
            for archive_inspector in self.get_image_archive_inspectors():
                for rpm_dict in archive_inspector.get_installed_rpm_dicts():
                    dedupe[rpm_dict['nvr']] = rpm_dict
            self._cache[cn] = list(dedupe.values())
        return self._cache[cn]

    def get_all_installed_package_build_dicts(self) -> Dict[str, Dict]:
        """
        :return: Returns a Dict[package name -> brew build dict] for all
        packages installed on ANY architecture of this image build.
        (OSBS enforces that all image archives install the same package NVR
        if they install it at all.)
        """
        cn = 'get_all_installed_package_build_dicts'
        if cn not in self._cache:
            dedupe: Dict[str, Dict] = dict()  # Maps nvr to build dict. This is because most archives will have the similar packages installed.
            for archive_inspector in self.get_image_archive_inspectors():
                dedupe.update(archive_inspector.get_installed_package_build_dicts())
            self._cache[cn] = dedupe

        return self._cache[cn]

    def get_image_archive_inspector(self, arch: str) -> Optional[ArchiveImageInspector]:
        """
        Example listArchives output: https://gist.github.com/jupierce/a28a53e4057b550b3c8e5d6a8ac5198c
        :return: Returns the archive inspector for the specified arch    OR    None if the build does not possess one.
        """
        arch = brew_arch_for_go_arch(arch)  # Make sure this is a brew arch
        found = filter(lambda ai: ai.image_arch() == arch, self.get_image_archive_inspectors())
        return next(found, None)

    def is_under_embargo(self) -> bool:
        """
        :return: Returns whether this image build contains currently embargoed content.
        """
        if not self.runtime.group_config.public_upstreams:
            # when public_upstreams are not configured, we assume there is no private content.
            return False

        cn = 'is_private'
        if cn not in self._cache:
            with self.runtime.shared_build_status_detector() as bs_detector:
                # determine if the image build is embargoed (or otherwise "private")
                self._cache[cn] = len(bs_detector.find_embargoed_builds([self._brew_build_obj], [self.get_image_meta().candidate_brew_tag()])) > 0

        return self._cache[cn]

    def list_brew_tags(self) -> List[Dict]:
        """
        :return: Returns a list of tag definitions which are applied on this build.
        """
        cn = 'list_brew_tags'
        if cn not in self._cache:
            with self.runtime.pooled_koji_client_session() as koji_api:
                self._cache[cn] = koji_api.listTags(self._brew_build_id)

        return self._cache[cn]

    def list_brew_tag_names(self) -> List[str]:
        """
        :return: Returns the list of tag names which are applied to this build.
        """
        return [t['name'] for t in self.list_brew_tags()]

    async def find_non_latest_rpms(self, arch_rpms_to_check: Optional[Dict[str, List[Dict]]] = None) -> Dict[str, List[Tuple[str, str, str]]]:
        """
        If the packages installed in this image build overlap packages in the configured YUM repositories,
        return NVRs of the latest avaiable rpms that are not also installed in this image.
        This indicates that the image has not picked up the latest from configured repos.

        Note that this is completely normal for non-STREAM assemblies. In fact, it is
        normal for any assembly other than the assembly used for nightlies. In the age of
        a thorough config:scan-sources, if this method returns anything, scan-sources is
        likely broken.

        :param arch_rpms_to_check: If set, narrow the rpms to check. This should be a dict with keys are arches and values are lists of rpm dicts
        :return: Returns a dict. Keys are arch names; values are lists of (installed_rpm, latest_rpm, repo) tuples
        """
        meta = self.get_image_meta()
        assert meta is not None
        arches = meta.get_arches()
        tasks: List[Awaitable[List[Tuple[str, str, str]]]] = []
        for arch in arches:
            iar = self.get_image_archive_inspector(arch)
            assert iar is not None
            tasks.append(iar.find_non_latest_rpms(arch_rpms_to_check[arch] if arch_rpms_to_check else None))
        result = dict(zip(arches, await asyncio.gather(*tasks)))
        return result
