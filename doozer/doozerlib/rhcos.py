import asyncio
import json
import os
import tempfile
from typing import Dict, List, Optional, Tuple
from urllib import request
from urllib.error import URLError

import koji
from artcommonlib import exectools, logutil, rhcos
from artcommonlib.arch_util import brew_suffix_for_arch, go_arch_for_brew_arch
from artcommonlib.constants import COREOS_RHEL10_STREAMS, RHCOS_RELEASES_BASE_URL, RHCOS_RELEASES_STREAM_URL
from artcommonlib.model import Missing, Model
from artcommonlib.release_util import isolate_el_version_in_release
from artcommonlib.rhcos import get_build_id_from_rhcos_pullspec
from tenacity import retry, stop_after_attempt, wait_fixed

from doozerlib import brew
from doozerlib.repodata import OutdatedRPMFinder, Repodata
from doozerlib.runtime import Runtime

logger = logutil.get_logger(__name__)


class RHCOSNotFound(Exception):
    pass


class RHCOSBuildFinder:
    def __init__(self, runtime, version: str, brew_arch: str = "x86_64", private: bool = False, custom: bool = False):
        """
        @param runtime  The Runtime object passed in from the CLI
        @param version  The 4.y ocp version as a string (e.g. "4.6")
        @param brew_arch  architecture we are interested in (e.g. "s390x")
        @param private  boolean, true for private stream, false for public (currently, no effect)
        @param custom If the caller knows this build is custom, the library will only search in the -custom buckets. When the RHCOS pipeline runs a custom build, artifacts
            should be stored in a different area; e.g. https://releases-rhcos--prod-pipeline.apps.int.prod-stable-spoke1-dc-iad2.itup.redhat.com/storage/releases/rhcos-4.8-custom/48.84.....-0/x86_64/commitmeta.json
            This is done by ART's RHCOS pipeline code when a custom build is indicated: https://gitlab.cee.redhat.com/openshift-art/rhcos-upshift/-/blob/fdad7917ebdd9c8b47d952010e56e511394ed348/Jenkinsfile#L30
        """
        self.runtime = runtime
        self.version = version
        self.brew_arch = brew_arch
        self.private = private
        self.custom = custom
        self.go_arch = go_arch_for_brew_arch(brew_arch)
        self._primary_container = None
        self.layered = self.runtime.group_config.rhcos.get("layered_rhcos", False)
        if self.layered is Missing:
            self.layered = False

    def get_primary_container_conf(self):
        """
        look up the group.yml-configured primary RHCOS container on demand and retain it.
        @return Model with entries for name and build_metadata_key
        """
        if not self._primary_container:
            self._primary_container = rhcos.get_primary_container_conf(self.runtime)
        return self._primary_container

    def rhcos_release_url(self) -> str:
        """
        base url for a release stream in the release browser (AWS bucket).
        @return e.g. "https://releases-rhcos-art...com/storage/releases/rhcos-4.6-s390x"
        """
        # TODO: create private rhcos builds and do something with "private" here
        bucket = self.brew_arch
        if self.custom:
            bucket += '-custom'

        multi_url = self.runtime.group_config.urls.rhcos_release_base["multi"]
        bucket_url = self.runtime.group_config.urls.rhcos_release_base[bucket]
        if multi_url:
            if bucket_url:
                raise ValueError(f"Multiple rhcos_release_base urls found in group config: `multi` and `{bucket}`")
            return multi_url
        if bucket_url:
            return bucket_url

        bucket_suffix = brew_suffix_for_arch(self.brew_arch)
        if self.custom:
            bucket_suffix += '-custom'

        return f"{RHCOS_RELEASES_BASE_URL}/rhcos-{self.version}{bucket_suffix}"

    @retry(reraise=True, stop=stop_after_attempt(10), wait=wait_fixed(3))
    def latest_rhcos_build_id(self) -> Optional[str]:
        """
        :return: Returns the build id for the latest RHCOS build for the specific CPU arch. Return None if not found.
        """
        # this is hard to test with retries, so wrap testable method
        return self._latest_rhcos_build_id()

    def _latest_rhcos_build_id(self) -> Optional[str]:
        # returns the build id string or None (raises RHCOSNotFound for failure to retrieve)
        # (may want to return "schema-version" also if this ever gets more complex)
        url = f"{self.rhcos_release_url()}/builds.json"
        try:
            with request.urlopen(url) as req:
                data = json.loads(req.read().decode())
        except URLError as ex:
            raise RHCOSNotFound(f"Loading RHCOS build at {url} failed: {ex}")

        if not data["builds"]:
            return None

        multi_url = self.runtime.group_config.urls.rhcos_release_base["multi"]
        arches_building = []
        if multi_url:
            arches_building = self.runtime.group_config.arches
        for b in data["builds"]:
            # Make sure all rhcos arch builds are complete
            if multi_url and not self.is_multi_build_complete(b, arches_building):
                continue
            return b["id"]

    def is_multi_build_complete(self, build_dict, arches_building):
        if len(build_dict["arches"]) != len(arches_building):
            missing_arches = set(arches_building) - set(build_dict["arches"])
            logger.info(f"Skipping {build_dict['id']} - missing these arch builds - {missing_arches}")
            return False
        for arch in arches_building:
            if not self.meta_has_required_attributes(self.rhcos_build_meta(build_dict["id"], arch=arch)):
                logger.warning(
                    f"Skipping {build_dict['id']} - {arch} meta.json isn't complete - forget to run rhcos release job?"
                )
                return False
        return True

    def meta_has_required_attributes(self, meta):
        required_keys = [payload_tag.build_metadata_key for payload_tag in self.runtime.group_config.rhcos.payload_tags]
        for metadata_key in required_keys:
            if metadata_key not in meta:
                return False
        return True

    @retry(reraise=True, stop=stop_after_attempt(10), wait=wait_fixed(3))
    def rhcos_build_meta(self, build_id: str, pullspec: str = None, arch: str = None, meta_type: str = "meta") -> Dict:
        """
        Queries the RHCOS release browser to return metadata about the specified RHCOS build.
        :param build_id: The RHCOS build_id to check (e.g. 410.81.20200520.0)
        :param arch: The arch to check - overrides the default self.brew_arch (e.g. ppc64le)
        :param meta_type: The data to retrieve. "commitmeta" (aka OS Metadata - ostree content) or "meta" (aka Build Metadata / Build record).
        :return: Returns a Dict containing the parsed requested metadata. See the RHCOS release browser for examples: https://releases-rhcos--prod-pipeline.apps.int.prod-stable-spoke1-dc-iad2.itup.redhat.com/

        Example 'meta.json':
        https://releases-rhcos--prod-pipeline.apps.int.prod-stable-spoke1-dc-iad2.itup.redhat.com/storage/prod/streams/4.14-9.2/builds/414.92.202305050010-0/x86_64/meta.json
         {
             "buildid": "410.81.20200520.0",
             ...
             "oscontainer": {
                 "digest": "sha256:b0997c9fe4363c8a0ed3b52882b509ade711f7cdb620cc7a71767a859172f423"
                 "image": "quay.io/openshift-release-dev/ocp-v4.0-art-dev"
             },
             ...
         }
        """
        if self.layered and pullspec:
            return self.rhcos_build_meta_layered(pullspec, meta_type)
        else:
            # this is hard to test with retries, so wrap testable method
            return self._rhcos_build_meta(build_id, arch, meta_type)

    def rhcos_build_meta_layered(self, pullspec: str, meta_type: str = "meta") -> Dict:
        """
        This is a helper function to retrieve meta.json from a layered RHCOS build
        """
        if meta_type == "commitmeta":
            with tempfile.TemporaryDirectory() as temp_dir:
                stdout, _ = exectools.cmd_assert(
                    f"oc image extract {pullspec}[-1] --path /usr/share/openshift/base/meta.json:{temp_dir} --confirm"
                )
                with open(os.path.join(temp_dir, "meta.json"), 'r') as f:
                    meta_data = json.load(f)
            return {"rpmostree.rpmdb.pkglist": meta_data["rpmdb.pkglist"]}
        elif meta_type == "meta":
            with tempfile.TemporaryDirectory() as temp_dir:
                stdout, _ = exectools.cmd_assert(
                    f"oc image extract {pullspec}[-1] --path /usr/share/rpm-ostree/extensions.json:{temp_dir} --confirm"
                )
                with open(os.path.join(temp_dir, "extensions.json"), 'r') as f:
                    extensions_data = json.load(f)
            return {"extensions": {"manifest": extensions_data}}

    def _rhcos_build_meta(self, build_id: str, arch: str = None, meta_type: str = "meta") -> Dict:
        """
        See public API rhcos_build_meta for details.
        """
        if not arch:
            arch = self.brew_arch
        url = f"{self.rhcos_release_url()}/{build_id}/{arch}/{meta_type}.json"
        with request.urlopen(url) as req:
            return json.loads(req.read().decode())

    def rhel_build_meta_layered(self, pullspec: str):
        """
        For layered node image, get its rhel image rpm list.

        Uses org.opencontainers.image.version label directly to get the RHEL
        base build_id (e.g. 9.6.20260204-0), rather than
        get_build_id_from_rhcos_pullspec() which returns the OCP ystream
        build_id used for Brew NVR construction (e.g. 4.21.9.6.202602041851-0).

        Arg(s):
            pullspec (str): the rhel image pullspec eg. quay.io/openshift-release-dev/ocp-v4.0-art-dev
        Return Value(s):
            list: rpm list for rhel build
        """
        if self.layered:
            image_info_str, _ = exectools.cmd_assert(f"oc image info -o json {pullspec}", retries=3)
            image_info = Model(json.loads(image_info_str))
            build_id = image_info.config.config.Labels.get("org.opencontainers.image.version")
            if not build_id:
                raise Exception(f"Unable to determine RHEL build_id from: {pullspec}")
            rhel_major = build_id.split(".")[0]
            rhel_minor = build_id.split(".")[1]
            url = f"{RHCOS_RELEASES_STREAM_URL}/rhel-{rhel_major}.{rhel_minor}/builds/{build_id}/{self.brew_arch}/commitmeta.json"
            logger.info(f"Send request to {url}")
            with request.urlopen(url) as req:
                return json.loads(req.read().decode())['rpmostree.rpmdb.pkglist']
        else:
            return []

    def latest_container(self, container_conf: dict = None) -> Tuple[Optional[str], Optional[str]]:
        """
        :param container_conf: a payload tag conf Model from group.yml (with build_metadata_key)
        :return: Returns (rhcos build id, image pullspec) or (None, None) if not found.
        """
        if self.layered:
            build_id, pullspec = rhcos.get_latest_layered_rhcos_build(container_conf, self.brew_arch)
            return build_id, pullspec
        else:
            build_id = self.latest_rhcos_build_id()
            if build_id is None:
                return None, None
            return build_id, rhcos.get_container_pullspec(
                self.rhcos_build_meta(build_id),
                container_conf or self.get_primary_container_conf(),
            )


class RHCOSBuildInspector:
    def __init__(
        self, runtime: Runtime, pullspec_for_tag: Dict[str, str], brew_arch: str, build_id: Optional[str] = None
    ):
        self.runtime = runtime
        self.brew_arch = brew_arch
        self.pullspec_for_tag = pullspec_for_tag
        self.build_id = build_id  # this is used for non-layered rhcos
        self.stream_version = None
        self.layered = self.runtime.group_config.rhcos.get("layered_rhcos", False)
        # Initialize RHEL 10 metadata attributes to None (they may be set conditionally later)
        self._build_meta_10 = None
        self._os_commitmeta_10 = None

        if self.layered:
            # set build_id to the OCP ystream build_id of the rhel-coreos image
            # (e.g. 4.21.9.6.202602041851-0, used for Brew NVR construction)
            self.build_id = get_build_id_from_rhcos_pullspec(pullspec_for_tag["rhel-coreos"])

            finder = RHCOSBuildFinder(runtime, self.stream_version, self.brew_arch)
            self._build_meta = finder.rhcos_build_meta_layered(
                pullspec=pullspec_for_tag.get("rhel-coreos-extensions", None), meta_type='meta'
            )
            self._os_commitmeta = finder.rhcos_build_meta_layered(
                pullspec=pullspec_for_tag.get("rhel-coreos", None), meta_type='commitmeta'
            )
            self.rhel_build_meta = finder.rhel_build_meta_layered(pullspec_for_tag.get("rhel-coreos", None))
            # if there is el10 then add it to metadata
            if pullspec_for_tag.get("rhel-coreos-10", None):
                self._build_meta_10 = finder.rhcos_build_meta_layered(
                    pullspec=pullspec_for_tag.get("rhel-coreos-10-extensions", None), meta_type='meta'
                )
                self._os_commitmeta_10 = finder.rhcos_build_meta_layered(
                    pullspec=pullspec_for_tag.get("rhel-coreos-10", None), meta_type='commitmeta'
                )
                self.rhel_build_meta = self.rhel_build_meta + finder.rhel_build_meta_layered(
                    pullspec_for_tag.get("rhel-coreos-10", None)
                )
        else:
            for tag, pullspec in pullspec_for_tag.items():
                image_build_id = get_build_id_from_rhcos_pullspec(pullspec)
                if self.build_id and self.build_id != image_build_id:
                    raise Exception(
                        f'Found divergent RHCOS build_id for {tag} {pullspec}. {image_build_id} versus {self.build_id}'
                    )
                self.build_id = image_build_id

            # The first digits of the RHCOS build are the major.minor of the rhcos stream name.
            # Which, near branch cut, might not match the actual release stream.
            # Sadly we don't have any other labels or anything to look at to determine the stream.
            version = self.build_id.split('.')[0]
            self.stream_version = version[0] + '.' + version[1:]  # e.g. 43.82.202102081639.0 -> "4.3"

            finder = RHCOSBuildFinder(runtime, self.stream_version, self.brew_arch, custom=True)
            self._build_meta = finder.rhcos_build_meta(self.build_id, meta_type='meta')
            self._os_commitmeta = finder.rhcos_build_meta(self.build_id, meta_type='commitmeta')

    def __repr__(self):
        return f'RHCOSBuild:{self.brew_arch}:{self.build_id}'

    def get_os_metadata(self) -> Dict:
        """
        :return: Returns a dict representing the RHCOS build's OS metadata (aka commitmeta.json)
        """
        return self._os_commitmeta

    def get_build_metadata(self) -> Dict:
        """
        :return: Returns a dict representing the RHCOS build's metadata (aka meta.json)
        """
        return self._build_meta

    def get_os_metadata_rpm_list(self, exclude_rhel: Optional[bool] = False) -> List[List]:
        """
        :return: Returns the raw RPM entries from the OS metadata. Example entry: ['NetworkManager', '1', '1.14.0', '14.el8', 'x86_64', 'rhel-coreos']
        Also include entries from the build meta.json extensions manifest. We don't have epoch for
        these so we just use 0 which may not be correct. So far nothing looks at epoch so it's not a problem.
        """
        entries = []

        # Process _os_commitmeta (rhel-coreos)
        commitmeta_entries = self._os_commitmeta.get('rpmostree.rpmdb.pkglist', [])
        if not commitmeta_entries:
            raise Exception(f"no pkglist in OS Metadata for build {self.build_id}")
        for entry in commitmeta_entries:
            entries.append(entry + ["rhel-coreos"])

        # Process _os_commitmeta_10 (rhel-coreos-10)
        if self._build_meta_10:
            commitmeta_10_entries = self._os_commitmeta_10.get('rpmostree.rpmdb.pkglist', [])
            for entry in commitmeta_10_entries:
                entries.append(entry + ["rhel-coreos-10"])

        def _process_extensions(extensions_manifest, repo_name):
            for name, vra in extensions_manifest.items():
                # e.g. "kernel-rt-core": "4.18.0-372.32.1.rt7.189.el8_6.x86_64"
                # or "qemu-img": "15:6.2.0-11.module+el8.6.0+16538+01ea313d.6.x86_64"
                values = vra.rsplit('-', 1)
                if len(values) != 2:
                    self.runtime.logger.warning("Skipping extension rpm %s with invalid version-release: %s", name, vra)
                    continue
                version, ra = values
                # if epoch is not specified, just use 0. for some reason it's included in the version in
                # RHCOS metadata as "epoch:version"; but if we query brew for it that way, it does not
                # like the format, so we separate it out from the version.
                epoch, version = version.split(':', 1) if ':' in version else ('0', version)
                release, arch = ra.rsplit('.', 1)
                entries.append([name, epoch, version, release, arch, repo_name])

        # Process _build_meta (rhcos-coreos-extensions)
        try:
            extensions = self._build_meta['extensions']['manifest']
            _process_extensions(extensions, "rhcos-coreos-extensions")
        except KeyError:
            pass  # no extensions before 4.8; ignore missing

        # Process _build_meta_10 (rhel-coreos-10-extensions)
        if self._build_meta_10:
            try:
                extensions_10 = self._build_meta_10['extensions']['manifest']
                _process_extensions(extensions_10, "rhel-coreos-10-extensions")
            except KeyError:
                pass

        if exclude_rhel and self.layered:
            # for node image exclude rpms in rhel layer
            filtered_entries = []
            for item in entries:
                if item[:-1] not in self.rhel_build_meta:
                    logger.info(f"RPM {item} exist in node image but not rhel image")
                    filtered_entries.append(item)
            entries = filtered_entries
        return entries

    def get_rpm_nvrs(self) -> List[str]:
        """
        :return: Returns a list of RPM nvrs that are installed in this build according to OS metadata.
                 Note that these are RPMs and not package brew builds. You cannot use koji.getBuild on
                 these NVRs.
        """
        rpm_nvrs: List[str] = list()
        for rpm_entry in self.get_os_metadata_rpm_list():
            if rpm_entry[-1] in COREOS_RHEL10_STREAMS:
                continue
            # Example entry ['NetworkManager', '1', '1.14.0', '14.el8', 'x86_64' ]
            # rpm_entry[1] is epoch.
            rpm_nvrs.append(f'{rpm_entry[0]}-{rpm_entry[2]}-{rpm_entry[3]}')

        return rpm_nvrs

    def get_rpm_nvras(self) -> List[str]:
        """
        :return: Returns a list of nvras that are installed in this build according to OS metadata.
                 Note that these are RPMs and not package brew builds. You cannot use koji.getBuild on
                 these NVRAs.
        """
        rpm_nvras: List[str] = list()
        for rpm_entry in self.get_os_metadata_rpm_list():
            if rpm_entry[-1] in COREOS_RHEL10_STREAMS:
                continue
            # Example entry ['NetworkManager', '1', '1.14.0', '14.el8', 'x86_64' ]
            # rpm_entry[1] is epoch.
            rpm_nvras.append(f'{rpm_entry[0]}-{rpm_entry[2]}-{rpm_entry[3]}.{rpm_entry[4]}')

        return rpm_nvras

    def get_package_build_objects(self) -> Dict[str, Dict]:
        """
        :return: Returns a Dict containing records for package builds corresponding to
                 RPMs used by this rhel9 RHCOS build.
                 Maps package_name -> brew build dict for package.
        """

        aggregate: Dict[str, Dict] = dict()
        rpms = self.get_rpm_nvras()
        with self.runtime.pooled_koji_client_session() as koji_api:
            self.runtime.logger.info("Getting %s rpm(s) from Brew...", len(rpms))
            tasks = []
            with koji_api.multicall(strict=False) as m:  # strict=False means don't raise the first error it encounters
                for nvra in rpms:
                    tasks.append(m.getRPM(nvra, brew.KojiWrapperOpts(caching=True), strict=True))
            rpm_defs = []
            for task in tasks:
                try:
                    rpm_defs.append(task.result)
                except koji.GenericError as e:
                    nvra = task.args[0]
                    if self.runtime.group_config.rhcos.allow_missing_brew_rpms and "No such rpm" in str(e):
                        self.runtime.logger.warning("Failed to find RPM %s in Brew", nvra, exc_info=True)
                        continue  # if conigured, just skip RPMs brew doesn't know about
                    raise Exception(f"Failed to find RPM {nvra} in brew: {e}")

            self.runtime.logger.info("Getting build infos for %s rpm(s)...", len(rpm_defs))
            tasks = []
            with koji_api.multicall(strict=True) as m:
                for rpm_def in rpm_defs:
                    tasks.append(m.getBuild(rpm_def['build_id'], brew.KojiWrapperOpts(caching=True), strict=True))
            for task in tasks:
                package_build = task.result
                package_name = package_build['package_name']
                aggregate[package_name] = package_build
        return aggregate

    def get_primary_container_conf(self):
        """
        look up the group.yml-configured primary RHCOS container.
        @return Model with entries for name and build_metadata_key
        """
        return rhcos.get_primary_container_conf(self.runtime)

    def get_container_configs(self):
        """
        look up the group.yml-configured RHCOS containers and return their configs as a list
        @return list(Model) with entries for name and 15:97build_metadata_key
        """
        return rhcos.get_container_configs(self.runtime)

    def get_container_pullspec(self, container_config: Model = None) -> str:
        """
        Determine the pullspec corresponding to the container config given (the
        primary by default), either as specified at instantiation or from the
        build metadata.

        @param container_config: Model with fields "name" and "build_metadata_key"
        :return: pullspec for the requested container image
        """
        container_config = container_config or self.get_primary_container_conf()
        if container_config.name in self.pullspec_for_tag:
            # per note above... when given a pullspec, prefer that to the build record
            return self.pullspec_for_tag[container_config.name]
        return rhcos.get_container_pullspec(self.get_build_metadata(), container_config)

    def get_container_digest(self, container_config: Model = None) -> str:
        """
        Extract the image digest for (by default) the primary container image
        associated with this build, historically the sha of the
        machine-os-content image published out on quay.

        @param container_config: Model with fields "name" and "build_metadata_key"
        :return: shasum from the pullspec for the requested container image
        """
        return self.get_container_pullspec(container_config).split("@")[1]

    def get_rhel_base_version(self) -> int:
        """
        Determines whether this RHCOS is based on RHEL 8, 9, ...
        """
        # OS metadata has changed a bit over time (i.e. there may be newer/cleaner ways
        # to determine this), but one thing that seems backwards compatible
        # is finding 'el' information in RPM list.
        for nvr in self.get_rpm_nvrs():
            el_ver = isolate_el_version_in_release(nvr)
            if el_ver:
                return el_ver

        raise IOError(f'Unable to determine RHEL version base for rhcos {self.build_id}')

    async def find_non_latest_rpms(self, exclude_rhel: Optional[bool] = False) -> List[Tuple[str, str, str]]:
        """
        If the packages installed in this image overlap packages in the build repo,
        return NVRs of the latest candidate builds that are not also installed in this image.
        This indicates that the image has not picked up the latest from build repo.

        Note that this is completely normal for non-STREAM assemblies. In fact, it is
        normal for any assembly other than the assembly used for nightlies.

        Unfortunately, rhcos builds are not performed in sync with all other builds.
        Thus, it is natural for them to lag behind when RPMs change. The should catch
        up with the next RHCOS build.

        :return: Returns a list of (installed_rpm, latest_rpm, repo_name)
        """
        # Get enabled repos for the image
        enabled_repos = self.runtime.group_config.rhcos.enabled_repos
        exclude_rpms = self.runtime.group_config.rhcos.get("exempt_rpms", [])
        if not enabled_repos:
            raise ValueError("RHCOS build repos need to be defined in group config rhcos.enabled_repos.")
        enabled_repos = enabled_repos.primitive()

        enabled_repos_rhel10 = [repo for repo in enabled_repos if "rhel-10" in repo]
        enabled_repos_rhel9 = [repo for repo in enabled_repos if "rhel-10" not in repo]

        group_repos = self.runtime.repos
        arch = self.brew_arch

        logger.info(
            "Fetching repodatas for enabled repos %s", ", ".join(f"{repo_name}-{arch}" for repo_name in enabled_repos)
        )

        # Fetch all repodatas concurrently
        tasks_rhel9 = [group_repos[repo_name].get_repodata_threadsafe(arch) for repo_name in enabled_repos_rhel9]
        tasks_rhel10 = [group_repos[repo_name].get_repodata_threadsafe(arch) for repo_name in enabled_repos_rhel10]

        all_repodatas = await asyncio.gather(*(tasks_rhel9 + tasks_rhel10))

        repodatas_rhel9 = all_repodatas[: len(tasks_rhel9)]
        repodatas_rhel10 = all_repodatas[len(tasks_rhel9) :]

        # Get all installed rpms
        rpms_to_check_rhel10 = []
        rpms_to_check_rhel9 = []
        for name, epoch, version, release, arch, repo_name in self.get_os_metadata_rpm_list(exclude_rhel):
            rpm_dict = {
                "name": name,
                "epoch": epoch,
                "version": version,
                "release": release,
                "arch": arch,
                "nvr": f"{name}-{version}-{release}",
            }
            if name in exclude_rpms:
                logger.info(
                    f"Exempting rpm {rpm_dict['nvr']} from non-latest check because its in the rhcos exempt list"
                )
                continue
            if repo_name in COREOS_RHEL10_STREAMS:
                rpms_to_check_rhel10.append(rpm_dict)
            else:
                rpms_to_check_rhel9.append(rpm_dict)

        logger.info("Determining outdated rpms for RHEL 9...")
        results = OutdatedRPMFinder().find_non_latest_rpms(rpms_to_check_rhel9, repodatas_rhel9, logger=logger)
        if enabled_repos_rhel10:
            logger.info("Determining outdated rpms for RHEL 10...")
            results.extend(
                OutdatedRPMFinder().find_non_latest_rpms(rpms_to_check_rhel10, repodatas_rhel10, logger=logger)
            )
        return results
