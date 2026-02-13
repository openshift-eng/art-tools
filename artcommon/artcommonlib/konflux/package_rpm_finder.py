import logging
from typing import Dict, List

from artcommonlib.konflux.konflux_build_record import KonfluxBuildRecord
from doozerlib import brew


class PackageRpmFinder:
    """
    This class serves a cache, mapping package names as they are listed in a Konflux build record
    to a list of Brew RPMs. During a Konflux scan-sources run, many container builds will have overlapping
    installed_packages, so this will help us limit redundant Brew API calls.
    """

    def __init__(self, runtime):
        self._logger = logging.getLogger(__name__)
        self._package_to_rpms: Dict[str, List[Dict]] = {}
        self._packages_build_dicts: Dict[str, Dict] = {}
        self._not_found_packages = []
        self._runtime = runtime

    def _cache_packages(self, packages: list):
        """
        For each package NVR:
        - call koji.GetBuild(nvr) to get a build info dict
        - some of these will return None (the Brew build could not be found), and can be excluded
        - call koji.listBuildRPMs(build_id) to get a list of RPMs
        - cache the package build <-> RPMs mapping to avoid identical API calls in the future
        """

        caching_packages = [p for p in packages if not self._package_to_rpms.get(p, None)]
        if not caching_packages:
            # All packages have already been mapped to an RPM build list
            return

        # Query Brew to fetch RPMs included in packages not yet cached
        with self._runtime.shared_koji_client_session() as session:
            self._logger.debug("Caching RPM build info for package NVRs %s", ", ".join(caching_packages))

            # Get package build IDs from package names
            with session.multicall(strict=True) as multicall:
                tasks = [multicall.getBuild(package) for package in caching_packages]
            builds = [task.result for task in tasks]

            # Identify builds that could not be found
            not_found_packages = [pkg for pkg, build in zip(caching_packages, builds) if not build]
            if not_found_packages:
                self._logger.warning(
                    "The following packages could not be found in Brew and will be excluded from the check: %s",
                    ", ".join(not_found_packages),
                )

                # Remove missing builds from packages in need for caching
                self._not_found_packages.extend(not_found_packages)
                caching_packages = [pkg for pkg, build in zip(caching_packages, builds) if build]
                builds = [build for build in builds if build]

            # Get RPM list from package build IDs using koji_api.listBuildRPMs
            build_ids = [build["build_id"] for build in builds]
            results: List[List[Dict]] = brew.list_build_rpms(build_ids, session)

            # Cache retrieved RPM builds for package
            for package_nvr, rpm_builds in zip(caching_packages, results):
                self._package_to_rpms[package_nvr] = rpm_builds

            # Cache packages build dicts
            for package_nvr, package_build in zip(caching_packages, builds):
                self._packages_build_dicts[package_nvr] = package_build

    def get_brew_rpms_from_build_record(self, build_record: KonfluxBuildRecord) -> List[Dict]:
        """
        Return a list of RPM build dictionaries associated with the packages installed in a Konflux build
        """

        installed_packages = build_record.installed_packages
        installed_packages = [pkg for pkg in installed_packages if pkg not in self._not_found_packages]
        self._cache_packages(installed_packages)
        return [rpm for package in installed_packages for rpm in self._package_to_rpms.get(package, [])]

    def get_installed_packages_build_dicts(self, build_record: KonfluxBuildRecord) -> Dict[str, Dict]:
        """
        Return a {package-name} -> {list[RPM build]} mapping associated with the packages installed in a Konflux build
        """
        installed_packages = build_record.installed_packages
        installed_packages = [pkg for pkg in installed_packages if pkg not in self._not_found_packages]
        self._cache_packages(installed_packages)
        return {
            self._packages_build_dicts[package_nvr]["name"]: self._packages_build_dicts[package_nvr]
            for package_nvr in installed_packages
            if package_nvr not in self._not_found_packages
        }

    def get_rpms_in_pkg_build(self, nvr: str) -> list:
        return self._package_to_rpms.get(nvr, [])
