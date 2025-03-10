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

    def __init__(self):
        self._logger = logging.getLogger(__name__)
        self._package_to_rpms: Dict[str, List[Dict]] = {}

    def get_brew_rpms_from_build_record(self, build_record: KonfluxBuildRecord, runtime) -> List[Dict]:
        """
        For each package NVR listed in a KonfluxBuildRecord installed_packages:
        - call koji.GetBuild(nvr) to get a build info dict
        - some of these will return None (the Brew build could not be found), and can be excluded
        - call koji.listBuildRPMs(build_id) to get a list of RPMs
        - cache the package build <-> RPMs mapping to avoid identical API calls in the future
        """
        installed_packages = build_record.installed_packages

        # Query Brew to fetch RPMs included in packages not yet cached
        caching_packages = [p for p in installed_packages if not self._package_to_rpms.get(p, None)]
        if caching_packages:
            with runtime.shared_koji_client_session() as session:
                self._logger.debug('Caching RPM build info for package NVRs %s', ', '.join(caching_packages))

                # Get package build IDs from package names
                with session.multicall(strict=True) as multicall:
                    tasks = [multicall.getBuild(package) for package in caching_packages]
                builds = [task.result for task in tasks]

                # Identify builds that could not be found
                not_found_packages = [pkg for pkg, build in zip(caching_packages, builds) if not build]
                if not_found_packages:
                    self._logger.warning(
                        'The following packages could not be found in Brew and will be excluded from the check: %s',
                        ', '.join(not_found_packages)
                    )

                # Remove missing builds from packages in need for caching
                caching_packages = [pkg for pkg, build in zip(caching_packages, builds) if build]
                builds = [build for build in builds if build]

                # Remove missing builds from installed_packages
                installed_packages = [pkg for pkg in installed_packages if pkg not in not_found_packages]

                # Get RPM list from package build IDs using koji_api.listBuildRPMs
                build_ids = [build['build_id'] for build in builds]
                results: List[List[Dict]] = brew.list_build_rpms(build_ids, session)

                # Cache retrieved RPM builds for package
                for package_build, rpm_builds in zip(caching_packages, results):
                    self._package_to_rpms[package_build] = rpm_builds

        # Gather RPMs associated with the given KonfluxBuildRecord
        return [rpm for package in installed_packages for rpm in self._package_to_rpms[package]]
