import os
import re
import urllib.error
import urllib.request

from aioredlock import Lock
from artcommonlib import redis

from pyartcd import constants, jenkins
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import LockManager
from pyartcd.runtime import Runtime


def get_jenkins_enabled_groups(runtime):
    """Fetch all Jenkins-enabled groups from commonlib.groovy.

    Parses ocp3Versions, ocp4Versions, ocp5Versions, okd4Versions, and
    nonOCPGroups from the aos-cd-jobs repository.
    """
    url = "https://raw.githubusercontent.com/openshift-eng/aos-cd-jobs/master/pipeline-scripts/commonlib.groovy"

    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            content = response.read().decode('utf-8')

        groups = set()

        # Parse nonOCPGroups = [ ... ] (anchor to line start to avoid commented-out declarations)
        match = re.search(r'^\s*nonOCPGroups\s*=\s*\[(.*?)\]', content, re.MULTILINE | re.DOTALL)
        if match:
            groups_str = match.group(1)
            groups.update(re.findall(r'"([^"]+)"', groups_str))

        # Parse ocp3Versions, ocp4Versions, ocp5Versions and prefix with "openshift-" and "okd-"
        for version_var in ['ocp3Versions', 'ocp4Versions', 'ocp5Versions']:
            match = re.search(rf'^\s*{version_var}\s*=\s*\[(.*?)\]', content, re.MULTILINE | re.DOTALL)
            if match:
                versions_str = match.group(1)
                versions = re.findall(r'"([^"]+)"', versions_str)
                groups.update(f"openshift-{v}" for v in versions)
                # OKD uses the same versions as OCP (see okdVersionParam in commonlib)
                groups.update(f"okd-{v}" for v in versions)

        if not groups:
            raise ValueError("Could not parse any groups from commonlib.groovy")

        runtime.logger.info("Fetched %d Jenkins-enabled groups from commonlib.groovy", len(groups))
        return groups

    except (urllib.error.URLError, urllib.error.HTTPError, Exception) as e:
        raise RuntimeError(f"Failed to fetch commonlib.groovy from GitHub: {e}") from e


@cli.command('cleanup-locks')
@pass_runtime
@click_coroutine
async def cleanup_locks(runtime: Runtime):
    lock_manager = LockManager([redis.redis_url()])
    active_locks = await lock_manager.get_locks()
    runtime.logger.info("Found %s active locks", len(active_locks))

    # Fetch Jenkins-enabled groups to skip Tekton-only locks
    jenkins_enabled_groups = get_jenkins_enabled_groups(runtime)

    removed_locks = []
    jenkins_url = jenkins.get_jenkins_url()

    try:
        for lock_name in active_locks:
            # Extract group from lock name based on format:
            # - lock:build:{version} -> openshift-{version}
            # - lock:compose:{assembly}:{group} -> {group} (last segment)
            # - lock:layered-products-scan:{group} -> {group}
            # Skip if group is not Jenkins-enabled (likely Tekton-only now)
            group = None
            parts = lock_name.split(':')

            if len(parts) >= 3:
                resource = parts[1]
                if resource == 'build':
                    # lock:build:{version} -> openshift-{version}
                    version = parts[2]
                    group = f"openshift-{version}"
                elif len(parts) >= 4 and resource == 'compose':
                    # lock:compose:{assembly}:{group} -> {group}
                    group = parts[3]
                elif resource in ['layered-products-scan', 'layered-products-build']:
                    # lock:layered-products-scan:{group} -> {group}
                    group = parts[2]

            # Only skip if we successfully derived a group and it's not Jenkins-enabled
            if group and group not in jenkins_enabled_groups:
                runtime.logger.info(
                    "Skipping lock %s for group %s (not Jenkins-enabled, likely Tekton-only)", lock_name, group
                )
                continue

            # Lock ID is a build URL minus Jenkins server base URL
            build_path = await lock_manager.get_lock_id(lock_name)

            # Skip locks with None or empty build_path
            # These locks cannot be validated against Jenkins and cannot be safely deleted
            if not build_path:
                runtime.logger.warning(
                    "Skipping lock %s with None or empty build path (cannot validate or delete)", lock_name
                )
                continue

            # Skip locks with random identifiers (created outside of Jenkins)
            # These locks cannot be validated against Jenkins builds and should not be cleaned up
            if build_path.startswith('random-'):
                runtime.logger.info(
                    "Skipping lock %s with random identifier %s (not a Jenkins build)", lock_name, build_path
                )
                continue

            build_url = f'{constants.JENKINS_UI_URL}/{build_path}'
            runtime.logger.info("Found build_url for lock %s: %s ", lock_name, build_url)

            try:
                is_build_running = jenkins.is_build_running(build_path)
                runtime.logger.info('Found build %s associated with lock %s', build_url, lock_name)

                if not is_build_running:
                    runtime.logger.warning(
                        'Deleting lock %s that was created by %s that\'s not currently running',
                        lock_name,
                        build_url.replace(jenkins_url, constants.JENKINS_UI_URL),
                    )
                    lock: Lock = await lock_manager.get_lock(resource=lock_name, lock_identifier=build_path)
                    await lock_manager.unlock(lock)
                    removed_locks.append(lock_name)

                else:
                    runtime.logger.info('Build %s is still running: won\'t delete lock %s', build_path, lock_name)

            except ValueError:
                runtime.logger.info('could not see if build is running.. checking if api is reachable')
                if jenkins.is_api_reachable():
                    # Make sure Jenkins API are responding
                    # Assume the build is not found because it was manually deleted, and clean up the orphan lock
                    runtime.logger.warning(
                        'Could not get build from lock %s with id %s: deleting', lock_name, build_path
                    )
                    lock: Lock = await lock_manager.get_lock(resource=lock_name, lock_identifier=build_path)
                    await lock_manager.unlock(lock)
                else:
                    runtime.logger.info("api isn't reachable :(")

    finally:
        await lock_manager.destroy()

    # Display removed locks in the build title
    jenkins.init_jenkins()
    if os.getenv('BUILD_URL') and os.getenv('JOB_NAME'):
        if removed_locks:
            jenkins.update_title(f' [{", ".join(removed_locks)}]')
