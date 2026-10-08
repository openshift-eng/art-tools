import os

import click
from aioredlock import Lock
from artcommonlib import redis

from pyartcd import constants, jenkins
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import LockManager
from pyartcd.runtime import Runtime
from pyartcd.tekton_locks import is_pipelinerun_active, parse_k8s_lock_path


@cli.command('cleanup-locks')
@click.option('--dry-run/--no-dry-run', default=False, help='Preview locks without deleting')
@pass_runtime
@click_coroutine
async def cleanup_locks(runtime: Runtime, dry_run: bool):
    if dry_run:
        runtime.logger.info("DRY RUN MODE: Will print locks to delete without actually deleting")

    lock_manager = LockManager([redis.redis_url()])
    active_locks = await lock_manager.get_locks()
    runtime.logger.info(f"Found {len(active_locks)} locks in Redis")

    kubeconfig_path = os.getenv('ARTC_CLEANUP_LOCKS_KUBECONFIG')

    removed_locks = []
    jenkins_url = jenkins.get_jenkins_url()

    try:
        for lock_name in active_locks:
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

            # Kubernetes/Tekton PipelineRun locks - query cluster directly for this specific resource
            if build_path.startswith('k8s/'):
                if kubeconfig_path:
                    # Parse lock path to extract namespace and resource name
                    parsed = parse_k8s_lock_path(build_path)
                    if parsed:
                        namespace, resource_name = parsed
                        try:
                            is_active = is_pipelinerun_active(kubeconfig_path, namespace, resource_name)
                        except Exception as e:
                            # Query error - skip this lock to be safe
                            runtime.logger.warning(
                                'Skipping k8s lock %s - error querying PipelineRun %s/%s: %s',
                                lock_name,
                                namespace,
                                resource_name,
                                e,
                            )
                            continue

                        if is_active is True:
                            # PipelineRun exists and is Running/Pending - protect this lock
                            runtime.logger.info(
                                'Skipping lock %s - PipelineRun %s/%s is Running/Pending',
                                lock_name,
                                namespace,
                                resource_name,
                            )
                            continue
                        elif is_active is False:
                            # PipelineRun exists but is completed - safe to delete
                            if dry_run:
                                runtime.logger.warning(
                                    '[DRY RUN] WOULD DELETE orphaned k8s lock: %s (PipelineRun %s/%s is completed)',
                                    lock_name,
                                    namespace,
                                    resource_name,
                                )
                                removed_locks.append(lock_name)
                            else:
                                runtime.logger.warning(
                                    'DELETING orphaned k8s lock: %s (PipelineRun %s/%s is completed)',
                                    lock_name,
                                    namespace,
                                    resource_name,
                                )
                                lock: Lock = await lock_manager.get_lock(resource=lock_name, lock_identifier=build_path)
                                await lock_manager.unlock(lock)
                                removed_locks.append(lock_name)
                            continue
                        else:
                            # is_active is None - PipelineRun doesn't exist, safe to delete
                            if dry_run:
                                runtime.logger.warning(
                                    '[DRY RUN] WOULD DELETE orphaned k8s lock: %s (PipelineRun %s/%s not found)',
                                    lock_name,
                                    namespace,
                                    resource_name,
                                )
                                removed_locks.append(lock_name)
                            else:
                                runtime.logger.warning(
                                    'DELETING orphaned k8s lock: %s (PipelineRun %s/%s not found)',
                                    lock_name,
                                    namespace,
                                    resource_name,
                                )
                                lock: Lock = await lock_manager.get_lock(resource=lock_name, lock_identifier=build_path)
                                await lock_manager.unlock(lock)
                                removed_locks.append(lock_name)
                            continue
                    else:
                        # Could not parse lock path - skip to be safe
                        runtime.logger.warning('Skipping k8s lock %s - could not parse lock path', lock_name)
                        continue
                else:
                    # No kubeconfig - skip k8s locks to be safe
                    runtime.logger.info('Skipping k8s lock %s - kubeconfig unavailable for cluster query', lock_name)
                    continue

            build_url = f'{constants.JENKINS_UI_URL}/{build_path}'
            runtime.logger.info("Found build_url for lock %s: %s ", lock_name, build_url)

            try:
                is_build_running = jenkins.is_build_running(build_path)
                runtime.logger.info('Found build %s associated with lock %s', build_url, lock_name)

                if not is_build_running:
                    if dry_run:
                        runtime.logger.warning(
                            '[DRY RUN] WOULD DELETE orphaned job lock: %s (Jenkins build %s is not running)',
                            lock_name,
                            build_url.replace(jenkins_url, constants.JENKINS_UI_URL),
                        )
                        removed_locks.append(lock_name)
                    else:
                        runtime.logger.warning(
                            'DELETING orphaned job lock: %s (Jenkins build %s is not running)',
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
                    if dry_run:
                        runtime.logger.warning(
                            '[DRY RUN] WOULD DELETE orphaned job lock: %s (build %s not found in Jenkins)',
                            lock_name,
                            build_path,
                        )
                    else:
                        runtime.logger.warning(
                            'DELETING orphaned job lock: %s (build %s not found in Jenkins)', lock_name, build_path
                        )
                        lock: Lock = await lock_manager.get_lock(resource=lock_name, lock_identifier=build_path)
                        await lock_manager.unlock(lock)
                else:
                    runtime.logger.info("api isn't reachable :(")

    finally:
        await lock_manager.destroy()

    # Display removed locks in the build title (skip in dry-run mode)
    if removed_locks and not dry_run and os.getenv('JOB_NAME') and (jenkins.get_build_path() or '').startswith('job/'):
        jenkins.init_jenkins()
        jenkins.update_title(f' [{", ".join(removed_locks)}]')
