import os

from aioredlock import Lock
from artcommonlib import redis

from pyartcd import constants, jenkins
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import LockManager
from pyartcd.runtime import Runtime


@cli.command("cleanup-locks")
@pass_runtime
@click_coroutine
async def cleanup_locks(runtime: Runtime):
    lock_manager = LockManager([redis.redis_url()])
    active_locks = await lock_manager.get_locks()
    runtime.logger.info("Found %s active locks", len(active_locks))

    removed_locks = []
    jenkins_url = jenkins.get_jenkins_url()

    try:
        for lock_name in active_locks:
            # Lock ID is a build URL minus Jenkins server base URL
            build_path = await lock_manager.get_lock_id(lock_name)
            build_url = f"{constants.JENKINS_UI_URL}/{build_path}"
            runtime.logger.info("Found build_url for lock %s: %s ", lock_name, build_url)

            try:
                is_build_running = jenkins.is_build_running(build_path)
                runtime.logger.info("Found build %s associated with lock %s", build_url, lock_name)

                if not is_build_running:
                    runtime.logger.warning(
                        "Deleting lock %s that was created by %s that's not currently running",
                        lock_name,
                        build_url.replace(jenkins_url, constants.JENKINS_UI_URL),
                    )
                    lock: Lock = await lock_manager.get_lock(resource=lock_name, lock_identifier=build_path)
                    await lock_manager.unlock(lock)
                    removed_locks.append(lock_name)

                else:
                    runtime.logger.info("Build %s is still running: won't delete lock %s", build_path, lock_name)

            except ValueError:
                runtime.logger.info("could not see if build is running.. checking if api is reachable")
                if jenkins.is_api_reachable():
                    # Make sure Jenkins API are responding
                    # Assume the build is not found because it was manually deleted, and clean up the orphan lock
                    runtime.logger.warning(
                        "Could not get build from lock %s with id %s: deleting", lock_name, build_path
                    )
                    lock: Lock = await lock_manager.get_lock(resource=lock_name, lock_identifier=build_path)
                    await lock_manager.unlock(lock)
                else:
                    runtime.logger.info("api isn't reachable :(")

    finally:
        await lock_manager.destroy()

    # Display removed locks in the build title
    jenkins.init_jenkins()
    if os.getenv("BUILD_URL") and os.getenv("JOB_NAME"):
        if removed_locks:
            jenkins.update_title(f" [{', '.join(removed_locks)}]")
