from aioredlock import Lock

from pyartcd import redis, jenkins, constants
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import LockManager
from pyartcd.runtime import Runtime


@cli.command('cleanup-locks')
@pass_runtime
@click_coroutine
async def cleanup_locks(runtime: Runtime):
    lock_manager = LockManager([redis.redis_url()])
    active_locks = await lock_manager.get_locks()

    try:
        for lock_name in active_locks:
            # Lock ID is a build URL minus Jenkins server base URL
            build_path = await lock_manager.get_lock_id(lock_name)
            build_url = f'{constants.JENKINS_UI_URL}/{build_path}'

            try:
                is_build_running = jenkins.is_build_running(build_path)
                runtime.logger.info('Found build %s associated with lock %s', build_url, lock_name)

                if not is_build_running:
                    runtime.logger.warning('Deleting lock %s that was created by %s that\'s not currently running',
                                           lock_name,
                                           build_url.replace(constants.JENKINS_SERVER_URL, constants.JENKINS_UI_URL))
                    lock: Lock = await lock_manager.get_lock(resource=lock_name, lock_identifier=build_path)
                    await lock_manager.unlock(lock)

                else:
                    runtime.logger.info('Build %s is still running: won\'t delete lock %s', build_path, lock_name)

            except ValueError:
                runtime.logger.warning('Could not get build from lock %s with id %s: skipping', lock_name, build_path)

    finally:
        await lock_manager.destroy()
