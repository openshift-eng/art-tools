from multiprocessing import Semaphore
from pathlib import Path

# See get_named_semaphore. The empty string key serves as a lock for the data structure.
_NAMED_SEMAPHORES = {"": Semaphore(1)}


def get_named_semaphore(lock_name: str, is_dir=False, count=1):
    """
    Returns a semaphore (which can be used as a context manager). The first time a lock_name
    is received, a new semaphore will be established. Subsequent uses of that lock_name will
    receive the same semaphore.
    :param lock_name: A unique name for resource threads are contending over. If using a directory name
                        as a lock_name, provide an absolute path.
    :param is_dir: The lock_name is a directory (method will ignore things like trailing slashes)
    :param count: The number of times the lock can be claimed. Default=1, which is a full mutex.
    :return: A semaphore associated with the lock_name.
    """
    if is_dir:
        p = "_dir::" + str(Path(str(lock_name)).absolute())  # normalize (e.g. strip trailing /)
    else:
        p = lock_name
    with _NAMED_SEMAPHORES[""]:
        if p in _NAMED_SEMAPHORES:
            return _NAMED_SEMAPHORES[p]
        else:
            new_semaphore = Semaphore(count)
            _NAMED_SEMAPHORES[p] = new_semaphore
            return new_semaphore
