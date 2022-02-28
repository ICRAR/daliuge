import time
import errno
import threading
import weakref

__all__ = ["lock_file"]


try:
    import fcntl
except ImportError:
    try:
        import msvcrt
    except ImportError:
        raise ImportError("Platform not supported (failed to import fcntl, msvcrt)")
    else:
        _lock_file_blocking_available = False

        def _lock_file_non_blocking(file_):
            try:
                msvcrt.locking(file_.fileno(), msvcrt.LK_NBLCK, 1)
                return True
            # TODO: check errno
            except IOError:
                return False

        def _unlock_file(file_):
            msvcrt.locking(file_.fileno(), msvcrt.LK_UNLCK, 1)

else:
    _lock_file_blocking_available = True
    def _lock_file_blocking(file_):
        fcntl.flock(file_.fileno(), fcntl.LOCK_EX)

    def _lock_file_non_blocking(file_):
        try:
            fcntl.flock(file_.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except IOError as error:
            if error.errno in [errno.EACCES, errno.EAGAIN]:
                return False
            else:
                raise

    def _unlock_file(file_):
        fcntl.flock(file_.fileno(), fcntl.LOCK_UN)


_locks_lock = threading.Lock()
_locks = weakref.WeakValueDictionary()


def lock_file(path, **kwargs):
    _locks_lock.acquire()
    try:
        lock = _locks.get(path)
        if lock is None:
            lock = _create_lock_file(path)
            _locks[path] = lock
    finally:
        _locks_lock.release()
    return _Locker(lock, **kwargs)
    
    
def _create_lock_file(path):
    thread_lock = _ThreadLock(path)
    file_lock = _LockFile(path)
    return _LockSet([thread_lock, file_lock])


class LockError(Exception):
    pass


def _acquire_non_blocking(acquire, timeout, retry_period, path):
    if retry_period is None:
        retry_period = 0.05
    
    start_time = time.time()
    while True:
        success = acquire()
        if success:
            return
        elif (timeout is not None and
                time.time() - start_time > timeout):
            raise LockError("Couldn't lock {0}".format(path))
        else:
            time.sleep(retry_period)


class _LockSet(object):
    def __init__(self, locks):
        self._locks = locks
    
    def acquire(self, timeout, retry_period):
        acquired_locks = []
        try:
            for lock in self._locks:
                lock.acquire(timeout, retry_period)
                acquired_locks.append(lock)
        except:
            for acquired_lock in reversed(acquired_locks):
                # TODO: handle exceptions
                acquired_lock.release()
            raise
    
    def release(self):
        for lock in reversed(self._locks):
            # TODO: Handle exceptions
            lock.release()


class _ThreadLock(object):
    def __init__(self, path):
        self._path = path
        self._lock = threading.Lock()
    
    def acquire(self, timeout=None, retry_period=None):
        if timeout is None:
            self._lock.acquire()
        else:
            _acquire_non_blocking(
                acquire=lambda: self._lock.acquire(False),
                timeout=timeout,
                retry_period=retry_period,
                path=self._path,
            )
    
    def release(self):
        self._lock.release()


class _LockFile(object):
    def __init__(self, path):
        self._path = path
        self._file = None

    def acquire(self, timeout=None, retry_period=None):
        if self._file is None:
            self._file = open(self._path, "wb")
        if timeout is None and _lock_file_blocking_available:
            _lock_file_blocking(self._file)
        else:
            _acquire_non_blocking(
                acquire=lambda: _lock_file_non_blocking(self._file),
                timeout=timeout,
                retry_period=retry_period,
                path=self._path,
            )
    
    def release(self):
        _unlock_file(self._file)
        self._file.close()
        self._file = None


class _Locker(object):
    """
    A lock wrapper to always apply the given *timeout* and *retry_period*
    to acquire() calls.
    """
    def __init__(self, lock, timeout=None, retry_period=None):
        self._lock = lock
        self._timeout = timeout
        self._retry_period = retry_period

    def acquire(self):
        self._lock.acquire(self._timeout, self._retry_period)

    def release(self):
        self._lock.release()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *args):
        self.release()
