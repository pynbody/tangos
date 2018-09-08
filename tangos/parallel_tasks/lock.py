from __future__ import absolute_import
from . import message, log, parallel_backend_loaded
import time
import six
from ..config import DEFAULT_SLEEP_BEFORE_ALLOWING_NEXT_LOCK

class MessageRequestLock(message.Message):
    def __init__(self, name, shared=False):
        self.name = name
        self.shared = shared

    @classmethod
    def deserialize(cls, source, message):
        obj = cls(*message)
        obj.source=source
        return obj

    def serialize(self):
        return (self.name, self.shared)

    def process(self):
        lock_id = self.name
        log.logger.debug("Received request for lock %r for proc %d, shared=%r", lock_id, self.source, self.shared)
        queue = _get_lock_queue(lock_id)
        queue.append((self.source, self.shared))
        if len(queue) == 1:
            _issue_next_lock(lock_id)
        elif _lock_in_shared_mode(lock_id) and self.shared:
            log.logger.debug("Issue shared lock %r to proc %d", lock_id, self.source)
            MessageGrantLock((lock_id, False)).send(self.source)
            _increment_lock_num_shared(lock_id,1)

class MessageRelinquishLock(message.Message):
    def process(self):
        lock_id = self.contents
        proc = self.source
        if _lock_in_shared_mode(lock_id):
            _release_lock_shared(lock_id, proc)
        else:
            _release_lock_exclusive(lock_id, proc)




class MessageGrantLock(message.Message):
    pass


_lock_queues = {}
_lock_num_sharers = {}

def _get_lock_queue(lock_id):
    lock_queue = _lock_queues.get(lock_id,[])
    _lock_queues[lock_id] = lock_queue
    return lock_queue

def _get_lock_num_sharers(lock_id):
    return _lock_num_sharers.get(lock_id, 0)

def _set_lock_num_sharers(lock_id, number):
    _lock_num_sharers[lock_id] = number

def _increment_lock_num_shared(lock_id, increment_by):
    _set_lock_num_sharers(lock_id, _get_lock_num_sharers(lock_id)+increment_by)

def _lock_in_shared_mode(lock_id):
    return _get_lock_num_sharers(lock_id)>0

def _issue_next_lock(lock_id, impose_filesystem_delay=False):
    queue = _get_lock_queue(lock_id)
    if len(queue)>0:
        proc = queue[0][0]
        shared = queue[0][1]
        if shared:
            _issue_shared_locks(lock_id, impose_filesystem_delay)
        else:
            log.logger.debug("Issue lock %r to proc %d", lock_id, proc)
            MessageGrantLock((lock_id, impose_filesystem_delay)).send(proc)

def _issue_shared_locks(lock_id, impose_filesystem_delay=False):
    queue = _get_lock_queue(lock_id)
    sharers_notified = 0
    for proc, shared in queue:
        if shared:
            log.logger.debug("Issue shared lock %r to proc %d",lock_id, proc)
            MessageGrantLock((lock_id, impose_filesystem_delay)).send(proc)
            sharers_notified += 1
    _increment_lock_num_shared(lock_id,sharers_notified)
    log.logger.debug("Lock %r is currently in shared mode, with %d process(es) sharing it",
                     lock_id, _get_lock_num_sharers(lock_id))

def _release_lock_exclusive(lock_id, proc):
    queue = _get_lock_queue(lock_id)
    assert queue[0] == (proc, False)
    queue.pop(0)
    log.logger.debug("Finished with lock %r for proc %d", lock_id, proc)
    if len(queue) > 0:
        _issue_next_lock(lock_id, True)

def _release_lock_shared(lock_id, proc):
    queue = _get_lock_queue(lock_id)
    assert (proc,True) in queue, "Conistency error in locking: can't find a record of the shared lock being released"
    index = queue.index((proc, True))
    del queue[index]
    _increment_lock_num_shared(lock_id, -1)
    log.logger.debug("Finished with shared lock %r for proc %d", lock_id, proc)
    if not _lock_in_shared_mode(lock_id):
        log.logger.debug("Lock %r exiting shared mode", lock_id)
        _issue_next_lock(lock_id)


def _any_locks_alive():
    return any([len(v)>0 for v in six.itervalues(_lock_queues)])


class ExclusiveLock(object):
    """Named, exclusive, re-entrant lock - only one MPI process can hold a lock of a given name at once"""
    _shared=False

    def __init__(self, name, delay_before_release=DEFAULT_SLEEP_BEFORE_ALLOWING_NEXT_LOCK):
        self.name = name
        self._delay = delay_before_release
        self._count = 0

    def acquire(self):
        if not parallel_backend_loaded():
            return
        if self._count==0:
            MessageRequestLock(self.name, self._shared).send(0)
            start = time.time()
            granted = MessageGrantLock.receive(0)
            lock_id, delay = granted.contents
            assert lock_id==self.name, "Received a lock that was not requested. The implementation of ExclusiveLock is not locally thread-safe; are you using multiple threads in one process?"
            if delay:
                time.sleep(self._delay)
            log.logger.debug("Lock %r acquired in %.1fs",self.name, time.time()-start)
        self._count+=1

    def release(self):
        if not parallel_backend_loaded():
            return
        self._count-=1
        if self._count==0:
            MessageRelinquishLock(self.name).send(0)

    def __enter__(self):
        self.acquire()

    def __exit__(self,type, value, traceback):
        self.release()


class SharedLock(ExclusiveLock):
    """Named, shared, re-entrant lock - multiple MPI processes can hold a lock of a given name at once, but not while an
    ExclusiveLock of the same name is also held"""
    _shared=True

