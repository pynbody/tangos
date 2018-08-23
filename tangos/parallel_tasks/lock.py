from __future__ import absolute_import
from . import message, log, parallel_backend_loaded
import time
import six
from ..config import DEFAULT_SLEEP_BEFORE_ALLOWING_NEXT_LOCK

class MessageRequestLock(message.Message):
    def process(self):
        lock_id = self.contents
        proc = self.source

        log.logger.debug("Received request for lock %r for proc %d", lock_id, proc)
        queue = _get_lock_queue(lock_id)
        queue.append(proc)
        if len(queue) == 1:
            _issue_next_lock(lock_id)

class MessageRelinquishLock(message.Message):
    def process(self):
        lock_id = self.contents
        proc = self.source
        queue = _get_lock_queue(lock_id)
        assert queue[0]==proc
        queue.pop(0)
        log.logger.debug("Finished with lock %r for proc %d",lock_id, proc)
        if len(queue)>0:
            _issue_next_lock(lock_id, True)

class MessageGrantLock(message.Message):
    pass


_lock_queues = {}

def _get_lock_queue(lock_id):
    global _lock_queues
    lock_queue = _lock_queues.get(lock_id,[])
    _lock_queues[lock_id] = lock_queue
    return lock_queue

def _issue_next_lock(lock_id, impose_filesystem_delay=False):
    queue = _get_lock_queue(lock_id)
    log.logger.debug("Issue lock %r to proc %d",lock_id, queue[0])
    MessageGrantLock((lock_id, impose_filesystem_delay)).send(queue[0])

def _any_locks_alive():
    return any([len(v)>0 for v in six.itervalues(_lock_queues)])


class RLock(object):
    def __init__(self, name, delay_before_release=DEFAULT_SLEEP_BEFORE_ALLOWING_NEXT_LOCK):
        self.name = name
        self._delay = delay_before_release
        self._count = 0

    def acquire(self):
        if not parallel_backend_loaded():
            return
        if self._count==0:
            MessageRequestLock(self.name).send(0)
            start = time.time()
            granted = MessageGrantLock.receive(0)
            lock_id, delay = granted.contents
            assert lock_id==self.name, "Received a lock that was not requested. The implementation of RLock is not locally thread-safe; are you using multiple threads in one process?"
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