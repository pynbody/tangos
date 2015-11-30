import time
import warnings
import importlib
import sys
from .. import core
import traceback

backend = None
_backend_name = 'pypar'

if "--backend" in sys.argv:
    index = sys.argv.index("--backend")
    _backend_name = sys.argv[index+1]
    sys.argv.pop(index)
    sys.argv.pop(index)

_lock_queues = {}

MESSAGE_REQUEST_JOB = 1
MESSAGE_DELIVER_JOB = 2
MESSAGE_REQUEST_LOCK = 3
MESSAGE_RELINQUISH_LOCK = 5
MESSAGE_START_ITERATION = 6
MESSAGE_OUT_OF_JOBS = 7
MESSAGE_EXIT = 8
MESSAGE_REQUEST_CREATOR_ID = 9
MESSAGE_DELIVER_CREATOR_ID = 10
MESSAGE_CHECK_LOCK = 11
MESSAGE_LOCK_CLEAR = 12

MESSAGE_GRANT_LOCK_OFFSET = 100


SLEEP_BEFORE_ALLOWING_NEXT_LOCK = 1.0
# number of seconds to sleep after a lock is released before reallocating it


def use(name):
    global backend, _backend_name
    if backend is not None:
        warnings.warn(RuntimeWarning, "Attempt to specify backend but parallelism is already initialised. This call had no effect.")
    else:
        _backend_name = name

def init_backend():
    global backend
    if backend is None:
        backend = importlib.import_module('.backend_'+_backend_name, package='halo_db.parallel_tasks')

def launch(function, num_procs=None, args=[]):
    init_backend()

    if _backend_name!='null':
        backend.launch(_exec_function_or_server, num_procs, [function, args])
    else:
        function(*args)

def distributed(file_list, proc=None, of=None):
    """Distribute a list of tasks between all nodes"""

    if type(file_list) == set:
        file_list = list(file_list)

    if _backend_name=='null':
        if proc is None:
            proc = 1
            of = 1
        i = (len(file_list) * (proc - 1)) / of
        j = (len(file_list) * proc) / of - 1
        assert proc <= of and proc > 0
        if proc == of:
            j += 1
        print proc, "processing", i, j, "(inclusive)"
        return file_list[i:j + 1]
    else:
        return _mpi_iterate(file_list)


class RLock(object):
    def __init__(self, name):
        self.name = name
        self._count = 0

    def acquire(self):
        if _backend_name=='null':
            return
        if self._count==0:
            backend.send(self.name, destination=0, tag=MESSAGE_REQUEST_LOCK)
            start = time.time()
            backend.receive(0,tag=_get_tag_for_lock(self.name))
            print "Lock %r acquired in %.1fs"%(self.name, time.time()-start)
        self._count+=1

    def release(self):
        if _backend_name=='null':
            return
        self._count-=1
        if self._count==0:
            backend.send(self.name, destination=0, tag=MESSAGE_RELINQUISH_LOCK)

    def __enter__(self):
        self.acquire()

    def __exit__(self,type, value, traceback):
        self.release()

def mpi_sync_db(session):
    """Causes the halo_db module to use the rank 0 processor's 'Creator' object"""
    if _backend_name=='null':
        return
    if backend.rank()==0:
        raise RuntimeError, "Cannot call mpi_sync_db from server process"
    if _backend_name!='null':
        backend.send(None, destination=0, tag=MESSAGE_REQUEST_CREATOR_ID)
        id = backend.receive(0,tag=MESSAGE_DELIVER_CREATOR_ID)
        core.current_creator = session.query(core.Creator).filter_by(id=id).first()




def _exec_function_or_server(function, args):
    if backend.rank()==0:
        _server_thread()
    else:
        function(*args)
        backend.send(None, destination=0, tag=MESSAGE_EXIT)
    _shutdown_mpi()

def _server_thread():
    # Sit idle until request for a job comes in, then assign first
    # available job and move on. Jobs are labelled through the
    # provided iterator

    j = -1
    num_jobs = None
    current_job = 0

    alive = [True for i in xrange(backend.size())]

    while any(alive[1:]):
        message, source, tag = backend.receive_any(source=None)
        if tag==MESSAGE_START_ITERATION:
            if num_jobs is None:
                num_jobs = message
            else:
                if num_jobs!=message:
                    raise RuntimeError, "Number of jobs expected by rank %d is inconsistent with %d"%(source, num_jobs)
        elif tag==MESSAGE_REQUEST_JOB:
            if current_job is not None:
                print "Manager --> send job %d to node %d"%(current_job, source)
            else:
                print "Manager --> end of jobs to node %d"%source
            backend.send(current_job, destination=source, tag=MESSAGE_DELIVER_JOB)
            if current_job is not None:
                current_job+=1
                if current_job==num_jobs:
                    num_jobs = None
                    current_job = None
        elif tag==MESSAGE_REQUEST_LOCK:
            _append_to_lock_queue(message, source)
        elif tag==MESSAGE_RELINQUISH_LOCK:
            _remove_from_lock_queue(message, source)
        elif tag==MESSAGE_EXIT:
            alive[source]=False
        elif tag==MESSAGE_REQUEST_CREATOR_ID:
            backend.send(_get_current_creator_id(), destination=source, tag=MESSAGE_DELIVER_CREATOR_ID)


    print "Manager --> All jobs done and all processors>0 notified; exiting thread"

def _get_current_creator_id():
    if core.current_creator.id is None:
        core.current_creator = core.internal_session.merge(core.current_creator)
        core.internal_session.commit()

    return core.current_creator.id



def _get_lock_queue(lock_id):
    global _lock_queues
    lock_queue = _lock_queues.get(lock_id,[])
    _lock_queues[lock_id] = lock_queue
    return lock_queue

def _append_to_lock_queue(lock_id, proc):
    print "Manager --> received request for lock %r for proc %d"%(lock_id, proc)
    queue = _get_lock_queue(lock_id)
    queue.append(proc)
    if len(queue)==1:
        _issue_next_lock(lock_id)

def _remove_from_lock_queue(lock_id, proc):
    queue = _get_lock_queue(lock_id)
    assert queue[0]==proc
    queue.pop(0)
    print "Manager --> finished with lock %r for proc %d"%(lock_id, proc)
    if len(queue)>0:
        time.sleep(SLEEP_BEFORE_ALLOWING_NEXT_LOCK)
        _issue_next_lock(lock_id)

def _issue_next_lock(lock_id):
    queue = _get_lock_queue(lock_id)
    print "Manager --> issue lock %r to proc %d"%(lock_id, queue[0])
    backend.send(lock_id, destination=queue[0], tag=_get_tag_for_lock(lock_id))

def _get_tag_for_lock(lock_id):
    return MESSAGE_GRANT_LOCK_OFFSET + hash(lock_id)&0xFFFF

def _any_locks_alive():
    return any([len(v)>0 for v in _lock_queues.itervalues()])







def _mpi_iterate(task_list):
    """Sets up an iterator returning items of task_list. If this is rank 0 processor, runs
    a separate thread which dishes out tasks to other ranks. If this is >0 processor, relies
    on getting tasks assigned by the rank 0 processor."""
    assert backend is not None, "Parallelism is not initialised"

    backend.send(len(task_list), tag=MESSAGE_START_ITERATION, destination=0)

    while True:
        backend.send(None, tag=MESSAGE_REQUEST_JOB, destination=0)
        job = backend.receive(0, tag=MESSAGE_DELIVER_JOB)

        if job is None:
            return
        else:
            yield task_list[job]


def _shutdown_mpi():
    global backend
    print backend.rank() + 1, " of ", backend.size(), ": waiting for tasks on other CPUs to complete"
    backend.barrier()
    backend.finalize()
    backend = None
    _bankend_name = 'null'

