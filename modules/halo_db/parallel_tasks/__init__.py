import time
import warnings
import importlib
backend = None
_backend_name = 'pypar'

_lock_queues = {}

MESSAGE_REQUEST_JOB = 1
MESSAGE_DELIVER_JOB = 2
MESSAGE_REQUEST_LOCK = 3
MESSAGE_RELINQUISH_LOCK = 5

MESSAGE_GRANT_LOCK_OFFSET = 100


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
    backend.launch(function, num_procs, args)

def distributed(file_list, proc=None, of=None):
    """Get a file list for this node (embarrassing parallelization)"""

    if type(file_list) == set:
        file_list = list(file_list)

    if _backend_name=='null':
        i = (len(file_list) * (proc - 1)) / of
        j = (len(file_list) * proc) / of - 1
        assert proc <= of and proc > 0
        if proc == of:
            j += 1
        print proc, "processing", i, j, "(inclusive)"
        return file_list[i:j + 1]
    else:
        init_backend()
        return _mpi_iterate(file_list)

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
        _issue_next_lock(lock_id)

def _issue_next_lock(lock_id):
    queue = _get_lock_queue(lock_id)
    print "Manager --> issue lock %r to proc %d"%(lock_id, queue[0])
    backend.send(lock_id, destination=queue[0], tag=_get_tag_for_lock(lock_id))

def _get_tag_for_lock(lock_id):
    return MESSAGE_GRANT_LOCK_OFFSET + hash(lock_id)&0xFFFFFF

def _any_locks_alive():
    return any([len(v)>0 for v in _lock_queues.itervalues()])




def _mpi_assign_thread(job_iterator):
    # Sit idle until request for a job comes in, then assign first
    # available job and move on. Jobs are labelled through the
    # provided iterator

    j = -1

    alive = [True for i in xrange(backend.size())]

    while any(alive[1:]) or _any_locks_alive():
        message, source, tag = backend.receive_any(source=None)
        print "recv message with tag",tag
        if tag==MESSAGE_REQUEST_JOB:
            try:
                time.sleep(0.05)
                j = job_iterator.next()[0]
                print "Manager --> Sending job", j, "to rank", source
            except StopIteration:
                alive[source] = False
                print "Manager --> Sending out of job message to ", source
                j = None

            backend.send(j, destination=source, tag=MESSAGE_DELIVER_JOB)
        elif tag==MESSAGE_REQUEST_LOCK:
            _append_to_lock_queue(message, source)
        elif tag==MESSAGE_RELINQUISH_LOCK:
            _remove_from_lock_queue(message, source)

    print "Manager --> All jobs done and all processors>0 notified; exiting thread"

class RLock(object):
    def __init__(self, name):
        self.name = name
        self._count = 0

    def acquire(self):
        if self._count==0:
            backend.send(self.name, destination=0, tag=MESSAGE_REQUEST_LOCK)
            backend.receive(0,tag=_get_tag_for_lock(self.name))
        self._count+=1

    def release(self):
        self._count-=1
        if self._count==0:
            backend.send(self.name, destination=0, tag=MESSAGE_RELINQUISH_LOCK)

    def __enter__(self):
        self.acquire()

    def __exit__(self,type, value, traceback):
        self.release()

def mpi_sync_db(session):
    """Causes the halo_db module to use the rank 0 processor's 'Creator' object"""

    if _backend_name!='null':

        import halo_db as db

        if backend.rank() == 0:
            x = session.merge(db.core.current_creator)
            session.commit()
            time.sleep(0.5)
            for i in xrange(1, backend.size()):
                backend.send(x.id, tag=3, destination=i)

            db.core.current_creator = x

        else:
            ID = backend.receive(source=0, tag=3)
            db.core.current_creator = session.query(
                db.Creator).filter_by(id=ID).first()
            print db.core.current_creator

    else:
        pass


def _mpi_iterate(task_list):
    """Sets up an iterator returning items of task_list. If this is rank 0 processor, runs
    a separate thread which dishes out tasks to other ranks. If this is >0 processor, relies
    on getting tasks assigned by the rank 0 processor."""

    if backend.rank() == 0:
        job_iterator = iter(enumerate(task_list))
        #import threading
        #i_thread = threading.Thread(target= lambda : _mpi_assign_thread(job_iterator))
        # i_thread.start()

        # kluge:
        i_thread = None
        _mpi_assign_thread(job_iterator)

        """
        while True:
            try:
                job = job_iterator.next()[0]
                print "Manager --> Doing job", job, "of", len(task_list), "myself"
                yield task_list[job]
            except StopIteration:
                print "Manager --> Out of jobs message to myself"
                if i_thread is not None:
                    i_thread.join()
                _mpi_end_embarrass()
                return
        """
    else:
        while True:

            backend.send(backend.rank(), tag=1, destination=0)
            job = backend.receive(0, tag=2)

            if job is None:
                _mpi_end_embarrass()
                return
            else:
                yield task_list[job]

    _mpi_end_embarrass()




def _mpi_end_embarrass():
    global backend
    print backend.rank() + 1, " of ", backend.size(), ": waiting for tasks on other CPUs to complete"
    backend.barrier()
    backend.finalize()
    backend = None
    _bankend_name = 'null'

