import time

_mpi_initialized = False

def distributed(file_list, proc=None, of=None):
    """Get a file list for this node (embarrassing parallelization)"""
    global _mpi_initialized

    if type(file_list) == set:
        file_list = list(file_list)

    if proc is None:
        import pypar
        _mpi_initialized = True
        return _mpi_iterate(file_list)

    i = (len(file_list) * (proc - 1)) / of
    j = (len(file_list) * proc) / of - 1
    assert proc <= of and proc > 0
    if proc == of:
        j += 1
    print proc, "processing", i, j, "(inclusive)"
    return file_list[i:j + 1]

def _mpi_assign_thread(job_iterator):
    # Sit idle until request for a job comes in, then assign first
    # available job and move on. Jobs are labelled through the
    # provided iterator
    import pypar
    import pypar.mpiext

    j = -1

    alive = [True for i in xrange(pypar.size())]

    while any(alive[1:]):
        dest = pypar.receive(source=pypar.mpiext.MPI_ANY_SOURCE, tag=1)
        try:
            time.sleep(0.05)
            j = job_iterator.next()[0]
            print "Manager --> Sending job", j, "to rank", dest
        except StopIteration:
            alive[dest] = False
            print "Manager --> Sending out of job message to ", dest
            j = None

        pypar.send(j, destination=dest, tag=2)

    print "Manager --> All jobs done and all processors>0 notified; exiting thread"


def mpi_sync_db(session):
    """Causes the halo_db module to use the rank 0 processor's 'Creator' object"""

    global _mpi_initialized

    if _mpi_initialized:
        import pypar
        import halo_db as db

        if pypar.rank() == 0:
            x = session.merge(db.current_creator)
            session.commit()
            time.sleep(0.5)
            for i in xrange(1, pypar.size()):
                pypar.send(x.id, tag=3, destination=i)

            db.current_creator = x

        else:
            ID = pypar.receive(source=0, tag=3)
            db.current_creator = session.query(
                db.Creator).filter_by(id=ID).first()
            print db.current_creator

    else:
        pass


def _mpi_iterate(task_list):
    """Sets up an iterator returning items of task_list. If this is rank 0 processor, runs
    a separate thread which dishes out tasks to other ranks. If this is >0 processor, relies
    on getting tasks assigned by the rank 0 processor."""
    import pypar
    if pypar.rank() == 0:
        job_iterator = iter(enumerate(task_list))
        #import threading
        #i_thread = threading.Thread(target= lambda : _mpi_assign_thread(job_iterator))
        # i_thread.start()

        # kluge:
        i_thread = None
        _mpi_assign_thread(job_iterator)
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

    while True:

        pypar.send(pypar.rank(), tag=1, destination=0)
        job = pypar.receive(0, tag=2)

        if job is None:
            _mpi_end_embarrass()
            return
        else:
            yield task_list[job]

    _mpi_end_embarrass()




def _mpi_end_embarrass():
    global _mpi_initialized
    if _mpi_initialized:
        import pypar
        print pypar.rank() + 1, " of ", pypar.size(), ": waiting for tasks on other CPUs to complete"
        pypar.barrier()
        pypar.finalize()
        _mpi_initialized = False
    else:
        print "Non-MPI run : Exit without MPI_Finalize"
