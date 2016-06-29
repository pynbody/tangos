from mpi4py import MPI
import warnings

comm = MPI.COMM_WORLD

def send(data, destination, tag=0):
    comm.send(data, dest=destination, tag = tag)

def receive_any(source=None):
    status = MPI.Status()
    if source is None:
        source = MPI.ANY_SOURCE
    data = comm.recv(source=source, tag=MPI.ANY_TAG, status=status)
    return data, status.source, status.tag

def receive(source=None, tag=0):
    if source is None:
        source = MPI.ANY_SOURCE
    return comm.recv(source=source,tag=tag)


def rank():
    return comm.Get_rank()

def size():
    return comm.Get_size()

def barrier():
    comm.Barrier()

def finalize():
    MPI.Finalize()


def launch(function, num_procs, args):
    if size()==1:
        raise RuntimeError, "MPI run needs minimum of 2 processors (one for manager, one for worker)"
    if num_procs is not None:
        if rank()==0:
            warnings.warn("Number of processors requested (%d) will be ignored as this is an MPI run that has already selected %d processors"%(num_procs,size()))
    function(*args)