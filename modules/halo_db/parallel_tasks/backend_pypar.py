import pypar, pypar.mpiext
import warnings

def send(data, destination, tag=0):
    pypar.send(data, destination=destination, tag = tag)

def receive_any(source=None):
    if source is None:
        source = pypar.any_source
    data, status = pypar.receive(source=source, return_status=True, tag=pypar.any_tag)
    return data, status.source, status.tag

def receive(source=None, tag=0):
    if source is None:
        source = pypar.mpiext.MPI_ANY_SOURCE
    return pypar.receive(source=source,tag=tag)


def rank():
    return pypar.rank()

def size():
    return pypar.size()

def barrier():
    pypar.barrier()

def finalize():
    pypar.finalize()


def launch(function, num_procs):
    if num_procs is not None:
        if rank()==0:
            warnings.warn("Number of processors requested (%d) will be ignored as this is an MPI run that has already selected %d processors"%(num_procs,size()))
    function()