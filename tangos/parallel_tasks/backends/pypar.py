from __future__ import absolute_import

import pypar, pypar.mpiext
import warnings
import numpy as np
from ..message import Message

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

class NumpyMetadataMessage(Message):
    pass

class NumpyDataMessage(Message):
    pass

def send_numpy_array(data, destination):
    pypar.send((data.shape, data.dtype),destination=destination,tag=NumpyMetadataMessage._tag)
    pypar.send(data,destination=destination,tag=NumpyDataMessage._tag,use_buffer=True,bypass=True)

def receive_numpy_array(source):
    shape,dtype = pypar.receive(source=source, tag=NumpyMetadataMessage._tag)
    ar = np.empty(shape,dtype=dtype)
    pypar.receive(source=source,buffer=ar,tag=NumpyDataMessage._tag)
    return ar



def launch(function, num_procs, args):
    if size()==1:
        raise RuntimeError("MPI run needs minimum of 2 processors (one for manager, one for worker)")
    if num_procs is not None:
        if rank()==0:
            warnings.warn("Number of processors requested (%d) will be ignored as this is an MPI run that has already selected %d processors"%(num_procs,size()))
    function(*args)