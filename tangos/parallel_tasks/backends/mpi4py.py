from __future__ import absolute_import

from mpi4py import MPI
import warnings

import numpy as np

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


def _get_dtype_code(numpy_dtype):
    return numpy_dtype.char

def send_numpy_array(data, destination):
    comm.send((data.shape, data.dtype), dest=destination, tag=1)
    # made necessary by strange bug with hdf arrays, similar to: https://groups.google.com/forum/#!topic/mpi4py/8gOVvT4ObvU
    # when sending without an explicit dtype code, sometimes get a KeyError
    dtype_code=_get_dtype_code(data.dtype)
    comm.Send([data, dtype_code], dest=destination, tag=2)

def receive_numpy_array(source):
    shape,dtype = comm.recv(source=source, tag=1)
    ar = np.empty(shape,dtype=dtype)
    comm.Recv(ar,source=source,tag=2)
    return ar

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
        raise RuntimeError("MPI run needs minimum of 2 processors (one for manager, one for worker)")
    if num_procs is not None:
        if rank()==0:
            warnings.warn("Number of processors requested (%d) will be ignored as this is an MPI run that has already selected %d processors"%(num_procs,size()))
    function(*args)