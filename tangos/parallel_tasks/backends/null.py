from __future__ import absolute_import

import warnings

def send(data, destination, tag=0):
    raise RuntimeError("Cannot send data to another CPU: parallelism is disabled")

def receive(source=None, tag=0):
    raise RuntimeError("Cannot receive data from another CPU: parallelism is disabled")

def receive_any(source=None):
    raise RuntimeError("Cannot receive data from another CPU: parallelism is disabled")

def rank():
    return 0

def size():
    return 1

def barrier():
    pass

def finalize():
    pass


def launch(function, num_procs, args):
    if num_procs is not None and num_procs!=1:
        warnings.warn("Number of processors requested (%d) will be ignored as this is a single-processor run"%num_procs)
    function(*args)