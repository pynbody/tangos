import numpy as np
import pynbody

from ..message import Message


def send_array(array: pynbody.array.SimArray, destination: int, use_shared_memory: bool = False):
    if use_shared_memory:
        if not hasattr(array, "_shared_fname"):
            raise ValueError("Array %r has no shared memory information" % array)
        _send_array_shared_memory(array, destination)
    else:
        _send_array_copy(array, destination)

def receive_array(source: int, use_shared_memory: bool = False):
    if use_shared_memory:
        return _receive_array_shared_memory(source)
    else:
        return _receive_array_copy(source)

def _send_array_copy(array: np.ndarray, destination: int):
    from .. import backend
    backend.send_numpy_array(array, destination)

def _receive_array_copy(source):
    from .. import backend
    return backend.receive_numpy_array(source)


class SharedMemoryArrayInfo(Message):
    pass

def _send_array_shared_memory(array: pynbody.array.SimArray, destination: int):
    info = pynbody.array._shared_array_deconstruct(array, transfer_ownership=False)
    SharedMemoryArrayInfo(info).send(destination)

def _receive_array_shared_memory(source):
    info = SharedMemoryArrayInfo.receive(source)
    return pynbody.array._shared_array_reconstruct(info.contents)
