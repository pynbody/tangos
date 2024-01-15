import os
import random
import time

import numpy as np
import pynbody.array.shared

from ..async_message import AsyncProcessedMessage
from ..message import Message
from . import transfer_array


class SharedObjectCatalogue(Message):
    def __init__(self,  /, object_id_per_particle=None, sort_key=None, unique_obj_numbers=None, boundaries=None):
        super().__init__(None)

        if object_id_per_particle is not None:
            assert sort_key is None
            assert unique_obj_numbers is None
            assert boundaries is None
            self.sort_key = np.argsort(object_id_per_particle, kind='mergesort')  # mergesort for stability
            self.unique_obj_numbers = np.unique(object_id_per_particle)
            self.boundaries = np.searchsorted(object_id_per_particle[self.sort_key], self.unique_obj_numbers)
        else:
            assert sort_key is not None
            assert unique_obj_numbers is not None
            assert boundaries is not None
            self.sort_key = sort_key
            self.unique_obj_numbers = unique_obj_numbers
            self.boundaries = boundaries

        self._ensure_shared_memory()

    @classmethod
    def _make_name(cls):
        random.seed(os.getpid() * time.time())
        fname = "tangos-objcat-" + \
                ("".join([random.choice('abcdefghijklmnopqrstuvwxyz')
                          for i in range(10)]))
        return fname

    @classmethod
    def _as_shared_memory_array(cls, array):
        if hasattr(array, "_shared_fname"):
            return array
        else:
            shared = pynbody.array.shared.make_shared_array(array.size, array.dtype, False, cls._make_name())
            shared[:] = array
            return shared

    def _ensure_shared_memory(self):
        self.sort_key = self._as_shared_memory_array(self.sort_key)
        self.unique_obj_numbers = self._as_shared_memory_array(self.unique_obj_numbers)
        self.boundaries = self._as_shared_memory_array(self.boundaries)

    def get_index_list(self, obj_number):
        obj_offset = np.searchsorted(self.unique_obj_numbers, obj_number)
        if obj_offset >= len(self.unique_obj_numbers) or self.unique_obj_numbers[obj_offset] != obj_number:
            raise IndexError("No such object")

        ptcl_start = self.boundaries[obj_offset]

        if obj_offset == len(self.unique_obj_numbers) - 1:
            ptcl_end = len(self.sort_key)
        else:
            ptcl_end = self.boundaries[obj_offset + 1]

        return self.sort_key[ptcl_start:ptcl_end]

    def get_object(self, obj_number, simulation):
        obj_offset = np.searchsorted(self.unique_obj_numbers, obj_number)
        if obj_offset >= len(self.unique_obj_numbers) or self.unique_obj_numbers[obj_offset] != obj_number:
            raise IndexError("No such object")

        ptcl_start = self.boundaries[obj_offset]

        if obj_offset == len(self.unique_obj_numbers) - 1:
            ptcl_end = len(self.sort_key)
        else:
            ptcl_end = self.boundaries[obj_offset + 1]

        return simulation[self.sort_key[ptcl_start:ptcl_end]]

    @classmethod
    def deserialize(cls, source, message):
        sort_key = transfer_array.receive_array(source, use_shared_memory=True)
        unique_obj_numbers = transfer_array.receive_array(source, use_shared_memory=True)
        boundaries = transfer_array.receive_array(source, use_shared_memory=True)

        return cls(sort_key=sort_key, unique_obj_numbers=unique_obj_numbers, boundaries=boundaries)

    def serialize(self):
        return ""

    def send(self, destination):
        # send envelope
        super().send(destination)

        # send contents
        transfer_array.send_array(self.sort_key, destination, use_shared_memory=True)
        transfer_array.send_array(self.unique_obj_numbers, destination, use_shared_memory=True)
        transfer_array.send_array(self.boundaries, destination, use_shared_memory=True)


def make_shared_object_catalogue_from_pynbody_halos(halocat: pynbody.halo.HaloCatalogue):
    grp_array = halocat.get_group_array()
    # TODO: this could be considerably optimized for some halo catalogue types where intrinsically the info
    # is stored as grp_num->id_array rather than an array of grp_nums. Furthermore, get_group_array
    # is potentailly 'lossy' in that some particles may belong to multiple groups.

    return SharedObjectCatalogue(grp_array)

class RequestSharedObjectCatalogueResponse(Message):
    """This simply indicates whether a shared object catalogue is available or not, and the actual
    catalogue is sent separately"""
    pass

class RequestSharedObjectCatalogue(AsyncProcessedMessage):
    def __init__(self, filename, object_typetag):
        self.filename = filename
        self.type_tag = object_typetag

    def serialize(self):
        return self.filename, self.type_tag

    @classmethod
    def deserialize(cls, source, message):
        obj = cls(*message)
        obj.source = source
        return obj

    def process_async(self):
        from . import snapshot_queue
        assert self.filename == snapshot_queue._server_queue.current_timestep
        object_ar = snapshot_queue._server_queue.get_shared_catalogue(self.type_tag)
        if object_ar is None:
            RequestSharedObjectCatalogueResponse(False).send(self.source)
        else:
            RequestSharedObjectCatalogueResponse(True).send(self.source)
            object_ar.send(self.source)

def get_shared_object_catalogue_from_server(filename, typetag, server_id):
    """Get the server to create and send us a shared object catalogue through the parallel"""
    RequestSharedObjectCatalogue(filename, typetag).send(server_id)
    available = RequestSharedObjectCatalogueResponse.receive(server_id).contents
    if available:
        return SharedObjectCatalogue.receive(server_id)
    else:
        return None
