import pickle

import pynbody.array.shared
import pynbody.halo.details.particle_indices

from ..async_message import AsyncProcessedMessage
from ..message import Message
from . import transfer_array


class PortableCatalogue(pynbody.halo.HaloCatalogue):
    def __init__(self, sim, number_mapper, particle_indices: pynbody.halo.details.particle_indices.HaloParticleIndices):
        super().__init__(sim, number_mapper=number_mapper)
        self._index_lists = particle_indices

    def get_index_list(self, halo_number):
        return self._index_lists.get_particle_index_list_for_halo(self.number_mapper.number_to_index(halo_number))

class ReturnSharedObjectCatalog(Message):
    def __init__(self, halo_catalogue = None, number_mapper=None, indices = None):
        assert halo_catalogue is None or (number_mapper is None and indices is None)
        assert halo_catalogue is not None or (number_mapper is not None and indices is not None)
        if halo_catalogue is not None:
            halo_catalogue.load_all()
            if halo_catalogue.number_mapper is None or halo_catalogue._index_lists is None:
                raise ValueError("Tangos doesn't know how to make a portable catalogue from this halo catalogue")
            index_lists = halo_catalogue._index_lists
            index_lists.particle_index_list = self._as_shared_memory_array(index_lists.particle_index_list)
            index_lists.particle_index_list_boundaries = self._as_shared_memory_array(index_lists.particle_index_list_boundaries)

            self.number_mapper = halo_catalogue.number_mapper
            self._index_lists = halo_catalogue._index_lists
        else:
            self.number_mapper = number_mapper
            self._index_lists = indices

        super().__init__()

    @classmethod
    def _as_shared_memory_array(cls, array):
        if hasattr(array, "_shared_fname"):
            return array
        else:
            shared = pynbody.array.shared.make_shared_array(array.shape, array.dtype, False)
            shared[:] = array
            return shared

    def attach_to_simulation(self, sim):
        return PortableCatalogue(sim, self.number_mapper, self._index_lists)

    @classmethod
    def deserialize(cls, source, message):
        number_mapper = pickle.loads(message)
        index_list = transfer_array.receive_array(source, use_shared_memory=True)
        index_list_boundaries = transfer_array.receive_array(source, use_shared_memory=True)
        indices = pynbody.halo.details.particle_indices.HaloParticleIndices(index_list, index_list_boundaries)
        return cls(number_mapper = number_mapper, indices = indices)

    def serialize(self):
        return pickle.dumps(self.number_mapper)

    def send(self, destination):
        # send envelope
        super().send(destination)

        # send contents
        transfer_array.send_array(self._index_lists.particle_index_list, destination,
                                  use_shared_memory=True)
        transfer_array.send_array(self._index_lists.particle_index_list_boundaries, destination,
                                  use_shared_memory=True)

class RequestSharedObjectCatalogue(AsyncProcessedMessage):
    def __init__(self, object_typetag):
        self.type_tag = object_typetag
        super().__init__()

    def serialize(self):
        return self.type_tag,

    @classmethod
    def deserialize(cls, source, message):
        obj = cls(*message)
        obj.source = source
        return obj

    def process_async(self):
        from . import snapshot_queue
        object_ar = snapshot_queue._server_queue.get_shared_catalogue(self.type_tag)
        ReturnSharedObjectCatalog(halo_catalogue=object_ar).send(self.source)

def get_shared_object_catalogue_from_server(sim, typetag, server_id):
    """Get the server to create and send us a shared object catalogue through the parallel"""
    RequestSharedObjectCatalogue(typetag).send(server_id)
    return ReturnSharedObjectCatalog.receive(server_id).attach_to_simulation(sim)
