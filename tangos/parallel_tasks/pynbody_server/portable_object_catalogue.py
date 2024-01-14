import numpy as np

from . import transfer_array
from ..message import Message

class PortableObjectCatalogue(Message):
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

    def get_object(self, obj_number, simulation):
        ptcl_start = np.searchsorted(self.unique_obj_numbers, obj_number)
        if ptcl_start >= len(self.unique_obj_numbers) or self.unique_obj_numbers[ptcl_start] != obj_number:
            raise IndexError("No such object")

        if ptcl_start == len(self.unique_obj_numbers) - 1:
            ptcl_end = len(self.sort_key)
        else:
            ptcl_end = self.boundaries[ptcl_start + 1]

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

