import pynbody.array.shared
import pynbody.halo
from pynbody.array.shared import (
    SharedArrayReference,
    from_shared_reference,
    to_shared_reference,
)
from pynbody.halo.portable import map_arrays

from ..async_message import AsyncProcessedMessage
from ..message import Message


class ReturnSharedObjectCatalog(Message):
    """Transmits a halo catalogue to other processes, carrying its contents in shared memory.

    The catalogue is described by its portable state (see
    :meth:`pynbody.halo.HaloCatalogue.get_portable_state`), a structure of numpy arrays and python
    primitives. Every array in it is placed in shared memory and replaced by a reference, so the message
    itself stays small and neither end needs to know what any individual array is for.
    """

    @classmethod
    def from_halo_catalogue(cls, halo_catalogue: pynbody.halo.HaloCatalogue):
        """Create a message describing *halo_catalogue*, with its arrays copied into shared memory.

        The returned message keeps those arrays alive, so it must be retained for as long as any
        recipient may still be using the catalogue."""
        # Two passes: the first gets the arrays into shared memory, which the second requires before it
        # can reduce them to picklable references.
        shared_state = map_arrays(halo_catalogue.get_portable_state(), _as_shared_memory_array)
        message = cls(map_arrays(shared_state, to_shared_reference))
        message._shared_state = shared_state
        return message

    def attach_to_simulation(self, sim) -> pynbody.halo.HaloCatalogue:
        """Recreate the catalogue described by this message, attached to the specified simulation"""
        state = map_arrays(self.contents, from_shared_reference, types=SharedArrayReference)
        return pynbody.halo.HaloCatalogue.from_portable_state(state, sim)


def _as_shared_memory_array(array):
    """Return *array* if it is already backed by shared memory, otherwise a copy of it that is"""
    if hasattr(array, "_shared_fname"):
        return array
    shared = pynbody.array.shared.make_shared_array(array.shape, array.dtype, False)
    shared[...] = array
    return shared


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
        snapshot_queue._server_queue.get_shared_catalogue(self.type_tag).send(self.source)

def get_shared_object_catalogue_from_server(sim, typetag, server_id):
    """Get the server to create and send us a shared object catalogue through the parallel"""
    RequestSharedObjectCatalogue(typetag).send(server_id)
    return ReturnSharedObjectCatalog.receive(server_id).attach_to_simulation(sim)
