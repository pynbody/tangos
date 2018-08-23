from __future__ import absolute_import
from .message import Message, ExceptionMessage
from . import log, parallel_backend_loaded, remote_import
from ..util.check_deleted import check_deleted
import pynbody
import gc
import six.moves.cPickle as pickle
import numpy as np
from six.moves import zip
import time

class ConfirmLoadPynbodySnapshot(Message):
    pass

class ObjectSpecification(object):
    def __init__(self, object_number, object_typetag='halo'):
        self.object_number = object_number
        self.object_typetag = object_typetag

    def __repr__(self):
        return "ObjectSpecification(%d,%r)"%(self.object_number, self.object_typetag)

    def __eq__(self, other):
        if not isinstance(other, ObjectSpecification):
            return False
        return self.object_number==other.object_number and self.object_typetag==other.object_typetag

    def __hash__(self):
        return hash((self.object_number, self.object_typetag))

class PynbodySnapshotQueue(object):
    def __init__(self):
        self.timestep_queue = []
        self.handler_queue = []
        self.load_requester_queue = []
        self.current_timestep = None
        self.current_snapshot = None
        self.current_subsnap_cache = {}
        self.current_handler = None
        self.in_use_by = []

    def add(self, handler, filename, requester):
        log.logger.debug("Pynbody server: client %d requests access to %r", requester, filename)
        if filename==self.current_timestep:
            self._notify_available(requester)
            self.in_use_by.append(requester)
        elif filename in self.timestep_queue:
            queue_position = self.timestep_queue.index(filename)
            self.load_requester_queue[queue_position].append(requester)
            assert self.handler_queue[queue_position] == handler
        else:
            self.timestep_queue.append(filename)
            self.handler_queue.append(handler)
            self.load_requester_queue.append([requester])
        self._load_next_if_free()

    def free(self, requester):
        self.in_use_by.remove(requester)
        log.logger.debug("Pynbody server: client %d is now finished with %r", requester, self.current_timestep)
        self._free_if_unused()
        self._load_next_if_free()

    def get_subsnap(self, filter_or_object_spec, fam):
        if (filter_or_object_spec, fam) in self.current_subsnap_cache:
            log.logger.debug("Pynbody server: cache hit for %r (fam %r)",filter_or_object_spec, fam)
            return self.current_subsnap_cache[(filter_or_object_spec, fam)]
        else:
            log.logger.debug("Pynbody server: cache miss for %r (fam %r)",filter_or_object_spec, fam)
            subsnap = self.get_subsnap_uncached(filter_or_object_spec, fam)
            self.current_subsnap_cache[(filter_or_object_spec, fam)] = subsnap
            return subsnap

    def get_subsnap_uncached(self, filter_or_object_spec, fam):

        snap = self.current_snapshot

        if isinstance(filter_or_object_spec, pynbody.filt.Filter):
            snap = snap[filter_or_object_spec]
        elif isinstance(filter_or_object_spec, ObjectSpecification):
            snap = self.current_handler.load_object(self.current_timestep, filter_or_object_spec.object_number,
                                                    filter_or_object_spec.object_typetag)
        else:
            raise TypeError("filter_or_object_spec must be either a pynbody filter or an ObjectRequestInformation object")

        if fam is not None:
            snap = snap[fam]

        return snap



    def _free_if_unused(self):
        if len(self.in_use_by)==0:
            log.logger.debug("Pynbody server: all clients are finished with the current snapshot; freeing.")
            with check_deleted(self.current_snapshot):
                self.current_snapshot = None
                self.current_timestep = None
                self.current_subsnap_cache = {}
                self.current_handler = None

    def _notify_available(self, node):
        log.logger.debug("Pynbody server: notify %d that snapshot is now available", node)
        ConfirmLoadPynbodySnapshot(type(self.current_snapshot)).send(node)

    def _load_next_if_free(self):
        if len(self.timestep_queue)==0:
            return

        if self.current_handler is None:
            # TODO: Error handling
            self.current_timestep = self.timestep_queue.pop(0)
            self.current_handler = self.handler_queue.pop(0)

            self.current_snapshot = self.current_handler.load_timestep(self.current_timestep)
            self.current_snapshot.physical_units()
            log.logger.info("Pynbody server: loaded %r", self.current_timestep)

            notify = self.load_requester_queue.pop(0)
            self.in_use_by = notify
            for n in notify:
                self._notify_available(n)
        else:
            log.logger.info("The currently loaded snapshot is still required and so other clients will have to wait")
            log.logger.info("(Currently %d snapshots are in the queue to be loaded later)", len(self.timestep_queue))



_server_queue = PynbodySnapshotQueue()

class RequestLoadPynbodySnapshot(Message):
    def process(self):
        _server_queue.add(self.contents[0], self.contents[1], self.source)

class ReleasePynbodySnapshot(Message):
    def process(self):
        _server_queue.free(self.source)

class ReturnPynbodyArray(Message):
    @classmethod
    def deserialize(cls, source, message):
        from . import backend
        contents = backend.receive_numpy_array(source=source)

        if message!="":
            contents = contents.view(pynbody.array.SimArray)
            contents.units = pickle.loads(message)

        obj = ReturnPynbodyArray(contents)
        obj.source = source

        return obj

    def serialize(self):
        assert isinstance(self.contents, np.ndarray)
        if hasattr(self.contents, 'units'):
            serialized_info = pickle.dumps(self.contents.units)
        else:
            serialized_info = ""

        return serialized_info

    def send(self, destination):
        # send envelope
        super(ReturnPynbodyArray, self).send(destination)

        # send contents
        from . import backend
        backend.send_numpy_array(self.contents.view(np.ndarray), destination)

class RequestPynbodyArray(Message):
    def __init__(self, filter_or_object_spec, array, fam=None):
        self.filter_or_object_spec = filter_or_object_spec
        self.array = array
        self.fam = fam

    @classmethod
    def deserialize(cls, source, message):
        obj = RequestPynbodyArray(*message)
        obj.source = source
        return obj

    def serialize(self):
        return (self.filter_or_object_spec, self.array, self.fam)

    def process(self):
        start_time = time.time()
        try:
            log.logger.debug("Receive request for array %r from %d",self.array,self.source)
            subsnap = _server_queue.get_subsnap(self.filter_or_object_spec, self.fam)

            with subsnap.immediate_mode, subsnap.lazy_derive_off:
                if subsnap._array_name_implies_ND_slice(self.array):
                    raise KeyError("Not transferring a single slice %r of a ND array"%self.array)
                if self.array=='remote-index-list':
                    subarray = subsnap.get_index_list(subsnap.ancestor)
                else:
                    subarray = subsnap[self.array]
                    assert isinstance(subarray, pynbody.array.SimArray)            
                array_result = ReturnPynbodyArray(subarray)

        except Exception as e:
            array_result = ExceptionMessage(e)
        
        array_result.send(self.source)
        del array_result
        gc.collect()
        log.logger.debug("Array sent after %.2fs"%(time.time()-start_time))



class ReturnPynbodySubsnapInfo(Message):
    def __init__(self, families, sizes, properties, loadable_keys, fam_loadable_keys):
        super(ReturnPynbodySubsnapInfo, self).__init__()
        self.families = families
        self.sizes = sizes
        self.properties = properties
        self.loadable_keys = loadable_keys
        self.fam_loadable_keys = fam_loadable_keys

    def serialize(self):
        return self.families, self.sizes, self.properties, self.loadable_keys, self.fam_loadable_keys

    @classmethod
    def deserialize(cls, source, message):
        obj = cls(*message)
        obj.source = source
        return obj



class RequestPynbodySubsnapInfo(Message):
    def __init__(self, filename, filter_):
        super(RequestPynbodySubsnapInfo, self).__init__()
        self.filename = filename
        self.filter_or_object_spec = filter_

    @classmethod
    def deserialize(cls, source, message):
        obj = cls(*message)
        obj.source = source
        return obj

    def serialize(self):
        return (self.filename, self.filter_or_object_spec)

    def process(self):
        start_time = time.time()
        assert(_server_queue.current_timestep == self.filename)
        log.logger.debug("Received request for subsnap info, spec %r", self.filter_or_object_spec)
        obj = _server_queue.get_subsnap(self.filter_or_object_spec, None)
        families = obj.families()
        fam_lengths = [len(obj[fam]) for fam in families]
        fam_lkeys = [obj.loadable_keys(fam) for fam in families]
        lkeys = obj.loadable_keys()
        ReturnPynbodySubsnapInfo(families, fam_lengths, obj.properties, lkeys, fam_lkeys).send(self.source)
        log.logger.debug("Subsnap info sent after %.2f",(time.time()-start_time))

class RemoteSubSnap(pynbody.snapshot.SimSnap):
    def __init__(self, connection, filter_or_object_spec):
        super(RemoteSubSnap, self).__init__()

        self.connection = connection
        self._filename = connection.identity
        self._server_id = connection._server_id

        RequestPynbodySubsnapInfo(connection.filename, filter_or_object_spec).send(self._server_id)
        info = ReturnPynbodySubsnapInfo.receive(self._server_id)

        index = 0
        for fam, size in zip(info.families, info.sizes):
            self._family_slice[fam] = slice(index, index+size)
            index+=size
        self._num_particles = index

        self.properties.update(info.properties)
        self._loadable_keys = info.loadable_keys
        self._fam_loadable_keys = {fam: lk for fam, lk in zip(info.families, info.fam_loadable_keys)}
        self._filter_or_object_spec = filter_or_object_spec

    def _find_deriving_function(self, name):
        cl = self.connection.underlying_pynbody_class
        if cl in self._derived_quantity_registry \
                and name in self._derived_quantity_registry[cl]:
            return self._derived_quantity_registry[cl][name]
        else:
            return super(RemoteSubSnap, self)._find_deriving_function(name)


    def _load_array(self, array_name, fam=None):
        RequestPynbodyArray(self._filter_or_object_spec, array_name, fam).send(self._server_id)
        try:
            start_time=time.time()
            log.logger.debug("Send array request")
            data = ReturnPynbodyArray.receive(self._server_id).contents
            log.logger.debug("Array received; waited %.2fs",time.time()-start_time)
        except KeyError:
            raise IOError("No such array %r available from the remote"%array_name)
        if fam is None:
            self[array_name] = data
        else:
            self[fam][array_name] = data


_connection_active = False

class RemoteSnapshotConnection(object):
    def __init__(self, input_handler, ts_extension, server_id=0):
        global _connection_active

        from ..input_handlers import pynbody
        assert isinstance(input_handler, pynbody.PynbodyInputHandler)

        if _connection_active:
            raise RuntimeError("Each client can only have one remote snapshot connection at any time")

        _connection_active = True

        super(RemoteSnapshotConnection, self).__init__()

        self._server_id = server_id
        self._input_handler = input_handler
        self.filename = ts_extension
        self.identity = "%d: %s"%(self._server_id, ts_extension)

        # ensure server knows what our messages are about
        remote_import.ImportRequestMessage(__name__).send(self._server_id)

        log.logger.debug("Pynbody client: attempt to connect to remote snapshot %r", ts_extension)
        RequestLoadPynbodySnapshot((input_handler, ts_extension)).send(self._server_id)
        self.underlying_pynbody_class = ConfirmLoadPynbodySnapshot.receive(self._server_id).contents
        self.connected = True

        log.logger.info("Pynbody client: connected to remote snapshot %r", ts_extension)

    def get_view(self, filter_or_object_spec):
        """Return a RemoteSubSnap that contains either the pynbody filtered region, or the specified object from a catalogue

        filter_or_object_spec is either an instance of pynbody.filt.Filter, or a tuple containing
        (typetag, number), which are respectively the object type tag and object number to be loaded
        """
        return RemoteSubSnap(self, filter_or_object_spec)

    def disconnect(self):
        global _connection_active

        if not self.connected:
            return

        ReleasePynbodySnapshot(self.filename).send(self._server_id)
        _connection_active = False
        self.connected = False

    def __del__(self):
        self.disconnect()






