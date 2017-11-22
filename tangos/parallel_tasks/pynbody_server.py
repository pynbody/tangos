from __future__ import absolute_import
from .message import Message, ExceptionMessage
from . import log, parallel_backend_loaded, remote_import
from ..util.check_deleted import check_deleted
import sys
import pynbody
import gc
import six.moves.cPickle as pickle
import numpy as np
from six.moves import zip


currently_loaded_snapshot = None

class ConfirmLoadPynbodySnapshot(Message):
    pass


class PynbodySnapshotQueue(object):
    def __init__(self):
        self.load_file_queue = []
        self.load_requester_queue = []
        self.current_snapshot_filename = None
        self.current_snapshot = None
        self.current_subsnap_cache = {}
        self.current_halocat = None
        self.in_use_by = []

    def halos(self):
        if self.current_halocat is None:
            self.current_halocat = self.current_snapshot.halos()
        return self.current_halocat

    def add(self, filename, requester):
        log.logger.debug("Pynbody server: client %d requests access to %r", requester, filename)
        if filename==self.current_snapshot_filename:
            self._notify_available(requester)
            self.in_use_by.append(requester)
        elif filename in self.load_file_queue:
            self.load_requester_queue[self.load_file_queue.index(filename)].append(requester)
        else:
            self.load_file_queue.append(filename)
            self.load_requester_queue.append([requester])
        self._load_next_if_free()

    def free(self, requester):
        self.in_use_by.remove(requester)
        log.logger.debug("Pynbody server: client %d is now finished with %r", requester, self.current_snapshot_filename)
        self._free_if_unused()
        self._load_next_if_free()

    def get_subsnap(self, filter_, fam):
        if (filter_,fam) in self.current_subsnap_cache:
            return self.current_subsnap_cache[(filter_, fam)]
        else:
            subsnap = self.get_subsnap_uncached(filter_, fam)
            self.current_subsnap_cache[(filter_, fam)] = subsnap
            return subsnap

    def get_subsnap_uncached(self, filter_, fam):

        snap = self.current_snapshot

        if isinstance(filter_, pynbody.filt.Filter):
            snap = snap[filter_]
        else:
            snap = self.halos()[filter_]

        if fam is not None:
            snap = snap[fam]

        return snap



    def _free_if_unused(self):
        if len(self.in_use_by)==0:
            log.logger.debug("Pynbody server: all clients are finished with the current snapshot; freeing.")
            with check_deleted(self.current_snapshot):
                self.current_snapshot = None
                self.current_snapshot_filename = None
                self.current_halocat = None
                self.current_subsnap_cache = {}

    def _notify_available(self, node):
        log.logger.debug("Pynbody server: notify %d that snapshot is now available", node)
        ConfirmLoadPynbodySnapshot(type(self.current_snapshot)).send(node)

    def _load_next_if_free(self):
        if len(self.load_file_queue)==0:
            return

        if self.current_snapshot_filename is None:
            # TODO: Error handling
            self.current_snapshot_filename = self.load_file_queue.pop(0)
            self.current_snapshot = pynbody.load(self.current_snapshot_filename)
            self.current_snapshot.physical_units()
            log.logger.info("Pynbody server: loaded %r", self.current_snapshot_filename)

            notify = self.load_requester_queue.pop(0)
            self.in_use_by = notify
            for n in notify:
                self._notify_available(n)
        else:
            log.logger.info("The currently loaded snapshot is still required and so other clients will have to wait")
            log.logger.info("(Currently %d snapshots are in the queue to be loaded later)", len(self.load_file_queue))



_server_queue = PynbodySnapshotQueue()

class RequestLoadPynbodySnapshot(Message):
    def process(self):
        _server_queue.add(self.contents, self.source)

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
    def __init__(self, filter_, array, fam=None):
        self.filter_ = filter_
        self.array = array
        self.fam = fam

    @classmethod
    def deserialize(cls, source, message):
        obj = RequestPynbodyArray(*message)
        obj.source = source
        return obj

    def serialize(self):
        return (self.filter_, self.array, self.fam)

    def process(self):
        with _server_queue.current_snapshot.immediate_mode, _server_queue.current_snapshot.lazy_derive_off:
            try:
                subsnap = _server_queue.get_subsnap(self.filter_, self.fam)
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
        self.filter_ = filter_

    @classmethod
    def deserialize(cls, source, message):
        obj = cls(*message)
        obj.source = source
        return obj

    def serialize(self):
        return (self.filename, self.filter_)

    def process(self):
        assert(_server_queue.current_snapshot_filename==self.filename)
        if isinstance(self.filter_, pynbody.filt.Filter):
            obj = _server_queue.current_snapshot[self.filter_]
        else:
            obj = _server_queue.halos()[self.filter_]
        families = obj.families()
        fam_lengths = [len(obj[fam]) for fam in families]
        fam_lkeys = [obj.loadable_keys(fam) for fam in families]
        lkeys = obj.loadable_keys()
        ReturnPynbodySubsnapInfo(families, fam_lengths, obj.properties, lkeys, fam_lkeys).send(self.source)


class RemoteSubSnap(pynbody.snapshot.SimSnap):
    def __init__(self, connection, filter_):
        super(RemoteSubSnap, self).__init__()

        self.connection = connection
        self._filename = connection.identity
        self._server_id = connection._server_id

        RequestPynbodySubsnapInfo(connection.filename, filter_).send(self._server_id)
        info = ReturnPynbodySubsnapInfo.receive(self._server_id)

        index = 0
        for fam, size in zip(info.families, info.sizes):
            self._family_slice[fam] = slice(index, index+size)
            index+=size
        self._num_particles = index

        self.properties.update(info.properties)
        self._loadable_keys = info.loadable_keys
        self._fam_loadable_keys = {fam: lk for fam, lk in zip(info.families, info.fam_loadable_keys)}
        self.filter_ = filter_

    def _find_deriving_function(self, name):
        cl = self.connection.underlying_pynbody_class
        if cl in self._derived_quantity_registry \
                and name in self._derived_quantity_registry[cl]:
            return self._derived_quantity_registry[cl][name]
        else:
            return super(RemoteSubSnap, self)._find_deriving_function(name)


    def _load_array(self, array_name, fam=None):
        RequestPynbodyArray(self.filter_,array_name,fam).send(self._server_id)
        try:
            data = ReturnPynbodyArray.receive(self._server_id).contents
        except KeyError:
            raise IOError("No such array %r available from the remote"%array_name)
        if fam is None:
            self[array_name] = data
        else:
            self[fam][array_name] = data


_connection_active = False

class RemoteSnapshotConnection(object):
    def __init__(self, fname, server_id=0):
        global _connection_active

        if _connection_active:
            raise RuntimeError("Each client can only have one remote snapshot connection at any time")

        _connection_active = True

        super(RemoteSnapshotConnection, self).__init__()

        self._server_id = 0
        self.filename = fname
        self.identity = "%d: %s"%(self._server_id,fname)

        # ensure server knows what our messages are about
        remote_import.ImportRequestMessage(__name__).send(self._server_id)

        log.logger.debug("Pynbody client: attempt to connect to remote snapshot %r", fname)
        RequestLoadPynbodySnapshot(fname).send(self._server_id)
        self.underlying_pynbody_class = ConfirmLoadPynbodySnapshot.receive(self._server_id).contents
        self.connected = True

        log.logger.info("Pynbody client: connected to remote snapshot %r", fname)

    def get_view(self, filter_):
        return RemoteSubSnap(self, filter_)

    def disconnect(self):
        global _connection_active

        if not self.connected:
            return

        ReleasePynbodySnapshot(self.filename).send(self._server_id)
        _connection_active = False
        self.connected = False

    def __del__(self):
        self.disconnect()






