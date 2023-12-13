import gc
import pickle
import time

import numpy as np
import pynbody
import pynbody.snapshot.copy_on_access

import tangos.parallel_tasks.pynbody_server.snapshot_queue

from .. import log, remote_import
from ..message import ExceptionMessage, Message
from . import snapshot_queue, transfer_array
from .snapshot_queue import (ConfirmLoadPynbodySnapshot,
                             ReleasePynbodySnapshot,
                             RequestLoadPynbodySnapshot, _server_queue)


class ReturnPynbodyArray(Message):

    def __init__(self, contents, shared_mem = False):
        self.shared_mem = shared_mem
        super().__init__(contents)

    @classmethod
    def deserialize(cls, source, message):
        units, shared_mem = pickle.loads(message)

        contents = transfer_array.receive_array(source, use_shared_memory=shared_mem)

        if units is not None:
            if not isinstance(contents, pynbody.array.SimArray):
                contents = contents.view(pynbody.array.SimArray)
            contents.units = units

        obj = ReturnPynbodyArray(contents, shared_mem=shared_mem)
        obj.source = source

        return obj

    def serialize(self):
        assert isinstance(self.contents, np.ndarray)
        if hasattr(self.contents, 'units'):
            units = self.contents.units
        else:
            units = None

        serialized_info = pickle.dumps((units, self.shared_mem))
        return serialized_info

    def send(self, destination):
        # send envelope
        super().send(destination)

        # send contents
        transfer_array.send_array(self.contents, destination, use_shared_memory=self.shared_mem)

class RequestPynbodyArray(Message):
    _time_to_start_processing = []

    def __init__(self, filter_or_object_spec, array, fam=None, request_sent_time=None):
        self.filter_or_object_spec = filter_or_object_spec
        self.array = array
        self.fam = fam
        self.request_sent_time = request_sent_time

    @classmethod
    def deserialize(cls, source, message):
        obj = RequestPynbodyArray(*message)
        obj.source = source
        return obj

    def serialize(self):
        return (self.filter_or_object_spec, self.array, self.fam, time.time())

    @classmethod
    def get_mean_wait_time(cls):
        if len(cls._time_to_start_processing)==0:
            return 0
        else:
            return np.mean(cls._time_to_start_processing)

    @classmethod
    def get_total_wait_time(cls):
        if len(cls._time_to_start_processing)==0:
            return 0
        else:
            return np.sum(cls._time_to_start_processing)

    @classmethod
    def get_std_wait_time(cls):
        if len(cls._time_to_start_processing)==0:
            return 0
        else:
            return np.std(cls._time_to_start_processing)

    @classmethod
    def get_num_requests(cls):
        return len(cls._time_to_start_processing)

    @classmethod
    def reset_performance_stats(cls):
        cls._time_to_start_processing = []

    def process(self):
        start_time = time.time()
        self._time_to_start_processing.append(start_time - self.request_sent_time)

        try:
            log.logger.debug("Receive request for array %r from %d",self.array,self.source)
            subsnap = _server_queue.get_subsnap(self.filter_or_object_spec, self.fam)
            transfer_via_shared_mem = _server_queue.current_shared_mem_flag

            with subsnap.immediate_mode, subsnap.lazy_derive_off:
                if subsnap._array_name_implies_ND_slice(self.array):
                    raise KeyError("Not transferring a single slice %r of a ND array"%self.array)
                if self.array=='remote-index-list':
                    subarray = subsnap.get_index_list(subsnap.ancestor).view(pynbody.array.SimArray)
                    # this won't be actually in shared memory – it's a regular numpy array
                    transfer_via_shared_mem = False
                else:
                    subarray = subsnap[self.array]
                    assert isinstance(subarray, pynbody.array.SimArray)
                array_result = ReturnPynbodyArray(subarray, transfer_via_shared_mem)

        except Exception as e:
            array_result = ExceptionMessage(e)

        array_result.send(self.source)
        del array_result
        gc.collect()
        log.logger.debug("Array sent after %.2fs"%(time.time()-start_time))



class ReturnPynbodySubsnapInfo(Message):
    def __init__(self, families, sizes, properties, loadable_keys, fam_loadable_keys):
        super().__init__()
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
        super().__init__()
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
        if self.filter_or_object_spec is not None:
            log.logger.debug("Received request for subsnap info, spec %r", self.filter_or_object_spec)
        else:
            log.logger.debug("Received request for snapshot info")
        obj = _server_queue.get_subsnap(self.filter_or_object_spec, None)
        families = obj.families()
        fam_lengths = [len(obj[fam]) for fam in families]
        fam_lkeys = [obj.loadable_keys(fam) for fam in families]
        lkeys = obj.loadable_keys()
        ReturnPynbodySubsnapInfo(families, fam_lengths, obj.properties, lkeys, fam_lkeys).send(self.source)
        log.logger.debug("Info sent after %.2f",(time.time()-start_time))


class RemoteSnap(pynbody.snapshot.copy_on_access.UnderlyingClassMixin, pynbody.snapshot.SimSnap):
    def __init__(self, connection, filter_or_object_spec):
        """Create a remote snapshot object

        filter_or_object_spec can be:
        - a pynbody filter
        - a tuple (typetag, number) specifying an object to be loaded
        - None to load the whole snapshot (only sensible in shared memory mode)
        """
        super().__init__(connection.underlying_pynbody_class)
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




    def _load_array(self, array_name, fam=None):
        RequestPynbodyArray(self._filter_or_object_spec, array_name, fam).send(self._server_id)
        try:
            start_time=time.time()
            log.logger.debug("Send array request")
            data = ReturnPynbodyArray.receive(self._server_id).contents
            log.logger.debug("Array received; waited %.2fs",time.time()-start_time)
        except KeyError:
            raise OSError("No such array %r available from the remote"%array_name)
        with self.auto_propagate_off:
            if len(data.shape)==1:
                ndim = 1
            elif len(data.shape)==2:
                ndim = data.shape[-1]
            else:
                assert False, "Don't know how to handle this data shape"

            if fam is None:
                self._create_array(array_name, ndim=ndim, source_array=data)
            else:
                self._create_family_array(array_name, fam, ndim=ndim, source_array=data)

    def _promote_family_array(self, name, *args, **kwargs):
        # special logic: the normal promotion procedure would copy everything for this array out of shared memory
        # which we don't want if the server can provide us with a shared memory view of the whole array

        if self.connection.shared_mem and not self.delay_promotion:
            if name in self._loadable_keys:
                for fam in self.families():
                    try:
                        del self[fam][name]
                    except KeyError:
                        pass
                self._load_array(name)
                return

        super()._promote_family_array(name, *args, **kwargs)






class RemoteSnapshotConnection:
    def __init__(self, input_handler, ts_extension, server_id=0, shared_mem=False):

        from ...input_handlers import pynbody
        assert isinstance(input_handler, pynbody.PynbodyInputHandler)

        if tangos.parallel_tasks.pynbody_server.snapshot_queue._connection_active:
            raise RuntimeError("Each client can only have one remote snapshot connection at any time")



        super().__init__()

        self._server_id = server_id
        self._input_handler = input_handler
        self.filename = ts_extension
        self.identity = "%d: %s"%(self._server_id, ts_extension)
        self.connected = False
        self.shared_mem = shared_mem

        # ensure server knows what our messages are about
        remote_import.ImportRequestMessage(__name__).send(self._server_id)

        log.logger.debug("Pynbody client: attempt to connect to remote snapshot %r", ts_extension)
        RequestLoadPynbodySnapshot((input_handler, ts_extension, self.shared_mem)).send(self._server_id)
        self.underlying_pynbody_class = ConfirmLoadPynbodySnapshot.receive(self._server_id).contents
        if self.underlying_pynbody_class is None:
            raise OSError("Could not load remote snapshot %r"%ts_extension)

        tangos.parallel_tasks.pynbody_server.snapshot_queue._connection_active = True
        self.connected = True
        log.logger.debug("Pynbody client: connected to remote snapshot %r", ts_extension)

        if self.shared_mem:
            self.shared_mem_view = self.get_view(None)

    def get_view(self, filter_or_object_spec):
        """Return a RemoteSubSnap that contains either the pynbody filtered region, or the specified object from a catalogue

        filter_or_object_spec is either an instance of pynbody.filt.Filter, or a tuple containing
        (typetag, number), which are respectively the object type tag and object number to be loaded
        """
        return RemoteSnap(self, filter_or_object_spec)

    def disconnect(self):

        if not self.connected:
            return

        ReleasePynbodySnapshot(self.filename).send(self._server_id)
        tangos.parallel_tasks.pynbody_server.snapshot_queue._connection_active = False
        self.connected = False

    def __del__(self):
        self.disconnect()
