import multiprocessing

import pynbody

from ...parallel_tasks.async_message import AsyncProcessedMessage
from ...parallel_tasks.message import Message
from ...util.check_deleted import check_deleted
from .. import config, log


class ConfirmLoadPynbodySnapshot(Message):
    pass



class PynbodySnapshotQueue:
    def __init__(self):
        self.timestep_queue = []
        self.handler_queue = []
        self.shared_mem_queue = []
        self.load_requester_queue = []
        self.current_timestep = None
        self.current_snapshot = None
        self.current_subsnap_cache = {}
        self.current_handler = None
        self.current_portable_catalogues = {}
        self.in_use_by = []


    def add(self, requester, handler, filename, shared_mem=False):
        log.logger.debug("Pynbody server: client %d requests access to %r", requester, filename)
        if shared_mem:
            log.logger.debug(" (shared memory mode)")
        if filename==self.current_timestep:
            self._notify_available(requester)
            self.in_use_by.append(requester)
        elif filename in self.timestep_queue:
            queue_position = self.timestep_queue.index(filename)
            self.load_requester_queue[queue_position].append(requester)
            assert self.handler_queue[queue_position] == handler
            assert self.shared_mem_queue[queue_position] == shared_mem
        else:
            self.timestep_queue.append(filename)
            self.handler_queue.append(handler)
            self.load_requester_queue.append([requester])
            self.shared_mem_queue.append(shared_mem)
        self._load_next_if_free()

    def free(self, requester):
        self.in_use_by.remove(requester)
        log.logger.debug("Pynbody server: client %d is now finished with %r", requester, self.current_timestep)
        self._free_if_unused()
        self._load_next_if_free()

    def get_subsnap(self, filter_or_object_spec, fam):
        if filter_or_object_spec is None:
            if fam is None:
                return self.current_snapshot
            else:
                if fam not in self.current_subsnap_cache.keys():
                    self.current_subsnap_cache[fam] = self.current_snapshot[fam]
                return self.current_subsnap_cache[fam]

        elif (filter_or_object_spec, fam) in self.current_subsnap_cache:
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
                                                    filter_or_object_spec.object_index,
                                                    filter_or_object_spec.object_typetag)
        elif isinstance(filter_or_object_spec, TrackingSpecification):
            snap = self.current_handler.load_tracked_region(self.current_timestep, filter_or_object_spec.track_data)
        else:
            raise TypeError("filter_or_object_spec must be either a pynbody filter or an ObjectRequestInformation object")

        if fam is not None:
            snap = snap[fam]

        return snap

    def get_catalogue(self, type_tag):
        return self.current_handler.get_catalogue(self.current_timestep, type_tag)

    def get_shared_catalogue(self, type_tag):
        if type_tag in self.current_portable_catalogues:
            log.logger.debug("Pynbody server: cache hit for catalogue %r", type_tag)
            return self.current_portable_catalogues[type_tag]
        else:
            log.logger.info("Generating a shared object catalogue for %rs", type_tag)
            self.current_portable_catalogues[type_tag] = self.get_catalogue(type_tag)
            return self.current_portable_catalogues[type_tag]

    def build_tree(self):
        if not hasattr(self.current_snapshot, "kdtree"):
            log.logger.info("Building KDTree")
            if config.pynbody_build_kdtree_all_cpus:
                # get number of processors on this system using python multiprocessing module
                num_threads = multiprocessing.cpu_count()
            else:
                num_threads = None
            self.current_snapshot.build_tree(num_threads=num_threads,
                                             shared_mem=self.current_shared_mem_flag)

    def _free_if_unused(self):
        if len(self.in_use_by)==0:
            from . import RequestPynbodyArray
            log.logger.info(
                f"Closing snapshot {self.current_timestep} after processing "
                f"{RequestPynbodyArray.get_num_requests()} array fetches")
            if RequestPynbodyArray.get_num_requests() > 0:
                log.logger.info("    Summed process waiting time: %.1fs", RequestPynbodyArray.get_total_wait_time())
                RequestPynbodyArray.reset_performance_stats()

            with check_deleted(self.current_snapshot):
                self.current_snapshot = None
                self.current_timestep = None
                self.current_subsnap_cache = {}
                self.current_portable_catalogues = {}
                self.current_handler = None

    def _notify_available(self, node):
        log.logger.debug("Pynbody server: notify %d that snapshot is now available", node)
        ConfirmLoadPynbodySnapshot(type(self.current_snapshot)).send(node)

    def _notify_unavailable(self, node):
        log.logger.debug("Pynbody server: notify %d that snapshot is unavailable", node)
        ConfirmLoadPynbodySnapshot(None).send(node)

    def _load_next_if_free(self):
        if len(self.timestep_queue)==0:
            return

        if self.current_handler is None:
            # TODO: Error handling
            self.current_timestep = self.timestep_queue.pop(0)
            self.current_handler = self.handler_queue.pop(0)
            self.current_shared_mem_flag = self.shared_mem_queue.pop(0)
            notify = self.load_requester_queue.pop(0)

            try:
                self.current_snapshot = self.current_handler.load_timestep(self.current_timestep)
                log.logger.info("Pynbody server: loaded %r", self.current_timestep)
                if self.current_shared_mem_flag:
                    log.logger.info("                (shared memory mode)")
                    self.current_snapshot._shared_arrays = True
                self.current_snapshot.physical_units()
                success = True
            except OSError:
                success = False

            if success:
                self.in_use_by = notify
                for n in notify:
                    self._notify_available(n)
            else:
                self.current_timestep = None
                self.current_handler = None
                self.current_snapshot = None

                for n in notify:
                    self._notify_unavailable(n)
                self._load_next_if_free()

        else:
            log.logger.info("The currently loaded snapshot is still required and so other clients will have to wait")
            log.logger.info("(Currently %d snapshots are in the queue to be loaded later)", len(self.timestep_queue))


_server_queue = PynbodySnapshotQueue()


class RequestLoadPynbodySnapshot(AsyncProcessedMessage):
    def process(self):
        _server_queue.add(self.source, *self.contents)


class ReleasePynbodySnapshot(AsyncProcessedMessage):
    def process(self):
        _server_queue.free(self.source)


_connection_active = False


class ObjectSpecification:
    def __init__(self, object_number, object_index, object_typetag='halo'):
        self.object_number = object_number
        self.object_index = object_index
        self.object_typetag = object_typetag

    def __repr__(self):
        return "ObjectSpecification(%d, %d, %r)"%(self.object_number, self.object_index, self.object_typetag)

    def __eq__(self, other):
        if not isinstance(other, ObjectSpecification):
            return False
        return self.object_number==other.object_number and self.object_typetag==other.object_typetag

    def __hash__(self):
        return hash((self.object_number, self.object_index, self.object_typetag))

class TrackingSpecification:
    def __init__(self, track_number, track_data):
        self.track_number = track_number
        self.track_data = track_data

    def __repr__(self):
        return f"TrackNumber({self.track_number})"

    def __eq__(self, other):
        if not isinstance(other, TrackingSpecification):
            return False
        return self.track_number==other.track_number

    def __hash__(self):
        return hash((self.track_number,self.track_data))
