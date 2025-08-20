import argparse
import copy
import pdb
import random
import sys
import time
import traceback

import numpy as np
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.orm

from .. import config, core, live_calculation, parallel_tasks, properties
from ..cached_writer import insert_list
from ..log import logger
from ..parallel_tasks import accumulative_statistics
from ..parallel_tasks.message import Message
from ..util import proxy_object, terminalcontroller, timing_monitor
from ..util.check_deleted import check_deleted
from . import GenericTangosTool


class AttributableDict(dict):
    pass

class FileListMessage(Message):
    pass

class ObjectsListMessage(Message):
    pass

class PropertyWriter(GenericTangosTool):
    tool_name = "write"
    tool_description = "Calculate properties and write them into the tangos database"

    def __init__(self):
        self.redirect = terminalcontroller.redirect
        self._writer_timeout = config.PROPERTY_WRITER_MAXIMUM_TIME_BETWEEN_COMMITS
        self._writer_minimum = config.PROPERTY_WRITER_MINIMUM_TIME_BETWEEN_COMMITS
        self._current_timestep = None
        self._current_timestep_id = None
        self._current_timestep_particle_data = None
        self._current_object_id = None
        self._current_object = None

    @classmethod
    def add_parser_arguments(self, parser):
        parser.add_argument('properties', action='store', nargs='+',
                            help="The names of the halo/object properties to calculate")
        parser.add_argument('--sims', '--for', action='store', nargs='*',
                            metavar='simulation_name',
                            help='Specify a simulation (or multiple simulations) to run on')
        parser.add_argument('--latest', action='store_true',
                            help='Run only on the latest timesteps')
        parser.add_argument('--timesteps-matching', action='append', type=str,
                            help='Run only on timesteps with extensions matching the specified string. Multiple timesteps may be specified.')
        parser.add_argument('--force', action='store_true',
                            help='Run calculations even if a value is already stored in the database')
        parser.add_argument('--debug', action='store_true',
                            help='Run in debug mode: print rather than creating database entries')
        parser.add_argument('--catch', '--pdb', action='store_true',
                            help='Launch the python debugger when a calculation error occurs')
        parser.add_argument('--backwards', action='store_true',
                            help='Process low-z timesteps first')
        parser.add_argument('--random', action='store_true',
                            help='Process timesteps in random order')
        parser.add_argument('--with-prerequisites', action='store_true',
                            help='Automatically calculate any missing prerequisites for the properties')
        parser.add_argument('--no-resume', action='store_true',
                            help="Prevent resumption from a previous calculation, even if tangos thinks it's possible")
        parser.add_argument('--load-mode', action='store', choices=['all', 'partial', 'server', 'server-partial', 'server-shared-mem'],
                            required=False, default=None,
                            help="Select a load-mode: " \
                                 "  --load-mode partial:           each processor attempts to load only the data it needs; " \
                                 "  --load-mode server:            a server process manages the data;"
                                 "  --load-mode server-partial:    a server process figures out the indices to load, which are then passed to the partial loader" \
                                 "  --load-mode all:               each processor loads all the data (default, and often fine for zoom simulations)." \
                                 "  --load-mode server-shared-mem: a server process manages the data, passing to other processes via shared memory")
        parser.add_argument('--type', action='store', type=str, dest='htype',
                            help="Secify the object type to run on by tag name (or integer). Can be halo, group, or BH.")
        parser.add_argument('--hmin', action='store', type=int, default=0,
                            help="Do not calculate below the specified halo/object number")
        parser.add_argument('--hmax', action='store', type=int,
                            help="Do not calculate above the specified halo/object number")
        parser.add_argument('--verbose', action='store_true',
                            help="Allow all output from calculations (by default print statements are suppressed)")
        parser.add_argument('--part', action='store', nargs=2, type=int,
                            metavar=('N', 'M'),
                            help="Emulate MPI by handling slice N out of the total workload of M items. If absent, use real MPI.")
        parser.add_argument('--backend', action='store', type=str,
                            help="Specify the paralellism backend (e.g. pypar, mpi4py)")
        parser.add_argument('--include-only', action='append', type=str,
                            help="Specify a filter that describes which objects the calculation should be executed for. Multiple filters may be specified, in which case they must all evaluate to true for the object to be included.")
        parser.add_argument('--explain-classes', action='store_true',
                            help="Log some explanation for why property classes are selected (when there is any ambiguity)")

    def _create_parser_obj(self):
        parser = argparse.ArgumentParser()
        core.supplement_argparser(parser)
        return parser

    def _build_timestep_list(self):
        query : sqlalchemy.orm.query.Query = core.sim_query_from_name_list(self.options.sims)
        timesteps = []
        if self.options.latest:
            for x in query.all():
                try:
                    timesteps.append(x.timesteps[-1])
                except IndexError:
                    pass
        else:
            timestep_filter = core.timestep.TimeStep.simulation_id.in_([q.id for q in query.all()])
            if self.options.timesteps_matching is not None and len(self.options.timesteps_matching)>0:
                subfilter = core.timestep.TimeStep.extension.like(self.options.timesteps_matching[0])
                for m in self.options.timesteps_matching[1:]:
                    subfilter |= core.timestep.TimeStep.extension.like(m)
                timestep_filter &= subfilter

            timesteps = core.get_default_session().query(core.timestep.TimeStep).filter(timestep_filter). \
                order_by(core.timestep.TimeStep.time_gyr).options(
                  sqlalchemy.orm.joinedload(core.timestep.TimeStep.simulation)
                  # this joined load might seem like overkill but without it
                  # test_db_writer.py:test_writer_with_property_accessing_timestep will fail because the
                  # timestep gets updated by a later query to have raiseload("*")
               ).all()

        unique_simulations = {f.simulation for f in timesteps}

        for sim in unique_simulations:
            sim.cache_properties()
            sqlalchemy.orm.make_transient(sim)

        for ts in timesteps:
            sqlalchemy.orm.make_transient(ts)

        if self.options.backwards:
            timesteps = timesteps[::-1]

        if self.options.random:
            random.seed(0)
            random.shuffle(timesteps)

        self._timesteps_to_process = timesteps

    @property
    def timesteps_to_process(self):
        if not hasattr(self, '_timesteps_to_process'):
            if self._is_lead_rank():
                self._build_timestep_list()
                self._transmit_file_list()
            else:
                self._timesteps_to_process = self._receive_file_list()
        return self._timesteps_to_process


    def _get_parallel_timestep_iterator(self):
        if parallel_tasks.backend is None:
            # Go sequentially
            ma_files = self.timesteps_to_process
        elif self.options.load_mode is not None and self.options.load_mode.startswith('server'):
            # In the case of loading from a centralised server, each node works on the _same_ timestep --
            # parallelism is then implemented at the halo level
            ma_files = parallel_tasks.synchronized(self.timesteps_to_process, allow_resume=not self.options.no_resume,
                                                   resumption_id='parallel-timestep-iterator')
        else:
            # In all other cases, different timesteps are distributed to different nodes
            ma_files = parallel_tasks.distributed(self.timesteps_to_process, allow_resume=not self.options.no_resume,
                                                  resumption_id='parallel-timestep-iterator')
        return ma_files

    def _get_parallel_object_iterator(self, items):
        if self.options.load_mode is not None and self.options.load_mode.startswith('server'):
            # Only in 'server' mode is parallelism undertaken at the halo level. See also
            # _get_parallel_timestep_iterator.

            assert parallel_tasks.backend.size()>1, "Cannot use this load mode outside of a parallel session"

            # First, we need to make a barrier because we can't start writing to the database
            # before all nodes have generated their local work lists
            parallel_tasks.barrier()

            return parallel_tasks.distributed(items, allow_resume=False)
        else:
            return items

    def parse_command_line(self, argv=None):
        parser = self._get_parser_obj()
        self.process_options(parser.parse_args(argv))

    def process_options(self, options):
        self.options = options
        core.process_options(self.options)
        self._compile_inclusion_criterion()

        if self.options.load_mode=='all':
            self.options.load_mode=None

        if self.options.verbose:
            self.redirect.enabled = False



    def _compile_inclusion_criterion(self):
        if self.options.include_only:
            includes = ["("+s+")" for s in self.options.include_only]
            include_only_combined = " & ".join(includes)
            self._include = include_only_combined
        else:
            self._include = None

    def _is_lead_rank(self):
        return parallel_tasks.backend is None or parallel_tasks.backend.rank()==1 or self.options.load_mode is None

    def _log_once_per_timestep(self, *args):
        if self._is_lead_rank():
            logger.info(*args)

    def _make_transient(self, object_list: list[core.halo.SimulationObjectBase]):
        """
        Prepare a stripped-back version of the objects in object_list for transmission/
        isolation from the database itself
        """

        for obj in object_list:
            sqlalchemy.orm.make_transient(obj)
            obj.timestep = None # no need to transmit this again

            # not sure why make_transient doesn't clear this... you'd think it should?
            obj._sa_instance_state.committed_state = {}

            obj.all_properties.clear()
            obj.all_links.clear()




    def _build_object_list(self, db_timestep):
        object_query = self._get_object_list_query(db_timestep)
        self._objects_this_timestep = object_query.all()

    def _filter_object_list(self):
        if self._include:
            inclusion = self._inclusion_mask_this_timestep
            if any([not (np.issubdtype(inc_i, np.bool_) or inc_i is None) for inc_i in inclusion]):
                raise ValueError("Specified inclusion criterion does not return a boolean")
            if len(inclusion) != len(self._objects_this_timestep):
                raise ValueError("Inclusion criterion did not generate results for all the objects")

            self._log_once_per_timestep("User-specified inclusion criterion excluded %d of %d objects",
                                        len(inclusion) - len(self._objects_this_timestep), len(inclusion))

            self._objects_this_timestep = [halo_i for halo_i, include_i in zip(self._objects_this_timestep, inclusion)
                                           if include_i]


    def _attach_track_data_to_trackers(self):
        track_objects = []
        track_ids = []
        for dbo in self._objects_this_timestep:
            if isinstance(dbo, core.halo.Tracker):
                track_objects.append(dbo)
                track_ids.append(dbo.halo_number)
        all_track_data : list[core.tracking.TrackData] = core.get_default_session().query(core.tracking.TrackData).filter(
            core.tracking.TrackData.halo_number.in_(track_ids)).all()
        track_id_to_trackdata = {trackdata.halo_number: trackdata for trackdata in all_track_data}
        for t in all_track_data:
            sqlalchemy.orm.make_transient(t)

        for tracker in track_objects:
            if tracker.halo_number in track_id_to_trackdata:
                tracker._tracker = track_id_to_trackdata[tracker.halo_number]
            else:
                logger.warning("Particle IDs for tracker %r not found in the database", tracker.halo_number)
                tracker._tracker = None

    def _get_object_list_query(self, db_timestep):
        object_filter = core.halo.SimulationObjectBase.timestep == db_timestep
        if self.options.htype is not None:
            object_filter = sqlalchemy.and_(object_filter, core.halo.SimulationObjectBase.object_typecode
                                    == core.halo.SimulationObjectBase.object_typecode_from_tag(self.options.htype))
        if self.options.hmin is not None:
            object_filter = sqlalchemy.and_(object_filter, core.halo.SimulationObjectBase.halo_number >= self.options.hmin)
        if self.options.hmax is not None:
            object_filter = sqlalchemy.and_(object_filter, core.halo.SimulationObjectBase.halo_number <= self.options.hmax)
        needed_properties = self._required_and_calculated_property_names()
        # it's important that anything we need from the database is loaded now, as if it's
        # lazy-loaded later when we have relinquished the lock, SQLite may get upset
        halo_query = (core.get_default_session().query(core.halo.SimulationObjectBase).
                      options(sqlalchemy.orm.joinedload(core.halo.SimulationObjectBase.timestep),
                              sqlalchemy.orm.joinedload(core.halo.SimulationObjectBase.timestep, core.TimeStep.simulation),
                              sqlalchemy.orm.raiseload("*")).
                      order_by(core.halo.SimulationObjectBase.halo_number).filter(object_filter))

        logger.debug('Gathering existing properties for all halos in timestep %r', db_timestep)
        halo_query = live_calculation.MultiCalculation(*needed_properties).supplement_halo_query(halo_query)
        return halo_query

    def _build_existing_properties(self):
        db_objects = self._objects_this_timestep

        existing_properties = []
        needed_properties = self._required_and_calculated_property_names()
        calculation = live_calculation.MultiCalculation(*needed_properties)

        for c in calculation.calculations:
            if isinstance(c, live_calculation.StoredProperty):
                c.set_extraction_pattern(live_calculation.extraction_patterns.HaloPropertyRawValueGetter())

        existing_properties_ar = calculation.values(db_objects)

        orm_warning_issued = False

        for i, db_object in enumerate(db_objects):

            properties_this_halo = AttributableDict()

            for j, k in enumerate(needed_properties):
                existing_property = existing_properties_ar[j][i]
                if isinstance(existing_property, core.halo.SimulationObjectBase):
                    if not orm_warning_issued:
                        self._log_once_per_timestep("Cannot pass database objects to a calculation; request specific properties instead")
                        orm_warning_issued = True
                    existing_property = None

                properties_this_halo[k] = existing_property

            properties_this_halo.halo_number = properties_this_halo['halo_number()']
            properties_this_halo.finder_id = properties_this_halo['finder_id()']
            properties_this_halo.finder_offset = properties_this_halo['finder_offset()']
            properties_this_halo.NDM = properties_this_halo['NDM()']
            properties_this_halo.NGas = properties_this_halo['NGas()']
            properties_this_halo.NStar = properties_this_halo['NStar()']
            properties_this_halo.object_typecode = properties_this_halo['type()']
            properties_this_halo['halo_number'] = properties_this_halo.halo_number
            properties_this_halo['finder_id'] = properties_this_halo.finder_id
            properties_this_halo.id = properties_this_halo['dbid()']

            existing_properties.append(properties_this_halo)

        if self._include is not None:
            _inclusion_column = needed_properties.index(self._include)
            self._inclusion_mask_this_timestep = existing_properties_ar[_inclusion_column]

        self._existing_properties_this_timestep = existing_properties


    def _is_commit_needed(self, end_of_timestep):
        if len(self._pending_properties)==0:
            return False
        elif end_of_timestep:
            return True
        elif time.time() - self._last_commit_time > self._writer_timeout:
            return True
        else:
            return False


    def _commit_results_if_needed(self, end_of_timestep=False):
        need_to_commit = self._is_commit_needed(end_of_timestep)

        if need_to_commit:
            self._commit_results()

        if need_to_commit or end_of_timestep:
            self.tracker.report_to_log_or_server(logger)
            self.timing_monitor.report_to_log_or_server(logger)

            from ..parallel_tasks import message
            message.update_performance_stats()

    def _commit_results(self):
        commit_on_server = self.options.load_mode and self.options.load_mode.startswith('server')
        insert_list(self._pending_properties, self._current_timestep_id, commit_on_server)
        self._pending_properties = []
        self._last_commit_time = time.time()

    def _queue_results_for_later_commit(self, db_object, names, results, existing_properties_data):
        for n, r in zip(names, results):
            if self.options.force or existing_properties_data[n] is None:
                existing_properties_data[n] = r
                if self.options.debug:
                    logger.info("Debug mode - not creating property %r for %r with value %r", n, db_object, r)
                else:
                    self._pending_properties.append((proxy_object.ProxyObjectFromDatabaseId(db_object.id), n, r))

    def _required_and_calculated_property_names(self):
        needed = []
        for x in self._property_calculator_instances:
            if isinstance(x.names, str):
                needed.extend([x.names])
            else:
                needed.extend([name for name in x.names])
            needed.extend([name for name in x.requires_property()])

        if self._include:
            needed.append(self._include)

        return (["NDM()", "NStar()", "NGas()", "halo_number()", "finder_id()", "finder_offset()",
                 "dbid()", "type()"] + [str(s) for s in np.unique(needed)])

    def _should_load_particles(self):
        return any([x.requires_particle_data for x in self._property_calculator_instances])

    def _must_load_timestep_particles(self):
        return self._should_load_particles() and self.options.load_mode is None

    def _unload_timestep(self):
        self._current_object = None
        self._current_halo_id = None
        with check_deleted(self._current_timestep_particle_data):
            self._current_timestep_particle_data=None
            self._current_timestep_id = None
            self._current_timestep = None

    def _set_current_timestep(self, db_timestep):
        if self._current_timestep_id == db_timestep.id:
            return

        with parallel_tasks.lock.SharedLock("insert_list"):
            # don't want this to happen in parallel with a database write -- seems to lazily fetch
            # rows in the background
            self._unload_timestep()

            if self._must_load_timestep_particles():
                self._current_timestep_particle_data = db_timestep.load(mode=self.options.load_mode)

            elif self._should_load_particles():
                # Keep a snapshot alive for this timestep, even if should_load_timestep_particles is False,
                # because it might be needed for the iord's if we are in partial-load mode.
                try:
                    self._current_timestep_particle_data = db_timestep.load(mode=self.options.load_mode)
                except OSError:
                    pass

            if self.options.load_mode is None:
                self._run_preloop(self._current_timestep_particle_data, db_timestep,
                                  self._property_calculator_instances, self._objects_this_timestep)
            else:
                self._run_preloop(None, db_timestep,
                                  self._property_calculator_instances, self._objects_this_timestep)

            self._current_timestep_id = db_timestep.id
            self._current_timestep = db_timestep

    def _clear_timestep_region_cache(self):
        if self._current_timestep_particle_data is None:
            return

        if hasattr(self._current_timestep_particle_data, '_tangos_cached_regions'):
            self._current_timestep_particle_data._tangos_cached_regions.clear()


    def _set_current_object(self, db_object):
        # in shared-mem mode, region cache can end up unexpectedly using lots of mem per process since it's stored
        # in a per-process cache. Let's assume there is not much performance penalty to just clearing it after each
        # halo is procssed:
        self._clear_timestep_region_cache()

        if self._current_object_id==db_object.id:
            return

        self._current_object_id=db_object.id
        self._current_object = None

        if self._should_load_particles():
            self._current_object  = db_object.load(mode=self.options.load_mode)

        if self.options.load_mode is not None:
            self._run_preloop(self._current_object, db_object.timestep,
                              self._property_calculator_instances, self._objects_this_timestep)


    def _get_current_object_specified_region_particles(self, db_halo, region_spec):
        return db_halo.timestep.load_region(region_spec, self.options.load_mode,
                                            self._estimate_num_region_calculations_this_timestep())

    def _get_object_snapshot_data_if_appropriate(self, db_halo, db_data, property_calculator):

        self._set_current_object(db_halo)

        if property_calculator.region_specification(db_data) is not None:
            return self._get_current_object_specified_region_particles(db_halo, property_calculator.region_specification(db_data))
        else:
            return self._current_object


    def _get_standin_property_value(self, property_calculator):
        if isinstance(property_calculator.names,str):
            return None
        num = len(property_calculator.names)
        return [None]*num

    def _get_property_value(self, db_object, property_calculator, existing_properties):

        result = self._get_standin_property_value(property_calculator)

        try:
            snapshot_data = self._get_object_snapshot_data_if_appropriate(db_object, existing_properties, property_calculator)
        except OSError as e:
            if self.tracker.should_log_error(e):
                logger.warning("Failed to load snapshot data for %r; skipping", db_object)
                self._log_traceback()
                logger.info("If this error arises again, it will be counted but not individually reported.")
            self.tracker.register_loading_error()
            return result

        with self.timing_monitor(property_calculator):
            try:
                with self.redirect:
                    result = property_calculator.calculate(snapshot_data, existing_properties)
                    self.tracker.register_success()
            except Exception as e:
                self.tracker.register_error()

                if self.tracker.should_log_error(e):
                    logger.info(f"Uncaught exception {e!r} during property calculation {property_calculator!r} applied to {db_object!r}")
                    self._log_traceback()
                    logger.info("If this error arises again, it will be counted but not individually reported.")

                if self.options.catch:
                    tbtype, value, tb = sys.exc_info()
                    pdb.post_mortem(tb)


        return result

    def _log_traceback(self):
        exc_data = traceback.format_exc()
        for line in exc_data.split("\n"):
            logger.info(line)

    def _run_preloop(self, f, db_timestep, cinstances, existing_properties_all_halos):
        for x in cinstances:
            try:
                with self.redirect:
                    x.preloop(f, db_timestep)
            except Exception:
                logger.exception(
                    f"Uncaught exception during property preloop {x!r} applied to {db_timestep!r}")
                if self.options.catch:
                    traceback.print_exc()
                    tbtype, value, tb = sys.exc_info()
                    pdb.post_mortem(tb)


    def run_property_calculation(self, db_object, property_calculator, existing_properties):
        names = property_calculator.names
        if type(names) is str:
            listize = True
            names = [names]
        else:
            listize = False

        if all([existing_properties[name] is not None for name in names]) and not self.options.force:
            self.tracker.register_already_exists()
            return

        if not property_calculator.accept(existing_properties):
            self.tracker.register_missing_prerequisite()
            return

        results = self._get_property_value(db_object, property_calculator, existing_properties)

        if listize:
            results = [results]

        self._queue_results_for_later_commit(db_object, names, results, existing_properties)

    def run_object_calculation(self, db_object, existing_properties):
        for calculator in self._property_calculator_instances:
            # the separation of db_object and existing_properties is a historical anomaly, but
            # we keep it for now, to avoid breaking existing code. Originally existing_properties
            # was a 'stand-in' dictionary of properties, but now db_object is transient while holding
            # the cache of properties itself, so we could just pass it in alone.
            self.run_property_calculation(db_object, calculator, existing_properties)

        self._commit_results_if_needed()

    def _estimate_num_region_calculations_this_timestep(self):
        num_region_props = 0
        for prop in self._property_calculator_instances:
            if prop.region_specification is not properties.PropertyCalculation.region_specification:
                # assume overriding means the class isn't just going to be returning None!
                num_region_props += 1
        num_objs = len(self._objects_this_timestep)
        return num_region_props * num_objs

    def _transmit_objects_list(self):
        if not self._should_share_query_results():
            return
        assert self._is_lead_rank()
        message = ObjectsListMessage((self._existing_properties_this_timestep, self._objects_this_timestep))
        for i in range(2, parallel_tasks.backend.size()):
            message.send(i)

    def _transmit_file_list(self):
        if not self._should_share_query_results():
            return
        assert self._is_lead_rank()
        message = FileListMessage(self.timesteps_to_process)
        for i in range(2, parallel_tasks.backend.size()):
            message.send(i)

    def _receive_file_list(self):
        assert self._should_share_query_results() and not self._is_lead_rank()
        result = FileListMessage.receive(1).contents
        return result

    def _should_share_query_results(self):
        return parallel_tasks.backend is not None and self.options.load_mode is not None

    def _receive_objects_list(self):
        assert parallel_tasks.backend is not None and self.options.load_mode is not None
        assert not self._is_lead_rank()
        self._existing_properties_this_timestep, self._objects_this_timestep = ObjectsListMessage.receive(1).contents


    def run_timestep_calculation(self, db_timestep):
        # previous steps may have updated the dictionary. Especially with multiple CPUs in operation,
        # things could be in an inconsistent state, so clear caches.
        core.dictionary.clear_dictionary_caches()

        self._log_once_per_timestep("Processing %r", db_timestep)
        self._property_calculator_instances = properties.instantiate_classes(db_timestep.simulation,
                                                                             self.options.properties,
                                                                             explain=self.options.explain_classes)

        for x in self._property_calculator_instances:
            if x.no_proxies():
                self._log_once_per_timestep("-"*80)
                self._log_once_per_timestep("IMPORTANT -- property calculator %r asks for no proxies, but this is no longer supported.", x.__class__)
                self._log_once_per_timestep("If your calculations succeed nonetheless, you can simply remove your override of no_proxies()")
                self._log_once_per_timestep("If they fail, you need to update your code; please refer to https://pynbody.github.io/tangos/custom_properties.html")
                self._log_once_per_timestep("-"*80)

        if self.options.with_prerequisites:
            self._add_prerequisites_to_calculator_instances(db_timestep)

        if self._is_lead_rank():
            with parallel_tasks.lock.SharedLock("insert_list"):
                logger.debug("Start object list query")
                self._build_object_list(db_timestep)
                self._build_existing_properties()
                self._filter_object_list()
                self._attach_track_data_to_trackers()
                core.get_default_session().expunge_all()
                self._make_transient(self._objects_this_timestep)
                logger.debug("End object list query")

            self._transmit_objects_list()
        else:
            logger.debug("Get object list from remote process")
            self._receive_objects_list()
            logger.debug("Success!")

        self._log_once_per_timestep("Successfully gathered existing properties; calculating halo properties now...")

        self._log_once_per_timestep("  %d halos to consider; %d calculation routines for each of them, resulting in %d properties per halo",
                                    len(self._objects_this_timestep), len(self._property_calculator_instances),
                                    sum([1 if isinstance(x.names, str) else len(x.names) for x in self._property_calculator_instances])
                                    )

        self._log_once_per_timestep("  The property modules are:")
        for x in self._property_calculator_instances:
            x_type = type(x)
            self._log_once_per_timestep(f"    {x_type.__module__}.{x_type.__qualname__}")

        self._set_current_timestep(db_timestep)

        for idx in self._get_parallel_object_iterator(range(len(self._objects_this_timestep))):
            db_halo = self._objects_this_timestep[idx]
            existing_properties = self._existing_properties_this_timestep[idx]

            db_halo.timestep = db_timestep
            existing_properties.timestep = db_timestep

            self.run_object_calculation(db_halo, existing_properties)

        self._log_once_per_timestep("Done with %r", db_timestep)

        self._commit_results_if_needed(end_of_timestep=True)

        self._objects_this_timestep = None

        self._unload_timestep()

    def _add_prerequisites_to_calculator_instances(self, db_timestep):
        will_calculate = []
        requirements = []
        for inst in self._property_calculator_instances:
            if isinstance(inst.names, str):
                will_calculate+=[inst.names]
            else:
                will_calculate+=inst.names
            requirements+=inst.requires_property()

        for r in requirements:
            if r not in will_calculate:
                new_instance = properties.instantiate_class(db_timestep.simulation, r)
                self._log_once_per_timestep("Missing prerequisites - added class %r", type(new_instance))
                self._log_once_per_timestep("                        providing properties %r", new_instance.names)
                self._property_calculator_instances = [new_instance]+self._property_calculator_instances
                self._add_prerequisites_to_calculator_instances(db_timestep) # everything has changed; start afresh
                break


    def run_calculation_loop(self):
        if self._should_share_query_results() and not self._is_lead_rank():
            # we are not going to touch the database from this rank
            core.get_default_session().close()
            core.get_default_engine().dispose()
        else:
            parallel_tasks.database.synchronize_creator_object()

        # NB both these objects must be created at the same place in all processes,
        # since creating them is a 'barrier'-like operation
        self.timing_monitor = timing_monitor.TimingMonitor(allow_parallel=True)
        self.tracker = CalculationSuccessTracker(allow_parallel=True)

        self._last_commit_time = time.time()
        self._pending_properties = []

        for f_obj in self._get_parallel_timestep_iterator():
            self.run_timestep_calculation(f_obj)



class CalculationSuccessTracker(accumulative_statistics.StatisticsAccumulatorBase):
    def __init__(self, allow_parallel=False):
        self.reset() # comes before __init__, since the latter stores a copy for use in report_to_log_if_needed
        super().__init__(allow_parallel=allow_parallel)

        self._posted_errors = parallel_tasks.shared_set.SharedSet('posted_errors',allow_parallel)

        if parallel_tasks.backend and parallel_tasks.backend.rank()>0:
            # because shared sets are named objects, if it was created before it might still be populated
            # this is most relevant in a testing setting (in realistic property_writer runs it won't have
            # been created before) but has caused problems before so we clear it and wait for all processes
            self._posted_errors.clear()
            parallel_tasks.barrier()


    def should_log_error(self, exception):
        tb = "\n".join(traceback.format_exception(type(exception), exception, exception.__traceback__))
        return not self._posted_errors.add_if_not_exists(tb)

    def report_to_log(self, logger):
        if sum([self._succeeded,
                self._skipped_error,
                self._skipped_loading_error,
                self._skipped_existing,
                self._skipped_missing_prerequisite]) == 0:
            return
        logger.info("PROPERTY CALCULATION SUMMARY")
        logger.info("            Succeeded: %d property calculations", self._succeeded)
        logger.info("              Errored: %d property calculations", self._skipped_error)
        logger.info("  Errored during load: %d property calculations", self._skipped_loading_error)
        logger.info("       Already exists: %d property calculations", self._skipped_existing)
        logger.info("Missing pre-requisite: %d property calculations", self._skipped_missing_prerequisite)

    def register_error(self):
        self._skipped_error+=1

    def register_loading_error(self):
        self._skipped_loading_error+=1

    def register_success(self):
        self._succeeded+=1

    def register_missing_prerequisite(self):
        self._skipped_missing_prerequisite+=1

    def register_already_exists(self):
        self._skipped_existing+=1

    def reset(self):
        self._succeeded = 0
        self._skipped_error = 0
        self._skipped_loading_error = 0
        self._skipped_existing = 0
        self._skipped_missing_prerequisite = 0

    def add(self, other):
        self._succeeded += other._succeeded
        self._skipped_error += other._skipped_error
        self._skipped_loading_error += other._skipped_loading_error
        self._skipped_existing += other._skipped_existing
        self._skipped_missing_prerequisite += other._skipped_missing_prerequisite

    def __eq__(self, other):
        return type(other) == type(self) and \
               self._succeeded == other._succeeded and \
               self._skipped_error == other._skipped_error and \
               self._skipped_loading_error == other._skipped_loading_error and \
               self._skipped_existing == other._skipped_existing and \
               self._skipped_missing_prerequisite == other._skipped_missing_prerequisite
