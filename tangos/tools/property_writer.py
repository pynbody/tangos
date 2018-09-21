from __future__ import absolute_import
import argparse
import contextlib
import gc
import pdb
import random
import sys
import time
import traceback
import warnings
import weakref
import six
import numpy as np
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.orm

from . import GenericTangosTool
from .. import properties
from ..util import terminalcontroller, timing_monitor, proxy_object
from .. import parallel_tasks, core
from ..util.check_deleted import check_deleted
from ..cached_writer import insert_list
from ..log import logger
from six.moves import zip
from tangos import live_calculation



class AttributableDict(dict):
    pass


class PropertyWriter(GenericTangosTool):
    tool_name = "write"
    tool_description = "Calculate properties and write them into the tangos database"

    def __init__(self):
        self.redirect = terminalcontroller.redirect
        self._writer_timeout = 60
        self._writer_minimum = 60  # don't commit at end of halo if < 1 minute past
        self._current_timestep_id = None
        self._loaded_timestep = None
        self._loaded_halo_id = None
        self._loaded_halo = None

    @classmethod
    def add_parser_arguments(self, parser):
        parser.add_argument('properties', action='store', nargs='+',
                            help="The names of the halo properties to calculate")
        parser.add_argument('--sims', '--for', action='store', nargs='*',
                            metavar='simulation_name',
                            help='Specify a simulation (or multiple simulations) to run on')
        parser.add_argument('--latest', action='store_true',
                            help='Run only on the latest timesteps')
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
        parser.add_argument('--load-mode', action='store', choices=['all', 'partial', 'server', 'server-partial'],
                            required=False, default=None,
                            help="Select a load-mode: " \
                                 "  --load-mode partial:        each node attempts to load only the data it needs; " \
                                 "  --load-mode server:         a server process manages the data;"
                                 "  --load-mode server-partial: a server process figures out the indices to load, which are then passed to the partial loader" \
                                 "  --load-mode all:            each node loads all the data (default, and often fine for zoom simulations).")
        parser.add_argument('--type', action='store', type=str, dest='htype',
                            help="Secify the object type to run on by tag name (or integer). Can be halo, group, or BH.")
        parser.add_argument('--hmin', action='store', type=int, default=0,
                            help="Do not calculate halos below the specified halo")
        parser.add_argument('--hmax', action='store', type=int,
                            help="Do not calculate halos above the specified halo")
        parser.add_argument('--verbose', action='store_true',
                            help="Allow all output from calculations (by default print statements are suppressed)")
        parser.add_argument('--part', action='store', nargs=2, type=int,
                            metavar=('N', 'M'),
                            help="Emulate MPI by handling slice N out of the total workload of M items. If absent, use real MPI.")
        parser.add_argument('--backend', action='store', type=str,
                            help="Specify the paralellism backend (e.g. pypar, mpi4py)")
        parser.add_argument('--include-only', action='append', type=str,
                            help="Specify a filter that describes which objects the calculation should be executed for. Multiple filters may be specified, in which case they must all evaluate to true for the object to be included.")

    def _create_parser_obj(self):
        parser = argparse.ArgumentParser()
        core.supplement_argparser(parser)
        return parser

    def _build_file_list(self):

        query = core.sim_query_from_name_list(self.options.sims)
        files = []
        if self.options.latest:
            for x in query.all():
                try:
                    files.append(x.timesteps[-1])
                except IndexError:
                    pass
        else:
            files = core.get_default_session().query(core.timestep.TimeStep).filter(
                core.timestep.TimeStep.simulation_id.in_([q.id for q in query.all()])). \
                order_by(core.timestep.TimeStep.time_gyr).all()

        if self.options.backwards:
            files = files[::-1]

        if self.options.random:
            random.seed(0)
            random.shuffle(files)

        self.files = files

    def _get_parallel_timestep_iterator(self):
        if self.options.part is not None:
            # In the case of a null backend with manual parallelism, pass the specified part specification
            ma_files = parallel_tasks.distributed(self.files, proc=self.options.part[0], of=self.options.part[1])
        elif self.options.load_mode is not None and self.options.load_mode.startswith('server'):
            # In the case of loading from a centralised server, each node works on the _same_ timestep --
            # parallelism is then implemented at the halo level
            ma_files = self.files
        else:
            # In all other cases, different timesteps are distributed to different nodes
            ma_files = parallel_tasks.distributed(self.files)
        return ma_files

    def _get_parallel_halo_iterator(self, items):
        if self.options.load_mode is not None and self.options.load_mode.startswith('server'):
            # Only in 'server' mode is parallelism undertaken at the halo level. See also
            # _get_parallel_timestep_iterator.

            assert parallel_tasks.backend.size()>1, "Cannot use this load mode outside of a parallel session"

            # First, we need to make a barrier because we can't start writing to the database
            # before all nodes have generated their local work lists
            parallel_tasks.barrier()

            return parallel_tasks.distributed(items)
        else:
            return items

    def parse_command_line(self, argv=None):
        parser = self._get_parser_obj()
        self.process_options(parser.parse_args(argv))

    def process_options(self, options):
        self.options = options
        core.process_options(self.options)
        self._build_file_list()
        self._compile_inclusion_criterion()

        if self.options.load_mode=='all':
            self.options.load_mode=None

        if self.options.verbose:
            self.redirect.enabled = False

        self.timing_monitor = timing_monitor.TimingMonitor()

    def _compile_inclusion_criterion(self):
        if self.options.include_only:
            includes = ["("+s+")" for s in self.options.include_only]
            include_only_combined = " & ".join(includes)
            self._include = live_calculation.parser.parse_property_name(include_only_combined)
        else:
            self._include = None


    def _build_halo_list(self, db_timestep):
        query = core.halo.Halo.timestep == db_timestep
        if self.options.htype is not None:
            query = sqlalchemy.and_(query, core.halo.Halo.object_typecode
                                    == core.halo.Halo.object_typecode_from_tag(self.options.htype))

        if self.options.hmin is not None:
            query = sqlalchemy.and_(query, core.halo.Halo.halo_number>=self.options.hmin)

        if self.options.hmax is not None:
            query = sqlalchemy.and_(query, core.halo.Halo.halo_number<=self.options.hmax)

        needed_properties = self._required_and_calculated_property_names()

        halo_query = core.get_default_session().query(core.halo.Halo).order_by(core.halo.Halo.halo_number).filter(query)
        if self._include:
            needed_properties.append(self._include)

        logger.info('Gathering existing properties for all halos in timestep %r',db_timestep)
        halo_query = live_calculation.MultiCalculation(*needed_properties).supplement_halo_query(halo_query)

        halos = halo_query.all()

        if self._include:
            inclusion, = self._include.values(halos)
            if any([not np.issubdtype(inc_i, np.bool) for inc_i in inclusion]):
                raise ValueError("Specified inclusion criterion does not return a boolean")
            if len(inclusion)!=len(halos):
                raise ValueError("Inclusion criterion did not generate results for all the halos")

            # perform filtering:
            halos = [halo_i for halo_i, include_i in zip(halos, inclusion) if include_i]
            logger.info("User-specified inclusion criterion excluded %d of %d halos",
                        len(inclusion)-len(halos),len(inclusion))

        return halos


    def _build_existing_properties(self, db_halo):
        existing_properties = db_halo.all_properties
        need_data = self._required_and_calculated_property_names()
        need_data_ids = [core.get_dict_id(x,None) for x in need_data]

        existing_properties_data = AttributableDict()
        for x in existing_properties:
            if x.name_id in need_data_ids:
                name = need_data[need_data_ids.index(x.name_id)]
                existing_properties_data[name] = x.data

        existing_properties_data.halo_number = db_halo.halo_number
        existing_properties_data.NDM = db_halo.NDM
        existing_properties_data.NGas = db_halo.NGas
        existing_properties_data.NStar = db_halo.NStar
        existing_properties_data['halo_number'] = db_halo.halo_number
        existing_properties_data['finder_id'] = db_halo.finder_id
        return existing_properties_data

    def _build_existing_properties_all_halos(self, halos):
        return [self._build_existing_properties(h) for h in halos]
        

    def _is_commit_needed(self, end_of_timestep, end_of_simulation):
        if len(self._pending_properties)==0:
            return False
        if end_of_simulation:
            return True
        elif end_of_timestep and (time.time() - self._start_time > self._writer_minimum):
            return True
        elif time.time() - self._start_time > self._writer_timeout:
            return True
        else:
            return False


    def _commit_results_if_needed(self, end_of_timestep=False, end_of_simulation=False):

        if self._is_commit_needed(end_of_timestep, end_of_simulation):
            logger.info("Attempting to commit %d halo properties...", len(self._pending_properties))
            insert_list(self._pending_properties)
            logger.info("%d properties were committed", len(self._pending_properties))
            self._pending_properties = []
            self._start_time = time.time()
            self.timing_monitor.summarise_timing(logger)

    def _queue_results_for_later_commit(self, db_halo, names, results, existing_properties_data):
        for n, r in zip(names, results):
            if isinstance(r, proxy_object.ProxyObjectBase):
                # TODO: possible optimization here using relative_to_timestep_cache
                r = r.relative_to_timestep_id(self._current_timestep_id).resolve(core.get_default_session())
            if self.options.force or (n not in list(existing_properties_data.keys())):
                existing_properties_data[n] = r
                if self.options.debug:
                    logger.info("Debug mode - not creating property %r for %r with value %r", n, db_halo, r)
                else:
                    self._pending_properties.append((db_halo, n, r))

    def _required_and_calculated_property_names(self):
        needed = []
        for x in self._property_calculator_instances:
            if isinstance(x.names, six.string_types):
                needed.extend([x.names])
            else:
                needed.extend([name for name in x.names])
            needed.extend([name for name in x.requires_property()])
        return list(np.unique(needed))

    def _should_load_halo_particles(self):
        return any([x.requires_particle_data for x in self._property_calculator_instances])

    def _must_load_timestep_particles(self):
        return self._should_load_halo_particles() and self.options.load_mode is None

    def _unload_timestep(self):
        self._loaded_halo = None
        self._current_halo_id = None
        with check_deleted(self._loaded_timestep):
            self._loaded_timestep=None
            self._current_timestep_id = None


    def _set_current_timestep(self, db_timestep):
        if self._current_timestep_id == db_timestep.id:
            return

        self._unload_timestep()

        if self._must_load_timestep_particles():
            self._loaded_timestep = db_timestep.load(mode=self.options.load_mode)

        elif self._should_load_halo_particles():
            # Keep a snapshot alive for this timestep, even if should_load_timestep_particles is False,
            # because it might be needed for the iord's if we are in partial-load mode.
            try:
                self._loaded_timestep = db_timestep.load(mode=self.options.load_mode)
            except IOError:
                pass

        if self.options.load_mode is None:
            self._run_preloop(self._loaded_timestep, db_timestep,
                              self._property_calculator_instances, self._existing_properties_all_halos)
        else:
            self._run_preloop(None, db_timestep,
                              self._property_calculator_instances, self._existing_properties_all_halos)

        self._current_timestep_id = db_timestep.id


    def _set_current_halo(self, db_halo):
        self._set_current_timestep(db_halo.timestep)

        if self._loaded_halo_id==db_halo.id:
            return

        self._loaded_halo_id=db_halo.id
        self._loaded_halo = None

        if self._should_load_halo_particles():
            self._loaded_halo  = db_halo.load(mode=self.options.load_mode)

        if self.options.load_mode is not None:
            self._run_preloop(self._loaded_halo, db_halo.timestep,
                              self._property_calculator_instances, self._existing_properties_all_halos)


    def _get_current_halo_specified_region_particles(self, db_halo, region_spec):
        return db_halo.timestep.load_region(region_spec,self.options.load_mode)

    def _get_halo_snapshot_data_if_appropriate(self, db_halo, db_data, property_calculator):

        self._set_current_halo(db_halo)

        if property_calculator.region_specification(db_data) is not None:
            return self._get_current_halo_specified_region_particles(db_halo, property_calculator.region_specification(db_data))
        else:
            return self._loaded_halo


    def _get_standin_property_value(self, property_calculator):
        if isinstance(property_calculator.names,six.string_types):
            return None
        num = len(property_calculator.names)
        return [None]*num

    def _get_property_value(self, db_halo, property_calculator, existing_properties):
        if property_calculator.no_proxies():
            db_data = db_halo
        else:
            db_data = existing_properties

        result = self._get_standin_property_value(property_calculator)

        try:
            snapshot_data = self._get_halo_snapshot_data_if_appropriate(db_halo, db_data, property_calculator)
        except IOError:
            logger.warn("Failed to load snapshot data for %r; skipping",db_halo)
            self.tracker.register_loading_error()
            return result

        with self.timing_monitor(property_calculator):
            try:
                with self.redirect:
                    result = property_calculator.calculate(snapshot_data, db_data)
                    self.tracker.register_success()
            except Exception:
                self.tracker.register_error()

                if self.tracker.should_log_error(property_calculator):
                    logger.exception("Uncaught exception during property calculation %r applied to %r"%(property_calculator, db_halo))
                    logger.info("Further errors from this calculation on this timestep will be counted but not individually reported.")

                if self.options.catch:
                    tbtype, value, tb = sys.exc_info()
                    pdb.post_mortem(tb)


        return result


    def _run_preloop(self, f, db_timestep, cinstances, existing_properties_all_halos):
        for x in cinstances:
            try:
                with self.redirect:
                    x.preloop(f, db_timestep)
            except Exception:
                logger.exception(
                    "Uncaught exception during property preloop %r applied to %r" % (x, db_timestep))
                if self.options.catch:
                    traceback.print_exc()
                    tbtype, value, tb = sys.exc_info()
                    pdb.post_mortem(tb)


    def run_property_calculation(self, db_halo, property_calculator, existing_properties):
        names = property_calculator.names
        if type(names) is str:
            listize = True
            names = [names]
        else:
            listize = False

        if all([name in list(existing_properties.keys()) for name in names]) and not self.options.force:
            self.tracker.register_already_exists()
            return

        if not property_calculator.accept(existing_properties):
            self.tracker.register_missing_prerequisite()
            return

        results = self._get_property_value(db_halo, property_calculator, existing_properties)

        if listize:
            results = [results]

        self._queue_results_for_later_commit(db_halo, names, results, existing_properties)

    def run_halo_calculation(self, db_halo, existing_properties):
        for calculator in self._property_calculator_instances:
            busy = True
            nloops = 0
            while busy is True:
                busy = False
                try:
                    nloops += 1
                    self.run_property_calculation(db_halo, calculator, existing_properties)
                except sqlalchemy.exc.OperationalError:
                    if nloops > 10:
                        raise
                    time.sleep(1)
                    busy = True

        self._commit_results_if_needed()


    def run_timestep_calculation(self, db_timestep):
        self.tracker = CalculationSuccessTracker()

        logger.info("Processing %r", db_timestep)
        self._property_calculator_instances = properties.instantiate_classes(db_timestep.simulation, self.options.properties)
        if self.options.with_prerequisites:
            self._add_prerequisites_to_calculator_instances(db_timestep)

        with parallel_tasks.lock.SharedLock("insert_list"):
            logger.debug("Start halo list query")
            db_halos = self._build_halo_list(db_timestep)
            logger.debug("End halo list query")

        self._existing_properties_all_halos = self._build_existing_properties_all_halos(db_halos)
        
        logger.info("Successfully gathered existing properties; calculating halo properties now...")

        logger.info("  %d halos to consider; %d property calculations for each of them",
                    len(db_halos), len(self._property_calculator_instances))

        for db_halo, existing_properties in \
                self._get_parallel_halo_iterator(list(zip(db_halos, self._existing_properties_all_halos))):
            self._existing_properties_this_halo = existing_properties
            self.run_halo_calculation(db_halo, existing_properties)

        logger.info("Done with %r",db_timestep)
        self._unload_timestep()

        self.tracker.report_to_log(logger)
        sys.stderr.flush()

        self._commit_results_if_needed(True)

    def _add_prerequisites_to_calculator_instances(self, db_timestep):
        will_calculate = []
        requirements = []
        for inst in self._property_calculator_instances:
            if isinstance(inst.names, six.string_types):
                will_calculate+=[inst.names]
            else:
                will_calculate+=inst.names
            requirements+=inst.requires_property()

        for r in requirements:
            if r not in will_calculate:
                new_instance = properties.instantiate_class(db_timestep.simulation, r)
                logger.info("Missing prerequisites - added class %r",type(new_instance))
                logger.info("                        providing properties %r",new_instance.names)
                self._property_calculator_instances = [new_instance]+self._property_calculator_instances
                self._add_prerequisites_to_calculator_instances(db_timestep) # everything has changed; start afresh
                break


    def run_calculation_loop(self):
        parallel_tasks.database.synchronize_creator_object()

        self._start_time = time.time()
        self._pending_properties = []

        for f_obj in self._get_parallel_timestep_iterator():
            self.run_timestep_calculation(f_obj)

        self._commit_results_if_needed(True,True)


class CalculationSuccessTracker(object):
    def __init__(self):
        self._skipped_existing = 0
        self._skipped_missing_prerequisite = 0
        self._skipped_error = 0
        self._skipped_loading_error = 0
        self._succeeded = 0

        self._posted_errors = set()

    def should_log_error(self, from_module):
        if from_module not in self._posted_errors:
            self._posted_errors.add(from_module)
            return True
        else:
            return False

    def report_to_log(self, logger):
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

