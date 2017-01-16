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

import numpy as np
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.orm

import properties
from ..util import terminalcontroller, timing_monitor
from .. import parallel_tasks, core
from ..cached_writer import insert_list
from ..log import logger


@contextlib.contextmanager
def check_deleted(a):
    if a is None:
        yield
        return
    else:
        a_s = weakref.ref(a)
        sys.exc_clear()
        del a
        yield
        gc.collect()
        if a_s() is not None:
            logger.error("check_deleted failed")
            logger.error("gc reports hanging references: %s", gc.get_referrers(a_s()))

class AttributableDict(dict):
    pass








class PropertyWriter(object):

    def __init__(self):
        self.redirect = terminalcontroller.redirect
        self._writer_timeout = 60
        self._writer_minimum = 60  # don't commit at end of halo if < 1 minute past
        self._current_timestep_id = None
        self._loaded_timestep = None
        self._loaded_halo_id = None
        self._loaded_halo_spherical = None
        self._loaded_halo = None

    def _get_parser_obj(self):
        parser = argparse.ArgumentParser()
        core.supplement_argparser(parser)
        parser.add_argument('properties', action='store', nargs='+',
                            help="The names of the halo properties to calculate")
        parser.add_argument('--sims','--for', action='store', nargs='*',
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
        parser.add_argument('--partial-load', action='store_true',
                            help="Load only one halo at a time - saves memory but some property calculations may prefer to see the whole simulation")
        parser.add_argument('--htype', action='store', type=int,
                            help="Secify the halo class to run on. 0=standard, 1=tracker (e.g. black holes)")
        parser.add_argument('--hmin', action='store', type=int, default=0,
                            help="Do not calculate halos below the specified halo")
        parser.add_argument('--hmax', action='store', type=int,
                            help="Do not calculate halos above the specified halo")
        parser.add_argument('--verbose', action='store_true',
                            help="Allow all output from calculations (by default print statements are suppressed)")
        parser.add_argument('--part', action='store', nargs=2, type=int,
                            metavar=('N','M'),
                            help="Emulate MPI by handling slice N out of the total workload of M items. If absent, use real MPI.")
        parser.add_argument('--backend', action='store', type=str, help="Specify the paralellism backend (e.g. pypar, mpi4py)")
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
            ma_files = parallel_tasks.distributed(self.files, proc=self.options.part[0], of=self.options.part[1])
        else:
            ma_files = parallel_tasks.distributed(self.files)
        return ma_files

    def parse_command_line(self, argv=None):
        parser = self._get_parser_obj()
        self.options = parser.parse_args(argv)
        core.process_options(self.options)
        self._build_file_list()


        if self.options.verbose:
            self.redirect.enabled = False

        self.classes = properties.providing_classes(self.options.properties)
        self.timing_monitor = timing_monitor.TimingMonitor()


    def _build_halo_list(self, db_timestep):
        query = sqlalchemy.and_(
            core.halo.Halo.timestep == db_timestep,
            sqlalchemy.or_(core.halo.Halo.NDM > 1000, core.halo.Halo.NDM == 0))
        if self.options.htype is not None:
            query = sqlalchemy.and_(query, core.halo.Halo.halo_type == self.options.htype)

        needed_properties = self._needed_properties()
        pid_list = []
        for p in needed_properties:
            try:
                pid_list.append(core.dictionary.get_dict_id(p))
            except:
                continue

        logger.info('gathering properties %r with ids %r', needed_properties, pid_list)

        halo_query = core.get_default_session().query(core.halo.Halo).order_by(core.halo.Halo.halo_number).filter(query)
        if len(pid_list)>0:
            halo_property_alias = sqlalchemy.orm.aliased(core.halo_data.HaloProperty)
            halo_alias = core.halo.Halo
            halo_query = halo_query.\
                outerjoin(halo_property_alias,(halo_alias.id==halo_property_alias.halo_id) & (halo_property_alias.name_id.in_(pid_list))).\
                options(sqlalchemy.orm.contains_eager(core.halo.Halo.all_properties, alias=halo_property_alias))
        halos = halo_query.all()
        halos = halos[self.options.hmin:]

        if self.options.hmax is not None:
            halos = halos[:self.options.hmax]

        return halos


    def _build_existing_properties(self, db_halo):
        existing_properties = db_halo.all_properties
        need_data = self._needed_property_data()
        existing_properties_data = AttributableDict(
                [(x.name.text, x.data) if x.name.text in need_data
                 else (x.name.text, None) for x in existing_properties])
        existing_properties_data.update(
                [(x.relation.text, x.halo_to) if x.relation.text in need_data
                 else (x.relation.text, None) for x in db_halo.links])
        existing_properties_data.halo_number = db_halo.halo_number
        existing_properties_data.NDM = db_halo.NDM
        existing_properties_data.NGas = db_halo.NGas
        existing_properties_data.NStar = db_halo.NStar
        existing_properties_data['halo_number'] = db_halo.halo_number
        existing_properties_data['finder_id'] = db_halo.finder_id
        return existing_properties_data

    def _build_existing_properties_all_halos(self, halos):
        logger.info("Gathering existing properties...")
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
            if isinstance(r, properties.ProxyHalo):
                # resolve halo
                r = core.get_default_session().query(core.halo.Halo).filter_by(
                    halo_number=int(r), timestep=db_halo.timestep).first()
            if self.options.force or (n not in existing_properties_data.keys()):
                existing_properties_data[n] = r
                if self.options.debug:
                    logger.info("Debug mode - not creating property %r for %r with value %r", n, db_halo, r)
                else:
                    self._pending_properties.append((db_halo, n, r))

    def _needed_properties(self):
        needed = []
        for x in self._property_calculator_instances:
            if type(x.name()) == str:
                needed.extend([x.name()])
            else:
                needed.extend([name for name in x.name()])
            needed.extend([name for name in x.requires_property()])
        return list(np.unique(needed))

    def _needed_property_data(self):
        needed = []
        for x in self._property_calculator_instances:
            needed.extend([name for name in x.requires_property()])
        return list(np.unique(needed))


    def _should_load_halo_particles(self):
        return any([x.requires_simdata() for x in self._property_calculator_instances])

    def _should_load_halo_sphere_particles(self):
        return any([(x.requires_simdata() and x.spherical_region()) for x in self._property_calculator_instances])

    def _should_load_timestep_particles(self):
        return self._should_load_halo_particles() and not self.options.partial_load

    def _set_current_timestep(self, db_timestep):
        if self._current_timestep_id == db_timestep.id:
            return

        self._loaded_halo_id=None
        self._loaded_halo=None
        self._loaded_halo_spherical=None

        if self._current_timestep_id is not None:
            with check_deleted(self._loaded_timestep):
                self._loaded_timestep=None




        if self._should_load_timestep_particles():
            self._loaded_timestep = db_timestep.load()
            self._run_preloop(self._loaded_timestep, db_timestep.filename,
                              self._property_calculator_instances, self._existing_properties_all_halos)

        else:
            # Keep a pynbody snapshot alive for this timestep, even if should_load_timestep_particles is False,
            # because it might be needed for the iord's if we are in partial-load mode.
            try:
                self._loaded_timestep = db_timestep.load()
            except IOError:
                pass

            self._run_preloop(None, db_timestep.filename,
                              self._property_calculator_instances, self._existing_properties_all_halos)

        self._current_timestep_id = db_timestep.id


    def _set_current_halo(self, db_halo):
        self._set_current_timestep(db_halo.timestep)

        if self._loaded_halo_id==db_halo.id:
            return

        self._loaded_halo_id=db_halo.id
        self._loaded_halo = None
        self._loaded_halo_spherical = None

        if self._should_load_halo_particles():
            self._loaded_halo  = db_halo.load(partial=self.options.partial_load)

        if self.options.partial_load:
            self._run_preloop(self._loaded_halo, db_halo.timestep.filename,
                             self._property_calculator_instances, self._existing_properties_all_halos)


    def _get_current_halo_spherical_region_particles(self, db_halo):
        if self._loaded_halo_spherical is None:
            if self.options.partial_load:
                self._loaded_halo_spherical = self._loaded_halo
            else:
                if 'Rvir' in self._existing_properties_this_halo and \
                   'SSC' in self._existing_properties_this_halo:
                    # TODO: This pynbody dependence should be factored out --
                    import pynbody
                    self._loaded_halo_spherical = self._loaded_halo.ancestor[pynbody.filt.Sphere(
                                                                             self._existing_properties_this_halo['Rvir'],
                                                                             self._existing_properties_this_halo['SSC'])]
                else:
                    warnings.warn("Using halo particles in place of requested spherical cut-out, "
                                  "since required halo properties are unavailable", RuntimeWarning)
                    return self._loaded_halo

        return self._loaded_halo_spherical

    def _get_halo_snapshot_data_if_appropriate(self, db_halo, property_calculator):

        self._set_current_halo(db_halo)

        if property_calculator.spherical_region():
            return self._get_current_halo_spherical_region_particles(db_halo)
        else:
            return self._loaded_halo


    def _get_standin_property_value(self, property_calculator):
        if isinstance(property_calculator.name(),str):
            return None
        num = len(property_calculator.name())
        return [None]*num

    def _get_property_value(self, db_halo, property_calculator, existing_properties):
        if property_calculator.no_proxies():
            db_data = db_halo
        else:
            db_data = existing_properties

        result = self._get_standin_property_value(property_calculator)

        try:
            snapshot_data = self._get_halo_snapshot_data_if_appropriate(db_halo, property_calculator)
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


    def _run_preloop(self, f, filename, cinstances, existing_properties_all_halos):
        for x in cinstances:
            try:
                with self.redirect:
                    x.preloop(f, filename,
                              existing_properties_all_halos)
            except Exception:
                logger.exception(
                    "Uncaught exception during property preloop %r applied to %r" % (x, filename))
                if self.options.catch:
                    traceback.print_exc()
                    tbtype, value, tb = sys.exc_info()
                    pdb.post_mortem(tb)


    def run_property_calculation(self, db_halo, property_calculator, existing_properties):
        names = property_calculator.name()
        if type(names) is str:
            listize = True
            names = [names]
        else:
            listize = False

        if all([name in existing_properties.keys() for name in names]) and not self.options.force:
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
        #print >>sys.stderr, term.RED + "H%d"%db_halo.halo_number + term.NORMAL,
        #sys.stderr.flush()
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
        with parallel_tasks.RLock("insert_list"):
            self._property_calculator_instances = properties.instantiate_classes(db_timestep.simulation, self.options.properties)
            db_halos = self._build_halo_list(db_timestep)
            self._existing_properties_all_halos = self._build_existing_properties_all_halos(db_halos)
            core.get_default_session().commit()

        logger.info("Done Gathering existing properties... calculating halo properties now...")

        logger.info("  %d halos to consider; %d property calculations for each of them",
                    len(db_halos), len(self._property_calculator_instances))

        #with parallel_tasks.RLock("insert_list"):
        #    self._existing_properties_all_halos = self._build_existing_properties_all_halos(db_halos)
        #    core.get_default_session().commit()

        for db_halo, existing_properties in zip(db_halos, self._existing_properties_all_halos):
            self._existing_properties_this_halo = existing_properties
            self.run_halo_calculation(db_halo, existing_properties)

        logger.info("Done with %r",db_timestep)

        self.tracker.report_to_log(logger)
        sys.stderr.flush()

        self._commit_results_if_needed(True)


    def run_calculation_loop(self):

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

