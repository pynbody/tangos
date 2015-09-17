#!/usr/bin/env python


import halo_db as db
from halo_db import parallel_tasks
import warnings
import pynbody
import numpy as np
import properties
import sim_output_finder
import terminalcontroller
from terminalcontroller import heading, term
import sys
import sqlalchemy
import sqlalchemy.exc
import time
import gc
import weakref
import contextlib
import argparse
import pdb
import traceback
import time
import random

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
            print "check_deleted failed"
            print "gc reports hanging references: ", gc.get_referrers(a_s())


np.seterr(all='ignore')


class AttributableDict(dict):
    pass


#BlockingSession = db.Session

def summarise_timing(timing_details):
    print
    heading("Current timing details")
    v_tot = 1e-10
    for k, v in timing_details.iteritems():
        if hasattr(v, "__len__"):
            v_tot += sum(v)
        else:
            v_tot += v

    for k, v in timing_details.iteritems():
        name = str(k)[:-2]
        name = name.split(".")[-1]
        name = "%20s" % (name[-20:])
        if hasattr(v, "__len__"):
            print term.BLUE + name + term.NORMAL, "%.1fs / %.1f%%" % (sum(v), 100 * sum(v) / v_tot)
            print term.GREEN, "  ------ INTERNAL BREAKDOWN ------", term.NORMAL
            for i, this_v in enumerate(v):
                print " ", term.GREEN, "%8s %8s" % (k._time_marks_info[i], k._time_marks_info[i + 1]),
                print term.NORMAL, " %.1fs / %.1f%% / %.1f%%" % (this_v, 100 * this_v / sum(v), 100 * this_v / v_tot)
            print term.GREEN, "  --------------------------------", term.NORMAL
        else:
            print term.BLUE + name + term.NORMAL, "%.1fs / %.1f%%" % (v, 100 * v / v_tot)


def create_property(halo, name, prop, session):

    name = db.get_or_create_dictionary_item(session, name)

    if isinstance(prop, db.Halo):
        px = db.HaloLink(halo, prop, name)
    else:
        px = db.HaloProperty(halo, name, prop)
    return px


def insert_list(property_list, retry=10):

    session = db.core.internal_session

    try:
        property_object_list = [create_property(
            p[0], p[1], p[2], session) for p in property_list if p[2] is not None]
        prop_new = []

        for (prop,pl) in zip(property_object_list, property_list):
            prop_merged = session.merge(prop)
            # don't understand why this should be necessary
            prop_merged.creator = db.core.current_creator

        session.commit()

    except sqlalchemy.exc.OperationalError:
        session.rollback()
        if retry > 0:
            print "DB is locked, retrying in 1 second (%d attempts remain)..." % retry
            time.sleep(1)
            insert_list(property_list, retry=retry - 1)
        else:
            raise





class DbWriter(object):

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
        db.supplement_argparser(parser)
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
        return parser


    def _build_file_list(self):

        query = db.sim_query_from_name_list(self.options.sims)
        files = []
        if self.options.latest:
            for x in query.all():
                try:
                    files.append(x.timesteps[-1])
                except IndexError:
                    pass
        else:
            files = db.core.internal_session.query(db.TimeStep).filter(
                db.TimeStep.simulation_id.in_([q.id for q in query.all()])). \
                order_by(db.TimeStep.time_gyr).all()

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
        parallel_tasks.mpi_sync_db(db.core.internal_session)
        return ma_files

    def parse_command_line(self, argv=None):
        parser = self._get_parser_obj()
        self.options = parser.parse_args(argv)
        db.process_options(self.options)
        self._build_file_list()


        if self.options.verbose:
            self.redirect = pynbody.util.ExecutionControl()

        self.classes = properties.providing_classes(self.options.properties[1:])

        self.timing_details = dict([(c, 0.0) for c in self.classes])


    def _build_halo_list(self, db_timestep):
        query = db.and_(
            db.Halo.timestep == db_timestep, db.or_(db.Halo.NDM > 1000, db.Halo.NDM == 0))
        if self.options.htype is not None:
            query = db.and_(query, db.Halo.halo_type == self.options.htype)

        halos = db.core.internal_session.query(db.Halo).filter(query).all()

        halos = halos[self.options.hmin:]

        if self.options.hmax is not None:
            halos = halos[:self.options.hmax]

        print
        heading(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()))
        heading("Processing %s: %d halos" % (repr(db_timestep), len(halos)))

        return halos


    def _build_existing_properties(self, db_halo):
        existing_properties = db_halo.properties
        existing_properties_data = AttributableDict(
                [(x.name.text, x.data) for x in existing_properties])
        existing_properties_data.update(
                [(x.relation.text, x.halo_to) for x in db_halo.links])
        existing_properties_data.halo_number = db_halo.halo_number
        existing_properties_data.NDM = db_halo.NDM
        existing_properties_data.NGas = db_halo.NGas
        existing_properties_data.NStar = db_halo.NStar
        existing_properties_data['halo_number'] = db_halo.halo_number
        return existing_properties_data

    def _build_existing_properties_all_halos(self, halos):
        return [self._build_existing_properties(h) for h in halos]


    def _perform_property_calculation(self, db_halo, property_calculator, existing_properties):
        pass

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
            print >>sys.stderr, term.GREEN, "Commit", term.NORMAL,
            sys.stderr.flush()
            insert_list(self._pending_properties)
            print >>sys.stderr, term.BLUE, "OK", term.NORMAL
            sys.stderr.flush()
            self._pending_properties = []
            self._start_time = time.time()
            summarise_timing(self.timing_details)

    def _queue_results_for_later_commit(self, db_halo, names, results, existing_properties_data):
        for n, r in zip(names, results):
            if isinstance(r, properties.ProxyHalo):
                # resolve halo
                r = db.core.internal_session.query(db.Halo).filter_by(
                    halo_number=int(r), timestep=db_halo.timestep).first()
            if self.options.force or (n not in existing_properties_data.keys()):
                existing_properties_data[n] = r
                if self.options.debug:
                    print term.WHITE, "Would create_property ", db_halo, n, r, term.NORMAL
                else:
                    self._pending_properties.append((db_halo, n, r))

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
            self._loaded_timestep.physical_units()
            self._run_preloop(self._loaded_timestep, db_timestep.filename,
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
            self._loaded_halo.physical_units()

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

        snapshot_data = self._get_halo_snapshot_data_if_appropriate(db_halo, property_calculator)

        property_calculator.start_timer()

        result = self._get_standin_property_value(property_calculator)

        try:
            with self.redirect:
                result = property_calculator.calculate(snapshot_data, db_data)
        except Exception, e:
            print>>sys.stderr, term.RED, ">> ERROR", type(
                e), e, term.NORMAL
            print>>sys.stderr, "=" * 60
            traceback.print_exc(file=sys.stderr)
            print>>sys.stderr, "-" * 60
            print>>sys.stderr, "=" * 60

            if self.options.catch:
                tbtype, value, tb = sys.exc_info()
                pdb.post_mortem(tb)

        self.timing_details[type(property_calculator)] += property_calculator.end_timer()
        print >>sys.stderr, term.BLUE + "C" + term.NORMAL,
        sys.stderr.flush()

        return result


    def _run_preloop(self, f, filename, cinstances, existing_properties_all_halos):
        for x in cinstances:
            try:
                print >> sys.stderr, term.BLUE + \
                                     "Pre" + term.NORMAL,
                sys.stderr.flush()
                with self.redirect:
                    x.preloop(f, filename,
                              existing_properties_all_halos)
            except RuntimeError, e:
                print >> sys.stderr, term.RED, ">> ERROR (in preloop)", term.YELLOW, e, term.NORMAL
                if catch:
                    traceback.print_exc()
                    tbtype, value, tb = sys.exc_info()
                    pdb.post_mortem(tb)
                norun.append(x)

    def run_property_calculation(self, db_halo, property_calculator, existing_properties):
        names = property_calculator.name()
        if type(names) is str:
            listize = True
            names = [names]
        else:
            listize = False

        if all([name in existing_properties.keys() for name in names]) and not self.options.force:
            print >>sys.stderr, term.YELLOW + "D" + term.NORMAL,
            sys.stderr.flush()
            return

        if not property_calculator.accept(existing_properties):
            print >>sys.stderr, term.YELLOW + "X" + term.NORMAL,
            sys.stderr.flush()
            return

        results = self._get_property_value(db_halo, property_calculator, existing_properties)

        if listize:
            results = [results]

        self._queue_results_for_later_commit(db_halo, names, results, existing_properties)

    def run_halo_calculation(self, db_halo, existing_properties):
        print >>sys.stderr, term.RED + "H%d"%db_halo.halo_number + term.NORMAL,
        sys.stderr.flush()

        for calculator in self._property_calculator_instances:
            self.run_property_calculation(db_halo, calculator, existing_properties)

        self._commit_results_if_needed()


    def run_timestep_calculation(self, db_timestep):
        self._property_calculator_instances = properties.instantiate_classes(self.options.properties[1:])
        db_halos = self._build_halo_list(db_timestep)
        self._existing_properties_all_halos = self._build_existing_properties_all_halos(db_halos)


        for db_halo, existing_properties in zip(db_halos, self._existing_properties_all_halos) :
            self._existing_properties_this_halo = existing_properties
            self.run_halo_calculation(db_halo, existing_properties)

        print >>sys.stderr, term.BLUE, "done", term.NORMAL,
        sys.stderr.flush()

        self._commit_results_if_needed(True)


    def run_calculation_loop(self):

        self._start_time = time.time()
        self._pending_properties = []

        for f_obj in self._get_parallel_timestep_iterator():
            self.run_timestep_calculation(f_obj)

        self._commit_results_if_needed(True,True)





if __name__ == "__main__":
    db.use_blocking_session()

    writer = DbWriter()
    writer.parse_command_line(sys.argv)
    writer.run_calculation_loop()
