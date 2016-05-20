import glob
import sys
import traceback
import os, os.path
from .. import config, core, halo_stat_files
from ..core import Simulation, TimeStep, get_or_create_dictionary_item, SimulationProperty, Halo
from ..log import logger

class SimulationAdder(object):
    """This class inspects the raw output from a simulation and creates the entries to represent it in the database.

    Subclasses implement specialisations for different formats and situations.

    The main method to call is scan_simulation_and_add_all_descendants, which insepcts, adds and/or updates information
    to match the database against the on-disk version of the simulation. """

    def __init__(self, basename, session=None):
        if session is None:
            session = core.get_default_session()
        self.session = session
        self.basename = strip_slashes(basename)

    def scan_simulation_and_add_all_descendants(self):
        if not self.simulation_exists():
            self.add_simulation()
            self.add_simulation_properties()
        else:
            logger.warn("Simulation already exists %s", self.basename)

        for ts_filename in self.enumerate_timestep_extensions():
            if not self.timestep_exists_for_extension(ts_filename):
                ts = self.add_timestep(ts_filename)
                self.add_timestep_properties(ts)
                self.add_halos_to_timestep(ts)

    def simulation_exists(self):
        num_matches = self.session.query(Simulation).filter_by(basename=self.basename).count()
        assert num_matches<2, "Consistency problem - more than one simulation with this name exists"
        return num_matches>0

    def timestep_exists_for_extension(self, ts_extension):
        ex = core.get_default_session().query(TimeStep).filter_by(
            simulation=self._get_simulation(),
            extension=ts_extension).first()
        return ex is not None

    def _convert_timestep_filename_to_extension(self, ts_filename):
        return strip_slashes(ts_filename[len(self.basename):])

    def add_timestep(self, ts_extension):
        ex = TimeStep(self._get_simulation(), ts_extension)
        return self.session.merge(ex)

    def add_simulation(self):
        sim = Simulation(self.basename)
        self.session.add(sim)
        self.session.commit()

    def add_simulation_properties(self):
        sim = self._get_simulation()
        for k, v in self.enumerate_properties():
            logger.info("Add simulation property %r -> %r",k,v)
            x = SimulationProperty(sim, k, v)
            self.session.add(x)
        self.session.commit()

    def add_halos_to_timestep(self, ts, min_NDM=1000):
        for num, NDM, Nstar, Ngas in self.enumerate_halos(ts):
            halos = []
            if NDM > min_NDM:
                h = core.halo.Halo(ts, num, NDM, Nstar, Ngas)
                halos.append(h)
        self.session.add_all(halos)
        self.session.commit()

    def enumerate_timestep_extensions(self):
        raise NotImplementedError

    def enumerate_properties(self):
        raise NotImplementedError

    def add_timestep_properties(self, ts):
        raise NotImplementedError

    def _get_simulation(self):
        return self.session.query(Simulation).filter_by(basename=self.basename).first()

class TestSimulationAdder(SimulationAdder):
    def enumerate_properties(self):
        with open(os.path.join(config.base, self.basename, "sim_info"),'r') as f:
            for line in f:
                yield line.split()

    def enumerate_timestep_extensions(self):
        pre_extension_length = len(os.path.join(config.base, self.basename))
        steps = glob.glob(os.path.join(config.base, self.basename, "step.*"))
        for i in steps:
            print i, i[pre_extension_length:], strip_slashes(i[pre_extension_length:])
            yield strip_slashes(i[pre_extension_length:])

    def add_timestep_properties(self, ts):
        ts.time_gyr = self._get_ts_property(ts, 'time')
        ts.redshift = self._get_ts_property(ts, 'redshift')

    def enumerate_halos(self, ts):
        nhalos = int(self._get_ts_property(ts, 'halos'))
        for i in xrange(nhalos):
            yield i+1, 2000, 0, 0

    def _get_ts_property(self, ts, property):
        with open(ts.filename, 'r') as f:
            for line in f:
                line_split = line.split()
                if line_split[0].lower() == property.lower():
                    return " ".join(line_split[1:])


class PynbodySimulationAdder(SimulationAdder):
    def add_timestep_properties(self, ts):
        import pynbody
        f = pynbody.load(ts.filename)
        ts.redshift = f.properties['z']
        try:
            ts.time_gyr = f.properties['time'].in_units("Gyr")
        except:
            ts.time_gyr = -1
        ts.available = True

    def enumerate_halos(self, ts):
        try:
            statfile = halo_stat_files.HaloStatFile(ts)
            logger.info("Reading halos for timestep %r using a stat file",ts)
            for X in statfile.iter_rows("n_dm", "n_star", "n_gas"):
                yield X
        except IOError:
            logger.warn("No halo statistics file found for timestep %r",ts)
            logger.warn("Reading halos directly using pynbody")
            import pynbody
            f = ts.load()
            h = f.halos()
            if hasattr(h, 'precalculate'):
                h.precalculate()
            if type(h)==pynbody.halo.RockstarIntermediateCatalogue:
                istart = 0
            else:
                istart = 1

            for i in xrange(istart, len(h)-istart):
                try:
                    hi = h[i]
                    if (not ts.halos.filter_by(halo_type=0, halo_number=i).count()>0) and len(hi.dm) > 1000:
                        yield i, len(hi.dm), len(hi.star), len(hi.gas)
                except (ValueError, KeyError) as e:
                    pass






def add_simulation_timesteps_gadget(basename, reassess=False):

    steps = set(glob.glob(config.base+"/"+basename+"/snapshot_???"))
    print steps
    sim = core.get_default_session().query(Simulation).filter_by(
        basename=basename).first()
    if sim is None:
        sim = Simulation(basename)
        core.get_default_session().add(sim)
        logger.info("Add gadget simulation %s", sim)
    else:
        logger.warn("Simulation already exists %s", sim)
    core.get_default_session().commit()
    steps_existing = set([ttt.relative_filename for ttt in sim.timesteps])
    add_ts = []

    for s in steps.union(steps_existing):
        ex = core.get_default_session().query(TimeStep).filter_by(
            simulation=sim, extension=strip_slashes(s[len(basename):])).first()
        if ex != None:
            logger.warn("Timestep already exists %s", ex)
        else:
            ex = TimeStep(sim, strip_slashes(s[len(basename):]))
            logger.info("Add timestep %s", ex)
            add_ts.append(ex)

    core.get_default_session().add_all(add_ts)
    for ts in add_ts:
        add_halos(ts)
    core.get_default_session().commit()


def add_simulation_timesteps_ramses(basename, reassess=False):
    from halo_db.tools.terminalcontroller import term

    outputs = glob.glob(config.base+"/"+basename + "/output_00*")
    sim = core.get_default_session().query(Simulation).filter_by(
        basename=basename).first()
    if sim is None:
        sim = Simulation(basename)
        core.get_default_session().add(sim)
        print term.RED + "Add ramses simulation:", sim, term.NORMAL
    else:
        print term.GREEN + "Simulation exists:", sim, term.NORMAL

    core.get_default_session().commit()

    steps = set(glob.glob(basename + "/output_00???"))
    steps_existing = set([ttt.relative_filename for ttt in sim.timesteps])

    add_ts = []

    for s in steps.union(steps_existing):
        ex = core.get_default_session().query(TimeStep).filter_by(
            simulation=sim, extension=strip_slashes(s[len(basename):])).first()
        if ex != None:
            print term.GREEN, "Timestep exists: ", ex, term.NORMAL
        else:
            ex = TimeStep(sim, strip_slashes(s[len(basename):]))
            print term.RED, "Add timestep:", ex, term.NORMAL
            add_ts.append(ex)

    core.get_default_session().add_all(add_ts)
    core.get_default_session().commit()

    prop_dict = {}

    nmls = glob.glob(config.base + basename + "/*.nml")
    if len(nmls) > 1:
        print "Too many nmls - ignoring"
    elif len(nmls) == 0:
        print "No nmls"
    else:
        f = open(nmls[0])
        store_next = None
        for l in f:
            if store_next is not None:
                x = l.split("=")[1].split(",")
                x = map(lambda y: float(y.strip()), x)
                for k, v in zip(store_next, x):
                    if k != "_":
                        prop_dict[k] = v
                store_next = None
            if l.startswith("!halo_db:"):
                store_next = map(str.strip, l[9:].split(","))

    for k in prop_dict:

        dict_k = get_or_create_dictionary_item(core.get_default_session(), k)

        x = sim.properties.filter_by(name=dict_k).first()
        if x is not None:
            print term.GREEN, "Simulation property exists: ", x, term.NORMAL
        else:
            x = SimulationProperty(sim, dict_k, prop_dict[k])
            print term.RED, "Create simulation property: ", x, term.NORMAL
            core.get_default_session().add(x)

    core.get_default_session().commit()





def strip_slashes(name):
    """Strip trailing and leading slashes from relative path"""
    if len(name)==0: return name
    while name[0] == "/":
        name = name[1:]
        if len(name)==0: return name
    while name[-1] == "/":
        name = name[:-1]
        if len(name)==0: return name
    return name


def add_simulation_timesteps(options):
    reassess=False
    basename = strip_slashes(options.sim)

    if len(glob.glob(config.base + basename + "/output_00*")) > 0:
        add_simulation_timesteps_ramses(basename, reassess)
        return

    if len(glob.glob(config.base + basename + "/snapshot_???"))>0:
        add_simulation_timesteps_gadget(basename, reassess)
        return

    import magic_amiga
    from halo_db.tools.terminalcontroller import term, heading
    import pynbody
    import time

    flags_include = ["dPhysDenMin", "dCStar", "dTempMax",
                     "dESN", "bLowTCool", "bSelfShield", "dExtraCoolShutoff"]
    # check_extensions = ["amiga.grp", "iord"]
    # ["HI","amiga.grp","HeI","HeII","coolontime","iord"]
    check_extensions = []

    steps = magic_amiga.find(None, basename=config.base+"/"+basename + "/", ignore=[])
    if len(steps)==0:
        raise IOError, "Can't find any simulation timesteps"

    # check whether simulation exists
    sim = core.get_default_session().query(Simulation).filter_by(
        basename=basename).first()
    heading(basename)

    full_basename = config.base+"/"+basename

    try:
        pfile = magic_amiga.get_param_file(full_basename + "/")
        logger.info("Param file = %s", pfile)
        pfile_dict = magic_amiga.param_file_to_dict(pfile)
        prop_dict = {}
        log_fn = pfile_dict["achOutName"] + ".log"
        log_path = pfile.split("/")[:-1]
        log_path.append(log_fn)
        log_path = "/".join(log_path)
        logger.info("Log file = %s", log_path)
    except RuntimeError:
        logger.warn("No .param file found for simulation %s",full_basename)
        pfile_dict = {}
        prop_dict = {}
        log_path = "None"

    try:
        if sim == None:
            sim = Simulation(basename)

            core.get_default_session().add(sim)
            print term.RED + "Add simulation: ", sim, term.NORMAL
        else:
            print term.GREEN + "Simulation exists: ", sim, term.NORMAL

        steps_existing = set([ttt.filename for ttt in sim.timesteps])

        for f in flags_include:
            if pfile_dict.has_key(f):
                prop_dict[f] = pfile_dict[f]

        try:
            f = file(log_path)
            for l in f:
                if "# Code compiled:" in l:
                    prop_dict["compiled"] = time.strptime(
                        l.split(": ")[1].strip(), "%b %d %Y %H:%M:%S")
                if "# Preprocessor macros: " in l:
                    prop_dict["macros"] = l.split(": ")[1].strip()
                    break
        except IOError:
            print term.BLUE, "Warning: No log file " + log_path, term.NORMAL
            pass

        for k in prop_dict:

            dict_k = get_or_create_dictionary_item(core.get_default_session(), k)

            x = sim.properties.filter_by(name=dict_k).first()
            if x is not None:
                print term.GREEN, "Simulation property exists: ", x, term.NORMAL
            else:
                print dict_k, prop_dict[k]
                x = SimulationProperty(sim, dict_k, prop_dict[k])
                print term.RED, "Create simulation property: ", x, term.NORMAL
                core.get_default_session().add(x)

        for s in steps.union(steps_existing):
            problem = False
            ex = core.get_default_session().query(TimeStep).filter_by(
                simulation=sim, extension=strip_slashes(s[len(full_basename):])).first()
            if ex != None:
                print term.GREEN, "Timestep exists: ", ex, term.NORMAL
                if reassess:

                    try:
                        f = pynbody.load(ex.filename, maxlevel=2)
                        length = len(f)
                        if abs(ex.time_gyr-pynbody.analysis.cosmology.age(f))>0.001:
                            ex.time_gyr = pynbody.analysis.cosmology.age(f)
                            print term.YELLOW, " Update: ", ex, term.NORMAL
                    except (IOError, RuntimeError):
                        problem = True
                        if not ex.available:
                            print term.YELLOW, " Problem loading", ex.filename, " - marking unavailable", term.NORMAL
                            ex.available = False

                    for ext in check_extensions:
                        try:
                            f = file(ex.filename + "." + ext)
                            ext_line = f.readline()
                        except IOError:
                            print term.YELLOW, " Extension " + ext + " missing or unreadable for file " + ex.filename + " - marking unavailable", term.NORMAL
                            ex.available = False
                            problem = True
                            break
                        if ext == "amiga.grp" and int(ext_line) != length:
                            print term.YELLOW, " Alyson's script has produced something of the wrong length for file " + ex.filename + "- marking unavailable", term.NORMAL
                            ex.available = False
                            problem = True
                            break

                    if not problem and not ex.available:
                        print term.YELLOW, " Simulation seems OK, marking as available", term.NORMAL
                        ex.available = True

                    if ex.halos.count()<2:
                        add_halos(ex)

            else:
                ex = None
                try:
                    ex = TimeStep(sim, strip_slashes(s[len(full_basename):]))
                    print term.RED, "Add timestep: ", ex, term.NORMAL

                    core.get_default_session().add(ex)
                    print ex
                    try:
                        add_halos(ex)
                        core.get_default_session().commit()
                    except Exception, e:
                        print term.RED, "ERROR", term.NORMAL, "while trying to add halos"
                        exc_type, exc_value, exc_traceback = sys.exc_info()
                        print e
                        traceback.print_tb(exc_traceback)
                        core.get_default_session().rollback()



                except IOError, e:
                    print e
                    print term.BLUE, "Couldn't load timestep requested for adding", s, term.NORMAL
                    if ex != None:
                        core.get_default_session().delete(ex)

    except:
        core.get_default_session().rollback()
        raise

    core.get_default_session().commit()