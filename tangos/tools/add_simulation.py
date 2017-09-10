from .. import core
from ..core import Simulation, TimeStep
from ..log import logger

class SimulationAdderUpdater(object):
    """This class contains the necessary tools to add a new simulation to the database"""

    def __init__(self, simulation_output, session=None):
        """:type simulation_output tangos.simulation_outputs.SimulationOutputSetHandler"""
        self.simulation_output = simulation_output
        if session is None:
            session = core.get_default_session()
        self.session = session

    @property
    def basename(self):
        return self.simulation_output.basename

    def scan_simulation_and_add_all_descendants(self):
        if not self.simulation_exists():
            logger.info("Add new simulation %r", self.basename)
            self.add_simulation()
        else:
            logger.warn("Simulation already exists %r", self.basename)

        self.add_simulation_properties()

        for ts_filename in self.simulation_output.enumerate_timestep_extensions():
            if not self.timestep_exists_for_extension(ts_filename):
                ts = self.add_timestep(ts_filename)
                self.add_timestep_properties(ts)
                self.add_halos_to_timestep(ts)
            else:
                logger.warn("Timestep already exists %r", ts_filename)

    def simulation_exists(self):
        num_matches = self.session.query(Simulation).filter_by(basename=self.basename).count()
        assert num_matches<2, "Consistency problem - more than one simulation with this name exists"
        return num_matches>0

    def timestep_exists_for_extension(self, ts_extension):
        ex = core.get_default_session().query(TimeStep).filter_by(
            simulation=self._get_simulation(),
            extension=ts_extension).first()
        return ex is not None

    def add_timestep(self, ts_extension):
        logger.info("Add timestep %s to simulation %s",ts_extension,self.basename)
        ex = TimeStep(self._get_simulation(), ts_extension)
        return self.session.merge(ex)

    def add_simulation(self):
        sim = Simulation(self.basename)
        self.session.add(sim)
        self.session.commit()

    def add_simulation_properties(self):
        sim = self._get_simulation()
        properties_dict = self.simulation_output.get_properties()
        properties_dict['handler'] = self.simulation_output.handler_class_name()
        for k, v in properties_dict.iteritems():
            if k not in sim.keys():
                logger.info("Add simulation property %r",k)
                sim[k] = v
            elif sim[k]!=v:
                logger.info("Update simulation property %r", k)
                sim[k] = v
            else:
                logger.warn("Simulation property %r already exists", k)

        self.session.commit()

    def add_halos_to_timestep(self, ts, min_NDM=1000):
        halos = []
        import numpy as np
        n_tot = []
        for num, NDM, Nstar, Ngas in self.simulation_output.enumerate_halos(ts.extension):
            n_tot.append(NDM+Nstar+Ngas)
        ids = np.zeros(len(n_tot), dtype=int)
        ids[np.argsort(np.array(n_tot))[::-1]] = np.arange(len(n_tot)) + 1
        cnt = 1
        for num, NDM, Nstar, Ngas in self.simulation_output.enumerate_halos(ts.extension):
            if NDM > min_NDM:
                h = core.halo.Halo(ts, ids[cnt-1], cnt, NDM, Nstar, Ngas)
                halos.append(h)
            cnt += 1
        logger.info("Add %d halos to timestep %r", len(halos),ts)
        self.session.add_all(halos)
        self.session.commit()

    def add_timestep_properties(self, ts):
        for key, value in self.simulation_output.get_timestep_properties(ts.extension).iteritems():
            setattr(ts, key, value)



    def _get_simulation(self):
        return self.session.query(Simulation).filter_by(basename=self.basename).first()


