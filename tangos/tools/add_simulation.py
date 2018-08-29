from __future__ import absolute_import
from .. import core, config
from ..core import Simulation, TimeStep
from ..log import logger
import six
import numpy as np

class SimulationAdderUpdater(object):
    """This class contains the necessary tools to add a new simulation to the database"""

    def __init__(self, simulation_output, session=None, renumber=True):
        """:type simulation_output tangos.simulation_outputs.HandlerBase"""
        self.simulation_output = simulation_output
        if session is None:
            session = core.get_default_session()
        self.session = session
        self.min_halo_particles = config.min_halo_particles
        self.max_num_objects = config.max_num_objects
        self.renumber = renumber

    @property
    def basename(self):
        return self.simulation_output.basename

    def scan_simulation_and_add_all_descendants(self):
        if not self.simulation_exists():
            logger.info("Add new simulation %r", self.basename)
            logger.info("... using the output handler %r", self.simulation_output.handler_class_name())
            self.add_simulation()
        else:
            logger.warn("Simulation already exists %r", self.basename)

        self.add_simulation_properties()

        for ts_filename in self.simulation_output.enumerate_timestep_extensions():
            if not self.timestep_exists_for_extension(ts_filename):
                ts = self.add_timestep(ts_filename)
                self.add_timestep_properties(ts)
                self.add_objects_to_timestep(ts, core.halo.Halo)
                self.add_objects_to_timestep(ts, core.halo.Group)
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
        logger.info("Add timestep %r to simulation %r",ts_extension,self.basename)
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
        for k, v in six.iteritems(properties_dict):
            if k not in list(sim.keys()):
                logger.info("Add simulation property %r",k)
                sim[k] = v
            elif sim[k]!=v:
                logger.info("Update simulation property %r", k)
                sim[k] = v
            else:
                logger.warn("Simulation property %r already exists", k)

        self.session.commit()

    @staticmethod
    def _autoadd_zeros(enumerate_fn):
        # enable enumerate functions to just return halo numbers, adding on the number of dark matter, star and
        # gas particles for backwards compatibility
        def adapted(*args, **kwargs):
            for result in enumerate_fn(*args, **kwargs):
                if not hasattr(result, "__len__"):
                    yield result, 0, 0, 0
                else:
                    yield result
        return adapted

    def add_objects_to_timestep(self, ts, create_class=core.halo.Halo):
        halos = []
        n_tot = []
        enumerator = self._autoadd_zeros(self.simulation_output.enumerate_objects)

        for finder_id, NDM, Nstar, Ngas in enumerator(ts.extension, object_typetag=create_class.tag,
                                                      min_halo_particles=self.min_halo_particles):
            n_tot.append(NDM+Nstar+Ngas)

        if self.renumber:
            database_id = np.zeros(len(n_tot), dtype=int)

            # Sort by total particle number, largest objects first. Use mergesort for sort stability.
            database_id[np.argsort(-np.array(n_tot),kind='mergesort')] = np.arange(len(n_tot)) + 1
        else:
            database_id = [None]*len(n_tot)

        for database_number,(finder_id, NDM, Nstar, Ngas) in zip(database_id,
                                                                 enumerator(ts.extension, object_typetag=create_class.tag,
                                                                            min_halo_particles=self.min_halo_particles)):
            if database_number is None:
                database_number = finder_id

            if (NDM >= self.min_halo_particles or NDM==0) \
                    and (self.max_num_objects is None or database_number<=self.max_num_objects ):
                h = create_class(ts, database_number, finder_id, NDM, Nstar, Ngas)
                halos.append(h)

        logger.info("Add %d %ss to timestep %r", len(halos), create_class.__name__, ts)
        self.session.add_all(halos)
        self.session.commit()

    def add_timestep_properties(self, ts):
        for key, value in six.iteritems(self.simulation_output.get_timestep_properties(ts.extension)):
            setattr(ts, key, value)



    def _get_simulation(self):
        return self.session.query(Simulation).filter_by(basename=self.basename).first()


