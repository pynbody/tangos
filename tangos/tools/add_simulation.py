import numbers

import numpy as np

from .. import config, core
from .. import parallel_tasks as pt
from ..core import Simulation, TimeStep
from ..log import logger


class SimulationAdderUpdater:
    """This class contains the necessary tools to add a new simulation to the database"""

    def __init__(self, simulation_output, renumber=True):
        """:type simulation_output tangos.simulation_outputs.HandlerBase"""
        self.simulation_output = simulation_output

        # if running in parallel, creating a session for the first time may trigger a race condition e.g. if the
        # database doesn't exist yet
        with pt.ExclusiveLock("creating_database"):
            core.get_default_session()

        self.min_halo_particles = config.min_halo_particles
        self.max_num_objects = config.max_num_objects
        self.renumber = renumber

        self.parallel = pt.parallelism_is_active()


    @property
    def session(self):
        return core.get_default_session()

    @property
    def basename(self):
        return self.simulation_output.basename

    def scan_simulation_and_add_all_descendants(self):
        if self.parallel:
            assert pt.parallelism_is_active(), "Parallel backend has not been initialized"
            from ..parallel_tasks import database
            database.synchronize_creator_object()
            create_simulation = pt.backend.rank()==1 # nb rank 0 is busy coordinating everything
        else:
            create_simulation = True

        if create_simulation:
            if not self.simulation_exists():
                logger.info("Add new simulation %r", self.basename)
                logger.info("... using the output handler %r", self.simulation_output.handler_class_name())
                self.add_simulation()
            else:
                logger.warning("Simulation already exists %r", self.basename)

            self.add_simulation_properties()

        # await the simulation being ready if we are running in parallel
        pt.barrier()

        for ts_filename in self.simulation_output.enumerate_timestep_extensions(parallel=self.parallel):
            if not self.timestep_exists_for_extension(ts_filename):
                ts = self.add_timestep(ts_filename)
                self.add_timestep_properties(ts)
                self.add_objects_to_timestep(ts, core.halo.Halo)
                self.add_objects_to_timestep(ts, core.halo.Group)
            else:
                logger.warning("Timestep already exists %r", ts_filename)

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
        with pt.ExclusiveLock("db_write_lock"):
            self.session.add(ex)
            self.session.commit()
        return ex

    def add_simulation(self):
        sim = Simulation(self.basename)
        # no need for a lock here - only one process should be adding a simulation
        self.session.add(sim)
        self.session.commit()

    def add_simulation_properties(self):
        sim = self._get_simulation()
        properties_dict = self.simulation_output.get_properties()
        properties_dict['handler'] = self.simulation_output.handler_class_name()
        for k, v in properties_dict.items():
            if isinstance(v, numbers.Number) and np.isnan(v):
                continue
            if k not in list(sim.keys()):
                logger.info("Add simulation property %r",k)
                sim[k] = v
            elif sim[k]!=v:
                logger.info("Update simulation property %r", k)
                sim[k] = v
            else:
                logger.warning("Simulation property %r already exists", k)

        self.session.commit()

    @staticmethod
    def _autoadd_zeros(enumerate_fn):
        # enable enumerate functions to just return halo numbers, adding on the number of dark matter, star and
        # gas particles for backwards compatibility
        def adapted(*args, **kwargs):
            for result in enumerate_fn(*args, **kwargs):
                if not hasattr(result, "__len__"):
                    yield result, result, 0, 0, 0
                else:
                    yield result
        return adapted

    def add_objects_to_timestep(self, ts, create_class=core.halo.Halo):
        halos = []
        n_tot = []
        enumerator = self._autoadd_zeros(self.simulation_output.enumerate_objects)

        for catalog_id, finder_id, NDM, Nstar, Ngas in enumerator(ts.extension, object_typetag=create_class.tag,
                                                      min_halo_particles=self.min_halo_particles):
            n_tot.append(NDM+Nstar+Ngas)

        if self.renumber:
            database_id = np.zeros(len(n_tot), dtype=int)

            # Sort by total particle number, largest objects first. Use mergesort for sort stability.
            database_id[np.argsort(-np.array(n_tot),kind='mergesort')] = np.arange(len(n_tot)) + 1
        else:
            database_id = [None]*len(n_tot)

        for database_number, (catalog_id, finder_id, NDM, Nstar, Ngas) in zip(database_id,
                                                                 enumerator(ts.extension, object_typetag=create_class.tag,
                                                                            min_halo_particles=self.min_halo_particles)):
            if database_number is None:
                database_number = catalog_id

            if (NDM+Nstar+Ngas >= self.min_halo_particles or NDM==0) \
                    and (self.max_num_objects is None or database_number<=self.max_num_objects ):
                h = create_class(ts, database_number, finder_id, catalog_id, NDM, Nstar, Ngas)
                halos.append(h)

        with pt.ExclusiveLock("db_write_lock"):
            logger.info("Add %d %ss to timestep %r", len(halos), create_class.__name__, ts)
            self.session.add_all(halos)
            self.session.commit()

    def add_timestep_properties(self, ts):
        for key, value in self.simulation_output.get_timestep_properties(ts.extension).items():
            setattr(ts, key, value)



    def _get_simulation(self):
        return self.session.query(Simulation).filter_by(basename=self.basename).first()
