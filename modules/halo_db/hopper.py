from . import core
import sqlalchemy.orm.dynamic, sqlalchemy.orm.query

class HopStrategy(object):
    def __init__(self, halo_from):
        if isinstance(halo_from, core.Halo):
            query = halo_from.links
        elif isinstance(halo_from, sqlalchemy.orm.query.Query):
            query = halo_from

        assert isinstance(query, sqlalchemy.orm.query.Query)
        self.query = query

    def target_timestep(self,ts):
        self.query = self.query.join("halo_to").filter(core.Halo.timestep_id==ts.id)

    def target_simulation(self, sim):
        self.query = self.query.join("halo_to","timestep").filter(core.TimeStep.simulation_id==sim.id)

    def count(self):
        return self.query.count()

    def all(self):
        return self.query.all()


class MultiHopStrategy(HopStrategy):
    def __init__(self, halo_from, nhops):
        super(MultiHopStrategy,self).__init__(halo_from)
        self.nhops = nhops
        for i in xrange(nhops-1):
            self.query = self.query.join("halo_to","links")