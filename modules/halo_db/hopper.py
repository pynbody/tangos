from . import core
import sqlalchemy, sqlalchemy.orm, sqlalchemy.orm.dynamic, sqlalchemy.orm.query
from sqlalchemy.orm import Session, relationship
from sqlalchemy import and_


class HopStrategy(object):
    def __init__(self, halo_from):
        query = halo_from.links
        assert isinstance(halo_from, core.Halo)
        assert isinstance(query, sqlalchemy.orm.query.Query)
        self.query = query.order_by(core.HaloLink.weight.desc())
        self.halo_from = halo_from


    def target_timestep(self,ts):
        if ts is None:
            self.query = self.query.filter(0==1)
        else:
            self.query = self.query.join("halo_to",aliased=True).filter(core.Halo.timestep_id==ts.id)

    def target_simulation(self, sim):
        self.query = self.query.join("halo_to","timestep",aliased=True).filter(core.TimeStep.simulation_id==sim.id)

    def count(self):
        return self.query.count()

    def all(self):
        return [x.halo_to for x in self.query.all()]

    def weights(self):
        return [x.weight for x in self.query.all()]

    def first(self):
        link_first = self.query.first()
        if link_first is None:
            return None
        else:
            return link_first.halo_to


class HopMajorDescendantStrategy(HopStrategy):
    def __init__(self, halo_from):
        super(HopMajorDescendantStrategy,self).__init__(halo_from)
        self.target_timestep(halo_from.timestep.next)

class HopMajorProgenitorStrategy(HopStrategy):
    def __init__(self, halo_from):
        super(HopMajorProgenitorStrategy,self).__init__(halo_from)
        self.target_timestep(halo_from.timestep.previous)





class MultiHopStrategy(HopStrategy):
    def __init__(self, halo_from, nhops_max, directed=None):
        super(MultiHopStrategy,self).__init__(halo_from)
        self.nhops_max = nhops_max
        session =  Session.object_session(halo_from)
        halolink_recurse = session.query(core.HaloLink.id,
                                         core.HaloLink.halo_from_id,
                                         core.HaloLink.halo_to_id,
                                         core.HaloLink.weight,
                                         sqlalchemy.literal(0).label("nhops")).\
            filter(core.HaloLink.halo_from_id==halo_from.id).\
            cte(name="halolink_recurse", recursive=True)

        halolink_recurse_alias = sqlalchemy.orm.aliased(halolink_recurse)

        halo_old = sqlalchemy.orm.aliased(core.Halo)
        halo_new = sqlalchemy.orm.aliased(core.Halo)
        timestep_old = sqlalchemy.orm.aliased(core.TimeStep)
        timestep_new = sqlalchemy.orm.aliased(core.TimeStep)

        recursion_query = session.query(core.HaloLink.id,
                core.HaloLink.halo_from_id,
                core.HaloLink.halo_to_id,
                (halolink_recurse_alias.c.weight*core.HaloLink.weight).label("weight"),
                (halolink_recurse_alias.c.nhops+1).label("nhops")
            )

        recursion_filter = and_(core.HaloLink.halo_from_id==halolink_recurse_alias.c.halo_to_id,
                                     halolink_recurse_alias.c.nhops<nhops_max)

        if directed is not None:
            recursion_query = recursion_query.\
                join(halo_old,core.HaloLink.halo_from).\
                join(halo_new,core.HaloLink.halo_to).\
                join(timestep_old,halo_old.timestep).\
                join(timestep_new,halo_new.timestep)

            if directed.lower()=='backwards':
                recursion_filter = and_(recursion_filter, timestep_new.time_gyr<timestep_old.time_gyr)
            elif directed.lower()=='forwards':
                recursion_filter = and_(recursion_filter, timestep_new.time_gyr>timestep_old.time_gyr)
            elif directed.lower()=='across':
                recursion_filter = and_(recursion_filter, timestep_new.time_gyr==timestep_old.time_gyr)


        halolink_recurse = halolink_recurse.union(recursion_query.filter(recursion_filter))

        class MultiHopHaloLink(core.Base):
            __table__ = halolink_recurse
            halo_from = relationship(core.Halo, primaryjoin=halolink_recurse.c.halo_from_id == core.Halo.id)
            halo_to = relationship(core.Halo, primaryjoin=(halolink_recurse.c.halo_to_id == core.Halo.id))

        self.query = session.query(MultiHopHaloLink).order_by(MultiHopHaloLink.weight.desc())




"""with recursive halolink_recurse(halo_from_id,halo_to_id,nhops) as (
 values(340,110,0)
 union all
 select halolink.halo_from_id, halolink.halo_to_id, halolink_recurse.nhops+1 from halolink, halolink_recurse
 where halolink_recurse.halo_to_id = halolink.halo_from_id and halolink_recurse.nhops<10
) select * from halolink_recurse;"""