import core
import sqlalchemy, sqlalchemy.orm, sqlalchemy.orm.dynamic, sqlalchemy.orm.query, sqlalchemy.exc
from sqlalchemy.orm import Session, relationship
from sqlalchemy import and_

def _recursive_map_ids_to_objects(x, db_class, session):
    if hasattr(x,'__len__'):
        return [_recursive_map_ids_to_objects(x_i,db_class, session) for x_i in x]
    else:
        return session.query(db_class).filter_by(id=x).first()

class HopStrategy(object):
    """HopStrategy and its descendants define methods helpful for finding related halos, e.g. progenitors/descendants,
    or corresponding halos in other simulation runs"""

    def __init__(self, halo_from):
        """Construct a HopStrategy starting from the specified halo"""
        query = halo_from.links
        assert isinstance(halo_from, core.Halo)
        assert isinstance(query, sqlalchemy.orm.query.Query)
        self.halo_from = halo_from
        self._order_by_names = ['weight']
        self.query = query
        self._link_orm_class = core.HaloLink

    def target_timestep(self, ts):
        """Only return those hops which reach the specified timestep"""
        if ts is None:
            self.query = self.query.filter(0==1)
        else:
            self.query = self.query.join("halo_to").filter(core.Halo.timestep_id==ts.id)

    def target_simulation(self, sim):
        """Only return those hops which reach the specified simulation"""
        self.query = self.query.join("halo_to","timestep").filter(core.TimeStep.simulation_id==sim.id)

    def target(self, db_obj):
        """Only return those hops which reach the specifid simulation or timestep"""
        if isinstance(db_obj, core.TimeStep):
            self.target_timestep(db_obj)
        elif isinstance(db_obj, core.Simulation):
            self.target_simulation(db_obj)
        else:
            raise ValueError("Unknown target type")

    def order_by(self, *names):
        """Specify an ordering for the output hop suggestions.

        Accepted names are:

         - 'weight' - the weight of the link, ascending (default). In the case of MultiHopStrategy, this is the
                      product of the weights along the path found.
         - 'time_asc' - the time of the snapshot, ascending order
         - 'time_desc' - the time of the snapshot, descending order
         - 'nhops' - the number of hops taken to reach the halo (MultiHopStrategy only)

        Multiple names can be given to order by more than one property.
        """
        self._order_by_names = [x.lower() for x in names]

    def count(self):
        """Return the number of hops matching the conditions"""
        return self.query.count()

    def _get_query_all(self):
        try:
            results = self._query_ordered.all()
        except sqlalchemy.exc.ResourceClosedError:
            results = []

        return self._remove_duplicate_targets(results)

    def _remove_duplicate_targets(self, paths):
        weeded_paths = []
        existing_targets = set()
        for path in paths:
            if path.halo_to_id not in existing_targets:
                existing_targets.add(path.halo_to_id)
                weeded_paths.append(path)
        return weeded_paths

    def all(self):
        """Return all possible hops matching the conditions"""
        return [x.halo_to for x in self._get_query_all()]

    def weights(self):
        """Return the weights for the possible hops"""
        return [x.weight for x in self._get_query_all()]

    def all_and_weights(self):
        """Return all possible hops matching the conditions, along with
        the weights"""
        all = self._get_query_all()
        weights = [x.weight for x in all]
        halos = [x.halo_to for x in all]
        return halos, weights

    def first(self):
        """Return the suggested hop."""
        link_first = self._query_ordered.first()
        if link_first is None:
            return None
        else:
            return link_first.halo_to

    @property
    def _order_by(self):
        return [self._generate_order_arg_from_name(name) for name in self._order_by_names]

    def _generate_order_arg_from_name(self, name):
        if name=='weight':
            return self._link_orm_class.weight.desc()
        elif name=='time_asc':
            return core.TimeStep.time_gyr
        elif name=='time_desc':
            return core.TimeStep.time_gyr.desc()
        else:
            raise ValueError, "Unknown ordering method %r"%name

    @property
    def _query_ordered(self):
        query = self.query
        assert isinstance(query, sqlalchemy.orm.query.Query)
        if 'time_asc' in self._order_by_names or 'time_desc' in self._order_by_names:
            query = query.join(core.Halo, self._link_orm_class.halo_to).join(core.TimeStep)

        query= query.order_by(*self._order_by)
        return query


class HopMajorDescendantStrategy(HopStrategy):
    """A hop strategy that suggests the major descendant for a halo"""
    def __init__(self, halo_from):
        super(HopMajorDescendantStrategy,self).__init__(halo_from)
        self.target_timestep(halo_from.timestep.next)

class HopMajorProgenitorStrategy(HopStrategy):
    """A hop strategy that suggests the major progenitor for a halo"""
    def __init__(self, halo_from):
        super(HopMajorProgenitorStrategy,self).__init__(halo_from)
        self.target_timestep(halo_from.timestep.previous)

class MultiHopStrategy(HopStrategy):
    """An extension of the HopStrategy class that takes multiple hops across
    HaloLinks, up to a specified maximum, before finding the target halo."""

    def __init__(self, halo_from, nhops_max, directed=None):
        super(MultiHopStrategy,self).__init__(halo_from)
        self.nhops_max = nhops_max
        self.directed = directed
        self.session =  Session.object_session(halo_from)

        self._generate_halolink_recurse_cte()
        self._generate_recursion_subquery()
        self._apply_recursion_subquery_filter()
        self._generate_query()

    def link_ids(self):
        """Return the links for the possible hops, in the form of a list of HaloLink IDs for
        each path"""
        return [[int(y) for y in x.links.split(",")] for x in self._query_ordered.all()]

    def node_ids(self):
        """Return the nodes, i.e. halo IDs visited, for each path"""
        return [[int(y) for y in x.nodes.split(",")] for x in self._query_ordered.all()]

    def nodes(self):
        return _recursive_map_ids_to_objects(self.node_ids(),core.Halo,self.session)

    def links(self):
        return _recursive_map_ids_to_objects(self.link_ids(),core.HaloLink,self.session)

    def _generate_recursion_subquery(self):
        links = self.halolink_recurse_alias.c.links +\
                                  sqlalchemy.literal(",") +\
                                  sqlalchemy.cast(core.HaloLink.id,sqlalchemy.String)

        nodes = self.halolink_recurse_alias.c.nodes +\
                                  sqlalchemy.literal(",") +\
                                  sqlalchemy.cast(core.HaloLink.halo_to_id,sqlalchemy.String)

        self.recursion_query = \
            self.session.query(core.HaloLink.id,
                               core.HaloLink.halo_from_id,
                               core.HaloLink.halo_to_id,
                               (self.halolink_recurse_alias.c.weight * core.HaloLink.weight).label("weight"),
                               (self.halolink_recurse_alias.c.nhops + 1).label("nhops"),
                               links, nodes)


    def _generate_halolink_recurse_cte(self):
        links = sqlalchemy.cast(core.HaloLink.id,sqlalchemy.String).label("links")
        nodes = (sqlalchemy.cast(core.HaloLink.halo_from_id, sqlalchemy.String) + \
                sqlalchemy.literal(",") + \
                sqlalchemy.cast(core.HaloLink.halo_to_id, sqlalchemy.String)).label("nodes")

        startpoint_filter = core.HaloLink.halo_from_id == self.halo_from.id

        startpoint_query = self.session.query(
                                         core.HaloLink.id,
                                         core.HaloLink.halo_from_id,
                                         core.HaloLink.halo_to_id,
                                         core.HaloLink.weight,
                                         sqlalchemy.literal(0).label("nhops"),
                                         links, nodes).filter(startpoint_filter)

        startpoint_query = self._supplement_halolink_query_with_filter(startpoint_query)

        halolink_recurse = startpoint_query.cte(name="halolink_recurse", recursive=True)
        halolink_recurse_alias = sqlalchemy.orm.aliased(halolink_recurse,name="halolink_recurse_old")
        self.halolink_recurse = halolink_recurse
        self.halolink_recurse_alias = halolink_recurse_alias



    def _apply_recursion_subquery_filter(self):

        recursion_filter = and_(core.HaloLink.halo_from_id == self.halolink_recurse_alias.c.halo_to_id,
                                self.halolink_recurse_alias.c.nhops < self.nhops_max)

        self.recursion_query = self._supplement_halolink_query_with_filter(self.recursion_query).filter(recursion_filter)


    def _supplement_halolink_query_with_filter(self, query):
        if self._needs_link_filter():
            halo_old = sqlalchemy.orm.aliased(core.Halo,name="halo_old")
            halo_new = sqlalchemy.orm.aliased(core.Halo,name="halo_new")
            timestep_old = sqlalchemy.orm.aliased(core.TimeStep,name="timestep_old")
            timestep_new = sqlalchemy.orm.aliased(core.TimeStep,name="timestep_new")

            filter = self._generate_link_filter(timestep_old, timestep_new)

            query = query. \
                join(halo_old, core.HaloLink.halo_from). \
                join(halo_new, core.HaloLink.halo_to). \
                join(timestep_old, halo_old.timestep). \
                join(timestep_new, halo_new.timestep). \
                filter(filter)


        return query

    def _needs_link_filter(self):
        return self.directed is not None

    def _generate_link_filter(self, timestep_old, timestep_new):
        if self.directed is None:
            return None

        directed = self.directed.lower()
        if directed == 'backwards':
            recursion_filter = timestep_new.time_gyr < timestep_old.time_gyr
        elif directed == 'forwards':
            recursion_filter = timestep_new.time_gyr > timestep_old.time_gyr
        elif directed == 'across':
            recursion_filter = sqlalchemy.func.abs(timestep_new.time_gyr-timestep_old.time_gyr)<1e-4
        else:
            raise ValueError, "Unknown direction %r"%directed

        return recursion_filter



    def _generate_query(self):
        halolink_recurse = self.halolink_recurse.union(self.recursion_query)

        class MultiHopHaloLink(core.Base):
            __table__ = halolink_recurse
            halo_from = relationship(core.Halo, primaryjoin=halolink_recurse.c.halo_from_id == core.Halo.id)
            halo_to = relationship(core.Halo, primaryjoin=(halolink_recurse.c.halo_to_id == core.Halo.id))

        self._link_orm_class = MultiHopHaloLink

        self.query = self.session.query(MultiHopHaloLink)

    def _generate_order_arg_from_name(self, name):
        if name=='nhops':
            return self._link_orm_class.c.nhops
        else:
            return super(MultiHopStrategy,self)._generate_order_arg_from_name(name)



