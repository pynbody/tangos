import core
import sqlalchemy, sqlalchemy.orm, sqlalchemy.orm.dynamic, sqlalchemy.orm.query, sqlalchemy.exc
from sqlalchemy.orm import Session, relationship
from sqlalchemy import and_, Table, Column, String, Integer, Float, MetaData, ForeignKey, Index
import string
import random
import contextlib
import sys

from . import consistent_collection
import time
import logging
import pyparsing as pp

NHOPS_MAX_DEFAULT = 100

def _recursive_map_ids_to_objects(x, db_class, session):
    if hasattr(x, '__len__'):
        return [_recursive_map_ids_to_objects(x_i, db_class, session) for x_i in x]
    else:
        return session.query(db_class).filter_by(id=x).first()


class HopStrategy(object):
    """HopStrategy and its descendants define methods helpful for finding related halos, e.g. progenitors/descendants,
    or corresponding halos in other simulation runs"""

    def __init__(self, halo_from, target=None, order_by=None):
        """Construct a HopStrategy starting from the specified halo"""
        query = halo_from.links
        assert isinstance(halo_from, core.halo.Halo)
        assert isinstance(query, sqlalchemy.orm.query.Query)
        self.session = Session.object_session(halo_from)
        self.halo_from = halo_from
        self._initialise_order_by(order_by)
        self.query = query
        self._link_orm_class = core.halo_data.HaloLink
        self._target = target
        self._all = None

    def _target_timestep(self, ts):
        """Only return those hops which reach the specified timestep"""
        if ts is None:
            self.query = self.query.filter(0 == 1)
        else:
            self.query = self.query.join("halo_to").filter(core.halo.Halo.timestep_id == ts.id)

    def _target_simulation(self, sim):
        """Only return those hops which reach the specified simulation"""
        self.query = self.query.join("halo_to", "timestep").filter(
            core.timestep.TimeStep.simulation_id == sim.id)

    def _filter_query_for_target(self, db_obj):
        """Only return those hops which reach the specifid simulation or timestep"""
        if db_obj is None:
            return
        elif isinstance(db_obj, core.timestep.TimeStep):
            self._target_timestep(db_obj)
        elif isinstance(db_obj, core.simulation.Simulation):
            self._target_simulation(db_obj)
        else:
            raise ValueError("Unknown target type")

    def _initialise_order_by(self, names):
        """Specify an ordering for the output hop suggestions.

        Accepted names are:

         - 'weight' - the weight of the link, ascending (default). In the case of MultiHopStrategy, this is the
                      product of the weights along the path found.
         - 'time_asc' - the time of the snapshot, ascending order
         - 'time_desc' - the time of the snapshot, descending order
         - 'halo_number_asc' - the halo number, ascending
         - 'halo_number_desc' - the halo number, descending
         - 'nhops' - the number of hops taken to reach the halo (MultiHopStrategy only)

        Multiple names can be given to order by more than one property.
        """
        if names is None:
            names = ['weight']
        elif isinstance(names, str):
            names = [names]
        self._order_by_names = [x.lower() for x in names]

    def count(self):
        """Return the number of hops matching the conditions"""
        return len(self._get_query_all())

    def _execute_query(self):
        try:
            self._filter_query_for_target(self._target)
            results = self._query_ordered.all()
        except sqlalchemy.exc.ResourceClosedError:
            results = []

        results = filter(lambda x: x is not None, results)

        self._all = results

    def _get_query_all(self):
        if self._all is None:
            self._execute_query()
        return self._all

    def temp_table(self):
        import temporary_halolist
        # N.B. this could be made more efficient
        ids_list = [x.id if hasattr(x,'id') else None for x in self.all() ]
        return temporary_halolist.temporary_halolist_table(self.session, ids_list)

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

    def all_weights_and_routes(self):
        """Return all possible hops matching the conditions, along with
        the weights and routes"""
        all = self._get_query_all()
        weights = [x.weight for x in all]
        halos = [x.halo_to for x in all]
        routes = [x.nodes for x in all]
        return halos, weights, routes

    def first(self):
        """Return the suggested hop."""
        link = self._get_query_all()
        if len(link) == 0:
            return None
        else:
            return link[0].halo_to

    def _order_by_clause(self, halo_alias, timestep_alias):
        return [self._generate_order_arg_from_name(name, halo_alias, timestep_alias) for name in self._order_by_names]

    def _generate_order_arg_from_name(self, name, halo_alias, timestep_alias):
        if name == 'weight':
            return self._link_orm_class.weight.desc()
        elif name == 'time_asc':
            return timestep_alias.time_gyr
        elif name == 'time_desc':
            return timestep_alias.time_gyr.desc()
        elif name == 'halo_number_asc':
            return halo_alias.halo_number
        elif name == 'halo_number_desc':
            return halo_alias.halo_number.desc()
        else:
            raise ValueError, "Unknown ordering method %r" % name

    def _ordering_requires_join(self):
        return 'time_asc' in self._order_by_names \
               or 'time_desc' in self._order_by_names \
               or 'halo_number_asc' in self._order_by_names \
               or 'halo_number_desc' in self._order_by_names

    @property
    def _query_ordered(self):
        query = self.query
        assert isinstance(query, sqlalchemy.orm.query.Query)
        timestep_alias = None
        halo_alias = None
        if self._ordering_requires_join():
            timestep_alias = sqlalchemy.orm.aliased(core.timestep.TimeStep)
            halo_alias = sqlalchemy.orm.aliased(core.halo.Halo)
            query = query.join(halo_alias, self._link_orm_class.halo_to).join(timestep_alias)

        query = query.order_by(*self._order_by_clause(halo_alias, timestep_alias))
        return query


class HopMajorDescendantStrategy(HopStrategy):
    """A hop strategy that suggests the major descendant for a halo"""

    def __init__(self, halo_from):
        target_ts = halo_from.timestep.next
        if target_ts:
            super(HopMajorDescendantStrategy, self).__init__(halo_from, target=target_ts)
        else:
            self._all = []


class HopMajorProgenitorStrategy(HopStrategy):
    """A hop strategy that suggests the major progenitor for a halo"""

    def __init__(self, halo_from):
        target_ts = halo_from.timestep.previous
        if target_ts:
            super(HopMajorProgenitorStrategy, self).__init__(halo_from, target=target_ts)
        else:
            self._all = []


class MultiHopStrategy(HopStrategy):
    """An extension of the HopStrategy class that takes multiple hops across
    HaloLinks, up to a specified maximum, before finding the target halo."""

    halo_old = sqlalchemy.orm.aliased(core.halo.Halo, name="halo_old")
    halo_new = sqlalchemy.orm.aliased(core.halo.Halo, name="halo_new")
    timestep_old = sqlalchemy.orm.aliased(core.timestep.TimeStep, name="timestep_old")
    timestep_new = sqlalchemy.orm.aliased(core.timestep.TimeStep, name="timestep_new")

    def __init__(self, halo_from, nhops_max=NHOPS_MAX_DEFAULT, directed=None, target=None,
                 order_by=None, combine_routes=True, min_aggregated_weight=0.0,
                 min_onehop_weight=0.0, store_full_paths=False,
                 include_startpoint=False):
        """Construct the MultiHopStrategy (without actually executing the strategy)

        :param halo_from:   The halo to start hopping from
        :type halo_from:    core.Halo

        :param nhops_max:   The maximum number of hops to take

        :param directed:    The direction in which to step, which can be
              'backwards' - only take hop if it's backwards in time
              'forwards'  - only take hop if it's forwards in time
              'across'    - only take hop if it's to the same time
              None        - take all available hops

              Note that specifying a direction causes a slight slow-down in each hop because the hop results have
              to be joined to the TimeStep object to work out the direction. But the overall query times are
              often much shorter because the pruning cuts down the number of paths to be explored.

        :param target:      Only return results in target, which can be a TimeStep or Simulation object.
            If None, return all results. Intermediate hops are not restricted by target, only the results returned.

        :param order_by:    Return results in specified order; see HopStrategy for more information

        :param combine_routes:  If True (default), discard multiple routes to the same halo and keep only the
            strongest route. When False, one result is returned for each possible route,
            even if it ultimately reaches the same halo. This can result in a significant
            slow-down.

        :param min_aggregated_weight: Threshold cumulative weight below which routes will be discarded.
              The cumulative weight is defined as the product of the individual hop weights.
              Small values indicate that a small fraction of mass is in common.

        :param min_onehop_weight:     Threshold individual weight below which a route will be discarded.

        :param store_full_paths:      Store full path of hops for debug purposes (default False; can slow down the
              calculation significantly)

        :param include_startpoint:    Return the starting halo in the results (default False)
        """
        super(MultiHopStrategy, self).__init__(halo_from, target, order_by)
        self.nhops_max = nhops_max
        self.directed = directed
        self._min_aggregated_weight = min_aggregated_weight
        self._min_onehop_weight = min_onehop_weight
        self._store_full_paths = store_full_paths
        self._include_startpoint = include_startpoint
        self._connection = self.session.connection()
        self._combine_routes = combine_routes

    def temp_table(self):
        """Execute the strategy and return results as a temp_table (see temporary_halolist module)"""

        import temporary_halolist
        tt = self._manage_temp_table()
        tt.__enter__()
        try:
            self._prepare_query()
        except:
            tt.__exit__(*sys.exc_info())
            raise
        thl = temporary_halolist.temporary_halolist_table(self.session,
                                                          self._query_ordered.from_self(
                                                              self._link_orm_class.halo_to_id),
                                                          callback=lambda: tt.__exit__(None, None, None)
                                                          )
        return thl

    def _prepare_query(self):
        self._generate_query()
        self._filter_query_for_target(self._target)
        self._seed_temp_table()
        self._make_hops()

    def _execute_query(self):
        with self._manage_temp_table():
            self._prepare_query()
            try:
                results = self._query_ordered.all()
            except sqlalchemy.exc.ResourceClosedError:
                results = []

        self._all = results

    def link_ids(self):
        """Return the links for the possible hops, in the form of a list of HaloLink IDs for
        each path"""
        raise NotImplementedError
        return [[int(y) for y in x.links.split(",")] for x in self._get_query_all()]

    def node_ids(self):
        """Return the nodes, i.e. halo IDs visited, for each path"""
        raise NotImplementedError
        return [[int(y) for y in x.nodes.split(",")] for x in self._get_query_all()]

    def nodes(self):
        return _recursive_map_ids_to_objects(self.node_ids(), core.halo.Halo, self.session)

    def links(self):
        return _recursive_map_ids_to_objects(self.link_ids(), core.halo_data.HaloLink, self.session)

    def _supplement_halolink_query_with_filter(self, query, table=None):

        if table is None:
            table = core.halo_data.HaloLink.__table__

        if self._needs_join_for_link_filter():
            query = query. \
                join(self.halo_old, table.c.halo_from_id == self.halo_old.id). \
                join(self.halo_new, table.c.halo_to_id == self.halo_new.id). \
                join(self.timestep_old, self.halo_old.timestep). \
                join(self.timestep_new, self.halo_new.timestep)

        filter = self._generate_link_filter(self.timestep_old, self.timestep_new, table)
        query = query.filter(filter)

        return query

    def _needs_join_for_link_filter(self):
        return self.directed is not None

    def _generate_link_filter(self, timestep_old, timestep_new, table):

        recursion_filter = table.c.weight > self._min_aggregated_weight

        if self.directed is not None:
            directed = self.directed.lower()
            if directed == 'backwards':
                recursion_filter &= timestep_new.time_gyr < timestep_old.time_gyr
            elif directed == 'forwards':
                recursion_filter &= timestep_new.time_gyr > timestep_old.time_gyr
            elif directed == 'across':
                recursion_filter &= sqlalchemy.func.abs(timestep_new.time_gyr - timestep_old.time_gyr) < 1e-4
            else:
                raise ValueError, "Unknown direction %r" % directed

        return recursion_filter

    def _delete_temp_table(self):
        self._table.drop(checkfirst=True, bind=self._connection)
        self._prelim_table.drop(checkfirst=True, bind=self._connection)
        # self._index.drop(bind=self.connection)
        core.Base.metadata.remove(self._table)
        core.Base.metadata.remove(self._prelim_table)

    def _create_temp_table(self):
        rstr = ''.join(random.choice(string.ascii_lowercase) for _ in range(4))

        multi_hop_link_table = Table(
            'multihoplink_final_' + rstr,
            core.Base.metadata,
            Column('id', Integer, primary_key=True),
            Column('source_id', Integer),
            Column('halo_from_id', Integer, ForeignKey('halos.id')),
            Column('halo_to_id', Integer, ForeignKey('halos.id')),
            Column('weight', Float),
            Column('nhops', Integer),
            Column('links', String),
            Column('nodes', String),
            prefixes=['TEMPORARY']
        )

        multi_hop_link_prelim_table = Table(
            'multihoplink_prelim_' + rstr,
            core.Base.metadata,
            Column('id', Integer, primary_key=True),
            Column('source_id', Integer),
            Column('halo_from_id', Integer, ForeignKey('halos.id')),
            Column('halo_to_id', Integer, ForeignKey('halos.id')),
            Column('weight', Float),
            Column('nhops', Integer),
            Column('links', String),
            Column('nodes', String),
            prefixes=['TEMPORARY']
        )

        self._table = multi_hop_link_table
        self._prelim_table = multi_hop_link_prelim_table
        self._table.create(checkfirst=True, bind=self._connection)

        self._prelim_table.create(checkfirst=True, bind=self._connection)

    @contextlib.contextmanager
    def _manage_temp_table(self):
        self._create_temp_table()
        yield
        self._delete_temp_table()

    def _seed_temp_table(self):
        insert_statement = self._table.insert().values(halo_from_id=self.halo_from.id, halo_to_id=self.halo_from.id,
                                    weight=1.0, nhops=0, links="", nodes=str(self.halo_from.id))

        self._connection.execute(insert_statement)

    def _make_hops(self):
        for i in xrange(0, self.nhops_max):
            generated_count = self._generate_next_level_prelim_links(i)
            if generated_count == 0:
                break
            filtered_count = self._filter_prelim_links_into_final()
            if filtered_count == 0:
                break
        self._nhops_taken = i # stored for debug/testing purposes

    def _generate_next_level_prelim_links(self, from_nhops=0):

        if self._store_full_paths:
            links = self._table.c.links + \
                    sqlalchemy.literal(",") + \
                    sqlalchemy.cast(core.halo_data.HaloLink.id, sqlalchemy.String)

            nodes = self._table.c.nodes + \
                    sqlalchemy.literal(",") + \
                    sqlalchemy.cast(core.halo_data.HaloLink.halo_to_id, sqlalchemy.String)

            links = sqlalchemy.literal("(") + links + sqlalchemy.literal(")")
            nodes = sqlalchemy.literal("(") + nodes + sqlalchemy.literal(")")

            if self._combine_routes:
                links = sqlalchemy.func.group_concat(links, "+")
                nodes = sqlalchemy.func.group_concat(nodes, "+")
        else:
            links = sqlalchemy.literal("")
            nodes = sqlalchemy.literal("")

        new_weight = self._table.c.weight * core.halo_data.HaloLink.weight

        if self._combine_routes:
            new_weight = sqlalchemy.func.max(new_weight)

        recursion_query = \
            self.session.query(
                core.halo_data.HaloLink.halo_from_id,
                core.halo_data.HaloLink.halo_to_id.label("halo_to_id"),
                new_weight,
                (self._table.c.nhops + sqlalchemy.literal(1)).label("nhops"),
                links, nodes, self._table.c.source_id). \
                select_from(self._table). \
                join(core.halo_data.HaloLink, and_(self._table.c.nhops == from_nhops,
                                                           self._table.c.halo_to_id == core.halo_data.HaloLink.halo_from_id)). \
                filter(core.halo_data.HaloLink.weight > self._min_onehop_weight
                       )

        if self._combine_routes:
            recursion_query = recursion_query.group_by(core.halo_data.HaloLink.halo_to_id, self._table.c.source_id)

        insert = self._prelim_table.insert().from_select(
            ['halo_from_id', 'halo_to_id', 'weight', 'nhops', 'links', 'nodes', 'source_id'],
            recursion_query)

        result = self._connection.execute(insert)

        return result.rowcount

    def _filter_prelim_links_into_final(self):

        q = self.session.query(self._prelim_table.c.halo_from_id,
                               self._prelim_table.c.halo_to_id,
                               self._prelim_table.c.weight,
                               self._prelim_table.c.nhops,
                               self._prelim_table.c.links,
                               self._prelim_table.c.nodes,
                               self._prelim_table.c.source_id)

        q = self._supplement_halolink_query_with_filter(q, self._prelim_table)

        added_rows = self._connection.execute(
            self._table.insert().from_select(['halo_from_id', 'halo_to_id', 'weight', 'nhops', 'links', 'nodes', 'source_id'],
                                             q)).rowcount

        self._connection.execute(self._prelim_table.delete())
        return added_rows

    def _generate_query(self):

        class MultiHopHaloLink(core.Base):
            __table__ = self._table
            halo_from = relationship(core.halo.Halo, primaryjoin=self._table.c.halo_from_id == core.halo.Halo.id)
            halo_to = relationship(core.halo.Halo, primaryjoin=(self._table.c.halo_to_id == core.halo.Halo.id))

        self._link_orm_class = MultiHopHaloLink

        self.query = self.session.query(MultiHopHaloLink)

        if not self._include_startpoint:
            self.query = self.query.filter(self._table.c.nhops>0)

    def _generate_order_arg_from_name(self, name, halo_alias, timestep_alias):
        if name == 'nhops':
            return self._link_orm_class.c.nhops
        else:
            return super(MultiHopStrategy, self)._generate_order_arg_from_name(name, halo_alias, timestep_alias)

class MultiSourceMultiHopStrategy(MultiHopStrategy):
    """A variant of MultiHopStrategy that finds halos corresponding to multiple start points.

    Note that the behaviour is necessarily somewhat different to the other classes which start from a single
    halo. Specifically, a target *must* be specified, and the direction of the hops to follow is inferred
    from the nature of the target.

    Additionally, as soon as any halo is "matched" in the target, the entire query is stopped. In other words,
    this class assumes that the number of hops is the same to reach all target halos.

    In terms of implementation, the class could be significantly optimised in future. While the all() function
    only retains the highest-weight final halos, this weeding could be done earlier - at the point of making the
    underlying query (to save the ORM mapping) or even during the hops."""

    def __init__(self, halos_from, target, **kwargs):
        directed = self._infer_direction(halos_from, target)
        kwargs["target"] = target
        kwargs["directed"] = directed
        super(MultiSourceMultiHopStrategy, self).__init__(halos_from[0], **kwargs)
        self._all_halo_from = halos_from

    def _infer_direction(self, halos_from, target):
        if isinstance(target, core.simulation.Simulation):
            return "across"
        elif isinstance(target, core.timestep.TimeStep):
            collected_halos = consistent_collection.ConsistentCollection(halos_from)
            if collected_halos.timestep.simulation_id!=target.simulation_id:
                return "across"
            elif collected_halos.timestep.time_gyr<target.time_gyr:
                return "forwards"
            else:
                return "backwards"


    def _seed_temp_table(self):
        for i,halo_from in enumerate(self._all_halo_from):
            insert_statement = self._table.insert().values(halo_from_id=halo_from.id, halo_to_id=halo_from.id,
                                        weight=1.0, nhops=0, links="", nodes=str(halo_from.id), source_id=i)
            self._connection.execute(insert_statement)

    def _generate_next_level_prelim_links(self, from_nhops=0):
        if self._should_halt():
            return 0
        else:
            return super(MultiSourceMultiHopStrategy, self)._generate_next_level_prelim_links(from_nhops)

    def _should_halt(self):
        return self.query.count()>0

    def _order_by_clause(self, halo_alias, timestep_alias):
        return [self._link_orm_class.source_id, self._table.c.weight]

    def all(self):
        all = self._get_query_all()
        source_ids = [x.source_id for x in self._all]
        num_sources = len(self._all_halo_from)
        matches = dict([(source, destination.halo_to) for source,destination in zip(source_ids, all)])
        return [matches.get(i,None) for i in xrange(num_sources)]

    def temp_table(self):
        # because of the custom manipulation performed above, the MultiHopStrategy implementation of
        # temp_table does not return the correct results. Ideally, we should fix this by implementing
        # the custom manipulation in all() inside the actual query instead. For now, the fix is to
        # use the clunky "get everything then put it back into the database as a new temp table" approach.
        return HopStrategy.temp_table(self)

    def _execute_query(self):
        super(MultiSourceMultiHopStrategy, self)._execute_query()






class MultiHopAllProgenitorsStrategy(MultiHopStrategy):
    """A hop strategy that suggests all progenitors for a halo at every step"""
    def __init__(self, halo_from, nhops_max=NHOPS_MAX_DEFAULT, include_startpoint=False, target='auto', combine_routes=True):
        self.sim_id = halo_from.timestep.simulation_id
        if target=='auto':
            target = halo_from.timestep.simulation
        super(MultiHopAllProgenitorsStrategy, self).__init__(halo_from, nhops_max,
                                                               directed='backwards',
                                                               include_startpoint=include_startpoint,
                                                               target=target,
                                                               order_by=['time_desc', 'halo_number_asc'],
                                                               combine_routes=combine_routes)

    def _supplement_halolink_query_with_filter(self, query, table):
        query = super(MultiHopAllProgenitorsStrategy, self)._supplement_halolink_query_with_filter(query, table)
        if self._target is None:
            return query
        else:
            return query.filter(self.timestep_new.simulation_id == self.sim_id)


class MultiHopMajorProgenitorsStrategy(MultiHopAllProgenitorsStrategy):
    """A hop strategy that suggests the major progenitor for a halo at every step"""

    def _supplement_halolink_query_with_filter(self, query, table):
        query = super(MultiHopMajorProgenitorsStrategy, self)._supplement_halolink_query_with_filter(query, table)
        return query.order_by(self.timestep_new.time_gyr.desc(), table.c.weight.desc(), self.halo_new.halo_number). \
            limit(1)


class MultiHopMajorDescendantsStrategy(MultiHopStrategy):
    """A hop strategy that suggests the major descendant for a halo at every step"""

    def __init__(self, halo_from, nhops_max=NHOPS_MAX_DEFAULT, include_startpoint=False):
        self.sim_id = halo_from.timestep.simulation_id
        super(MultiHopMajorDescendantsStrategy, self).__init__(halo_from, nhops_max,
                                                               directed='forwards',
                                                               include_startpoint=include_startpoint,
                                                               target=halo_from.timestep.simulation)

    def _supplement_halolink_query_with_filter(self, query, table):
        query = super(MultiHopMajorDescendantsStrategy, self)._supplement_halolink_query_with_filter(query, table)
        return query.filter(self.timestep_new.simulation_id == self.sim_id). \
            order_by(self.timestep_new.time_gyr, table.c.weight.desc(), self.halo_new.halo_number). \
            limit(1)


