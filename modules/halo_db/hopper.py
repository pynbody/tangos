import core
import sqlalchemy, sqlalchemy.orm, sqlalchemy.orm.dynamic, sqlalchemy.orm.query, sqlalchemy.exc
from sqlalchemy.orm import Session, relationship
from sqlalchemy import and_, Table, Column, String, Integer, Float, MetaData, ForeignKey, Index
import string
import random
import contextlib
import time
import logging
import pyparsing as pp


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
        assert isinstance(halo_from, core.Halo)
        assert isinstance(query, sqlalchemy.orm.query.Query)
        self.session = Session.object_session(halo_from)
        self.halo_from = halo_from
        self._initialise_order_by(order_by)
        self.query = query
        self._link_orm_class = core.HaloLink
        self._target = target
        self._all = None

    def _target_timestep(self, ts):
        """Only return those hops which reach the specified timestep"""
        if ts is None:
            self.query = self.query.filter(0 == 1)
        else:
            self.query = self.query.join("halo_to").filter(core.Halo.timestep_id == ts.id)

    def _target_simulation(self, sim):
        """Only return those hops which reach the specified simulation"""
        self.query = self.query.join("halo_to", "timestep").filter(core.TimeStep.simulation_id == sim.id)

    def _filter_query_for_target(self, db_obj):
        """Only return those hops which reach the specifid simulation or timestep"""
        if db_obj is None:
            return
        elif isinstance(db_obj, core.TimeStep):
            self._target_timestep(db_obj)
        elif isinstance(db_obj, core.Simulation):
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
        return temporary_halolist.temporary_halolist_table(self.session, [x.id for x in self.all()])

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

    @property
    def _order_by_clause(self):
        return [self._generate_order_arg_from_name(name) for name in self._order_by_names]

    def _generate_order_arg_from_name(self, name):
        if name == 'weight':
            return self._link_orm_class.weight.desc()
        elif name == 'time_asc':
            return core.TimeStep.time_gyr
        elif name == 'time_desc':
            return core.TimeStep.time_gyr.desc()
        else:
            raise ValueError, "Unknown ordering method %r" % name

    @property
    def _query_ordered(self):
        query = self.query
        assert isinstance(query, sqlalchemy.orm.query.Query)
        if 'time_asc' in self._order_by_names or 'time_desc' in self._order_by_names:
            query = query.join(core.Halo, self._link_orm_class.halo_to).join(core.TimeStep)

        query = query.order_by(*self._order_by_clause)
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

    halo_old = sqlalchemy.orm.aliased(core.Halo, name="halo_old")
    halo_new = sqlalchemy.orm.aliased(core.Halo, name="halo_new")
    timestep_old = sqlalchemy.orm.aliased(core.TimeStep, name="timestep_old")
    timestep_new = sqlalchemy.orm.aliased(core.TimeStep, name="timestep_new")

    def __init__(self, halo_from, nhops_max, directed=None, target=None,
                 order_by=None, combine_routes=True, min_aggregated_weight=0.0,
                 min_onehop_weight=0.0, store_full_paths=False):
        super(MultiHopStrategy, self).__init__(halo_from, target, order_by)
        self.nhops_max = nhops_max
        self.directed = directed
        self._min_aggregated_weight = min_aggregated_weight
        self._min_onehop_weight = min_onehop_weight
        self._store_full_paths = store_full_paths
        assert isinstance(self.session, Session)
        self.connection = self.session.connection()
        self._combine_routes = combine_routes

    def temp_table(self):
        import temporary_halolist
        tt = self._manage_temp_table()
        tt.__enter__()
        try:
            self._prepare_query()
        except:
            tt.__exit__()
            raise
        thl = temporary_halolist.temporary_halolist_table(self.session,
                                                          self._query_ordered.from_self(
                                                              self._link_orm_class.halo_to_id),
                                                          callback=lambda: tt.__exit__(None, None, None)
                                                          )
        return thl

    def _prepare_query(self):
        self._seed_temp_table()
        self._make_hops()
        self._generate_query()
        self._filter_query_for_target(self._target)

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
        return _recursive_map_ids_to_objects(self.node_ids(), core.Halo, self.session)

    def links(self):

        return _recursive_map_ids_to_objects(self.link_ids(), core.HaloLink, self.session)

    def _supplement_halolink_query_with_filter(self, query, table=None):

        if table is None:
            table = core.HaloLink.__table__

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
        self._table.drop(checkfirst=True, bind=self.connection)
        self._prelim_table.drop(checkfirst=True, bind=self.connection)
        # self._index.drop(bind=self.connection)
        core.Base.metadata.remove(self._table)
        core.Base.metadata.remove(self._prelim_table)

        """
        print
        print "END"
        print
        logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
        """

    def _create_temp_table(self):

        """
        logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
        print
        print "START", self.directed
        print
        """

        rstr = ''.join(random.choice(string.ascii_lowercase) for _ in range(4))

        multi_hop_link_table = Table(
            'multihoplink_final_' + rstr,
            core.Base.metadata,
            Column('id', Integer, primary_key=True),
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
        self._table.create(checkfirst=True, bind=self.connection)

        self._prelim_table.create(checkfirst=True, bind=self.connection)

    @contextlib.contextmanager
    def _manage_temp_table(self):
        self._create_temp_table()
        yield
        self._delete_temp_table()

    def _seed_temp_table(self):
        links = sqlalchemy.cast(core.HaloLink.id, sqlalchemy.String)
        nodes = (sqlalchemy.cast(core.HaloLink.halo_from_id, sqlalchemy.String) + \
                 sqlalchemy.literal(",") + \
                 sqlalchemy.cast(core.HaloLink.halo_to_id, sqlalchemy.String))

        startpoint_filter = core.HaloLink.halo_from_id == self.halo_from.id

        startpoint_query = self.session.query(
            core.HaloLink.halo_from_id,
            core.HaloLink.halo_to_id,
            core.HaloLink.weight,
            sqlalchemy.literal(0),
            links,
            nodes).filter(startpoint_filter)

        startpoint_query = self._supplement_halolink_query_with_filter(startpoint_query)

        self.connection.execute(
            self._table.insert().from_select(['halo_from_id', 'halo_to_id', 'weight', 'nhops', 'links', 'nodes'],
                                             startpoint_query))

    def _make_hops(self):
        for i in xrange(1, self.nhops_max):
            generated_count = self._generate_next_level_prelim_links(i - 1)
            if generated_count == 0:
                break
            filtered_count = self._filter_prelim_links_into_final()

    def _generate_next_level_prelim_links(self, from_nhops=0):

        if self._store_full_paths:
            links = self._table.c.links + \
                    sqlalchemy.literal(",") + \
                    sqlalchemy.cast(core.HaloLink.id, sqlalchemy.String)

            nodes = self._table.c.nodes + \
                    sqlalchemy.literal(",") + \
                    sqlalchemy.cast(core.HaloLink.halo_to_id, sqlalchemy.String)

            links = sqlalchemy.literal("(") + links + sqlalchemy.literal(")")
            nodes = sqlalchemy.literal("(") + nodes + sqlalchemy.literal(")")

            if self._combine_routes:
                links = sqlalchemy.func.group_concat(links, "+")
                nodes = sqlalchemy.func.group_concat(nodes, "+")
        else:
            links = sqlalchemy.literal("")
            nodes = sqlalchemy.literal("")

        new_weight = self._table.c.weight * core.HaloLink.weight

        if self._combine_routes:
            new_weight = sqlalchemy.func.max(new_weight)

        recursion_query = \
            self.session.query(
                core.HaloLink.halo_from_id,
                core.HaloLink.halo_to_id.label("halo_to_id"),
                new_weight,
                (self._table.c.nhops + sqlalchemy.literal(1)).label("nhops"),
                links, nodes). \
                select_from(self._table). \
                join(core.HaloLink, and_(self._table.c.nhops == from_nhops,
                                         self._table.c.halo_to_id == core.HaloLink.halo_from_id)). \
                filter(core.HaloLink.weight > self._min_onehop_weight
                       )

        if self._combine_routes:
            recursion_query = recursion_query.group_by(core.HaloLink.halo_to_id)

        ct = recursion_query.count()

        insert = self._prelim_table.insert().from_select(
            ['halo_from_id', 'halo_to_id', 'weight', 'nhops', 'links', 'nodes'],
            recursion_query)

        result = self.connection.execute(insert)

        return result.rowcount

    def _filter_prelim_links_into_final(self):

        q = self.session.query(self._prelim_table.c.halo_from_id,
                               self._prelim_table.c.halo_to_id,
                               self._prelim_table.c.weight,
                               self._prelim_table.c.nhops,
                               self._prelim_table.c.links,
                               self._prelim_table.c.nodes)

        q = self._supplement_halolink_query_with_filter(q, self._prelim_table)

        added_rows = self.connection.execute(
            self._table.insert().from_select(['halo_from_id', 'halo_to_id', 'weight', 'nhops', 'links', 'nodes'],
                                             q)).rowcount

        self.connection.execute(self._prelim_table.delete())
        return added_rows

    def _generate_query(self):

        class MultiHopHaloLink(core.Base):
            __table__ = self._table
            halo_from = relationship(core.Halo, primaryjoin=self._table.c.halo_from_id == core.Halo.id)
            halo_to = relationship(core.Halo, primaryjoin=(self._table.c.halo_to_id == core.Halo.id))

        self._link_orm_class = MultiHopHaloLink

        self.query = self.session.query(MultiHopHaloLink)

    def _generate_order_arg_from_name(self, name):
        if name == 'nhops':
            return self._link_orm_class.c.nhops
        else:
            return super(MultiHopStrategy, self)._generate_order_arg_from_name(name)


class MultiHopMajorProgenitorsStrategy(MultiHopStrategy):
    """A hop strategy that suggests the major progenitor for a halo at every step"""

    def __init__(self, halo_from, nhops_max=100):
        self.sim_id = halo_from.timestep.simulation_id
        super(MultiHopMajorProgenitorsStrategy, self).__init__(halo_from, nhops_max,
                                                               directed='backwards')

    def _supplement_halolink_query_with_filter(self, query, table=None):
        query = super(MultiHopMajorProgenitorsStrategy, self)._supplement_halolink_query_with_filter(query, table)
        return query.filter(self.timestep_new.simulation_id == self.sim_id). \
            order_by(self.timestep_new.time_gyr.desc(), self.halo_new.halo_number). \
            limit(1)


class MultiHopMajorDescendantsStrategy(MultiHopStrategy):
    """A hop strategy that suggests the major descendant for a halo at every step"""

    def __init__(self, halo_from, nhops_max=100):
        self.sim_id = halo_from.timestep.simulation_id
        super(MultiHopMajorDescendantsStrategy, self).__init__(halo_from, nhops_max,
                                                               directed='forwards')

    def _supplement_halolink_query_with_filter(self, query, table=None):
        query = super(MultiHopMajorDescendantsStrategy, self)._supplement_halolink_query_with_filter(query, table)
        return query.filter(self.timestep_new.simulation_id == self.sim_id). \
            order_by(self.timestep_new.time_gyr, self.halo_new.halo_number). \
            limit(1)
