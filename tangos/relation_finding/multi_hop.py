import contextlib
import random
import string
import sys

import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.orm
import sqlalchemy.orm.dynamic
import sqlalchemy.orm.query
from sqlalchemy import Column, Index, Integer, Table, and_
from sqlalchemy.orm import defer, relationship

from .. import config, core, temporary_halolist
from ..config import DOUBLE_PRECISION
from ..log import logger
from ..util.timing_monitor import TimingMonitor
from .one_hop import HopStrategy


class MultiHopStrategy(HopStrategy):
    """An extension of the HopStrategy class that takes multiple hops across
    HaloLinks, up to a specified maximum, before finding the target halo."""

    halo_old = sqlalchemy.orm.aliased(core.halo.SimulationObjectBase, name="halo_old")
    halo_new = sqlalchemy.orm.aliased(core.halo.SimulationObjectBase, name="halo_new")
    timestep_old = sqlalchemy.orm.aliased(core.timestep.TimeStep, name="timestep_old")
    timestep_new = sqlalchemy.orm.aliased(core.timestep.TimeStep, name="timestep_new")

    def __init__(self, halo_from, nhops_max=None, directed=None, target=None,
                 order_by=None, combine_routes=True, min_aggregated_weight=0.0,
                 min_onehop_weight=0.0, min_onehop_reverse_weight=None,
                 include_startpoint=False, one_simulation=None):
        """Construct a strategy for finding Halos via multiple "hops" along HaloLinks

        :param halo_from:   The halo to start hopping from
        :type halo_from:    core.SimulationObjectBase

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

        :param one_simulation:  Only follow links to objects in the same simulation, if True. If False, follow
            links to any simulation. By default this is None, and the behaviour is dictated by directed: if
            directed is 'backwards' or 'forwards' one_simulation is True, whereas for other values it is False.

        :param order_by:    Return results in specified order; see HopStrategy for more information

        :param combine_routes:  If True (default), discard multiple routes to the same halo and keep only the
            strongest route. When False, one result is returned for each possible route,
            even if it ultimately reaches the same halo. This can result in a significant
            slow-down.

        :param min_aggregated_weight: Threshold cumulative weight below which routes will be discarded.
              The cumulative weight is defined as the product of the individual hop weights.
              Small values indicate that a small fraction of mass is in common.

        :param min_onehop_weight:     Threshold individual weight below which a route will be discarded.

        :param min_onehop_reverse_weight: Threshold individual weight for the link pointing in the opposite
                                          direction to that being followed, or None for no restriction.
                                          Note that this requires an extra join (if not None) and if there
                                          is no reverse link at all, the result will be dropped.

        :param include_startpoint:    Return the starting halo in the results (default False)
        """
        super().__init__(halo_from, target, order_by)
        if nhops_max is None:
            nhops_max = config.num_multihops_max_default
        self.nhops_max = nhops_max
        self.directed = directed
        self._min_aggregated_weight = min_aggregated_weight
        self._min_onehop_weight = min_onehop_weight
        self._min_onehop_reverse_weight = min_onehop_reverse_weight
        self._include_startpoint = include_startpoint
        if one_simulation is None:
            one_simulation = directed=='backwards' or directed=='forwards'
        self._one_simulation = one_simulation
        self._connection = self.session.connection()
        self._combine_routes = combine_routes
        self._debug_output = False # set to True to see information about discovered links as hops progress

        self.timing_monitor = TimingMonitor()
    def temp_table(self):
        """Execute the strategy and return results as a temp_table (see temporary_halolist module)"""
        if self._all is None:
            return self._temp_table_without_leaving_sql()
        else:
            return temporary_halolist.temporary_halolist_table(self.session, [x.halo_to_id for x in self._all])

    def _temp_table_without_leaving_sql(self):
        tt = self._manage_temp_table()
        tt.__enter__()
        try:
            self._generate_multihop_results()
        except:
            tt.__exit__(*sys.exc_info())
            raise

        thl = temporary_halolist.temporary_halolist_table(self.session,
                                                          self._order_query(self._generate_query(halo_ids_only=True)),
                                                          callback=lambda: tt.__exit__(None, None, None)
                                                          )
        return thl

    def _generate_multihop_results(self):
        self._seed_temp_table()
        self._make_hops()

    def _execute_query(self):
        with self._manage_temp_table():
            self._generate_multihop_results()
            try:
                q = self._order_query(self._generate_query(halo_ids_only=False))
                results = q.all()
                # NB the time here seems to be mainly making the ORM objects
                # rather than the query itself
            except sqlalchemy.exc.ResourceClosedError:
                results = []

        self._all = results


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
            if self._one_simulation:
                recursion_filter &= (timestep_new.simulation_id == timestep_old.simulation_id)
            if directed == 'backwards':
                recursion_filter &= (timestep_new.time_gyr < timestep_old.time_gyr*(1.0-config.max_relative_time_difference))
            elif directed == 'forwards':
                recursion_filter &= (timestep_new.time_gyr > timestep_old.time_gyr*(1.0+config.max_relative_time_difference))
            elif directed == 'across':
                existing_timestep_ids = self.session.query(core.SimulationObjectBase.timestep_id).\
                    select_from(self._link_orm_class).join(self._link_orm_class.halo_to).distinct()
                recursion_filter &= ~timestep_new.id.in_(existing_timestep_ids)
                recursion_filter &= sqlalchemy.func.abs(timestep_new.time_gyr - timestep_old.time_gyr) \
                                    < config.max_relative_time_difference * timestep_old.time_gyr
            else:
                raise ValueError("Unknown direction %r" % directed)

        return recursion_filter

    def _delete_temp_table(self):
        self._table_index.drop(bind=self._connection)
        self._table.drop(checkfirst=True, bind=self._connection)
        self._prelim_table.drop(checkfirst=True, bind=self._connection)
        # self._index.drop(bind=self.connection)
        core.Base.metadata.remove(self._table)
        core.Base.metadata.remove(self._prelim_table)

    def _create_temp_table(self):
        rstr = ''.join(random.choice(string.ascii_lowercase) for _ in range(4))

        # The intent of the following two tables is that they are temporary. With SQLite, it is
        # essential that they are implemented literally as TEMPORARY tables as otherwise the performance
        # is hugely degraded. However, MySQL places two crippling limitations on TEMPORARY tables:
        #  1) No foreign keys allowed referring to columns in the permanent database
        #  2) Cannot join a TEMPORARY table to itself
        #
        # Restriction (1) can be evaded by not declaring the foreign key in the schema, and then
        # providing foreign_keys information in _construct_orm_class. However restriction (2) is
        # completely debilitating. Luckily the performance in MySQL is fine even if we don't
        # declare these tables as TEMPORARY, so we simply switch off the prefix.
        #
        # There is a strange third issue with MySQL which is that, if we declare the foreign key
        # when creating a table, the connection hangs. This seems to be because of a deadlock;
        # other open connections prevent creating the association to the existing tables (the
        # MySQL 'metadata lock'). So, even though MySQL will NOT be using temporary tables,
        # we still don't declare the foreign key dependence.

        dialect = self._connection.dialect.dialect_description.split("+")[0].lower()
        if dialect == 'mysql':
            prefixes = []
        else:
            prefixes = ['TEMPORARY']

        multi_hop_link_table = Table(
            'multihoplink_final_' + rstr,
            core.Base.metadata,
            Column('id', Integer, primary_key=True),
            Column('source_id', Integer),
            # Foreign keys below NOT declared at schema-level, see note above re MySQL
            Column('halo_from_id', Integer),
            Column('halo_to_id', Integer),
            Column('weight', DOUBLE_PRECISION),
            Column('nhops', Integer),
            prefixes = prefixes
        )

        multi_hop_link_prelim_table = Table(
            'multihoplink_prelim_' + rstr,
            core.Base.metadata,
            Column('id', Integer, primary_key=True),
            Column('source_id', Integer),
            # Foreign keys below NOT declared at schema-level, see note above re MySQL
            Column('halo_from_id', Integer),
            Column('halo_to_id', Integer),
            Column('weight', DOUBLE_PRECISION),
            Column('nhops', Integer),
            prefixes = prefixes
        )

        self._table_index = Index('temp.source_id_index_' + rstr, multi_hop_link_table.c.source_id, multi_hop_link_table.c.nhops)
        self._table_nhop_index = Index('temp.nhop_index_' + rstr, multi_hop_link_table.c.nhops)

        self._table = multi_hop_link_table
        self._prelim_table = multi_hop_link_prelim_table

        self._table.create(checkfirst=True, bind=self._connection)
        self._prelim_table.create(checkfirst=True, bind=self._connection)

    @contextlib.contextmanager
    def _manage_temp_table(self):
        self._create_temp_table()
        self._link_orm_class = self._construct_orm_class()
        yield
        self._delete_temp_table()

    def _seed_temp_table(self):
        insert_statement = self._table.insert().values(halo_from_id=self.halo_from.id, halo_to_id=self.halo_from.id,
                                    weight=1.0, nhops=0, source_id=0)

        self._connection.execute(insert_statement)

    def _make_hops(self):
        for i in range(0, self.nhops_max):
            with self.timing_monitor(self):
                self._nhops_taken = i
                generated_count = self._generate_next_level_prelim_links(i)
                if generated_count != 0:
                    filtered_count = self._filter_prelim_links_into_final()
                else:
                    filtered_count = 0

            # for performance info: self.timing_monitor.summarise_timing(logger)

            if self._hopping_finished(filtered_count):
                break


    def _hopping_finished(self, filtered_count):
        return filtered_count==0

    def _generate_next_level_prelim_links(self, from_nhops=0):
        self.timing_monitor.mark('prelim-insert')
        new_weight = self._table.c.weight * core.halo_data.HaloLink.weight

        recursion_query = \
            self.session.query(
                core.halo_data.HaloLink.halo_from_id.label('halo_from_id'),
                core.halo_data.HaloLink.halo_to_id.label("halo_to_id"),
                new_weight.label('new_weight'),
                (self._table.c.nhops + sqlalchemy.literal(1)).label("nhops"),
                self._table.c.source_id.label('source_id')). \
                select_from(self._table). \
                outerjoin(core.halo_data.HaloLink, and_(self._table.c.nhops == from_nhops,
                                                           self._table.c.halo_to_id == core.halo_data.HaloLink.halo_from_id)). \
                filter(core.halo_data.HaloLink.weight > self._min_onehop_weight
                       )

        insert = self._prelim_table.insert().from_select(
            ['halo_from_id', 'halo_to_id', 'weight', 'nhops', 'source_id'],
            recursion_query)

        num_inserted = self._connection.execute(insert).rowcount

        self.timing_monitor.mark('prelim-thin')
        if self._combine_routes:
            # Ideally, before self._prelim_table.insert(), one would adapt recursion_query to return the argmax of
            # new_weight, grouped by halo_to_id and source_id. That could have been achieved using:
            #
            #   from ..util.sql_argmax import sql_argmax
            #   recursion_query = sql_argmax(recursion_query, "new_weight",
            #                               ["halo_to_id", "source_id"])
            #
            # However, this turns out to be very slow and there are no obvious ways to optimise it. It's essentially
            # using a huge CTE/subquery to replace a native argmax facility in SQL. Originally, with sqlite, we simply did:
            #
            #   recursion_query = recursion_query.group_by(core.halo_data.HaloLink.halo_to_id, self._table.c.source_id)
            #
            # It is actually unclear to me now why this ever worked, but anyway it doesn't work in MySQL. The balance of
            # remaining correct without killing performance is to actually insert all the rows into prelim_table, and
            # then delete the ones which are not wanted afterwards. This is presumably faster because the temp table
            # has relevant indices.
            from ..util.sql_argmax import delete_non_maximal_rows

            deleted_count = delete_non_maximal_rows(self._connection, self._prelim_table,
                                                    self._prelim_table.c.weight,
                                                    [self._prelim_table.c.halo_to_id, self._prelim_table.c.source_id])
            num_inserted-=deleted_count

        return num_inserted

    def _debug_print_links(self, tab):
        halo_from = sqlalchemy.orm.aliased(core.halo.Halo)
        halo_to = sqlalchemy.orm.aliased(core.halo.Halo)
        halo_source = sqlalchemy.orm.aliased(core.halo.Halo)
        q = self.session.query(halo_from, halo_to, tab.c.source_id, tab.c.weight, tab.c.nhops).\
            select_from(tab).\
            join(halo_from, tab.c.halo_from_id == halo_from.id).\
            join(halo_to, tab.c.halo_to_id == halo_to.id)
        for row in q.all():
            print(f"[s{row[2]} i{row[4]}] {row[0].path} -> {row[1].path} w={row[3]:.2f}")

    def _filter_prelim_links_into_final(self):
        self.timing_monitor.mark('final-insert')
        if self._debug_output:
            print()
            print(f"[{self._nhops_taken}] Preliminary links:")
            self._debug_print_links(self._prelim_table)

        q = self.session.query(self._prelim_table.c.halo_from_id,
                               self._prelim_table.c.halo_to_id,
                               self._prelim_table.c.weight,
                               self._prelim_table.c.nhops,
                               self._prelim_table.c.source_id)

        q = self._supplement_halolink_query_with_reverse_hop_filter(q, self._prelim_table)
        q = self._supplement_halolink_query_with_filter(q, self._prelim_table)

        added_rows = self._connection.execute(
            self._table.insert().from_select(['halo_from_id', 'halo_to_id', 'weight', 'nhops', 'source_id'],
                                             q)).rowcount

        if self._debug_output:
            logger.info(f"[{self._nhops_taken}] Accepted links:")
            self._debug_print_links(self._table)

        self.timing_monitor.mark('final-thin')
        self._connection.execute(self._prelim_table.delete())
        return added_rows

    def _supplement_halolink_query_with_reverse_hop_filter(self, query, table=None):
        if self._min_onehop_reverse_weight is None:
            return query

        if table is None:
            table = core.halo_data.HaloLink.__table__

        hl_alias = sqlalchemy.orm.aliased(core.HaloLink)

        query = query. \
                join(hl_alias,
                     and_(hl_alias.halo_from_id==table.c.halo_to_id,
                     hl_alias.halo_to_id==table.c.halo_from_id)).\
                filter(hl_alias.weight>self._min_onehop_reverse_weight)


        return query

    def _construct_orm_class(self):
        rstr = ''.join(random.choice(string.ascii_lowercase) for _ in range(4))
        class_name = "MultiHopHaloLink_"+rstr
        class_base = (core.Base,)
        class_attrs = {"__table__": self._table,
                       "halo_from": relationship(core.halo.SimulationObjectBase, primaryjoin=self._table.c.halo_from_id == core.halo.SimulationObjectBase.id,
                                                 foreign_keys = [self._table.c.halo_from_id]),
                       "halo_to"  : relationship(core.halo.SimulationObjectBase, primaryjoin=(self._table.c.halo_to_id == core.halo.SimulationObjectBase.id),
                                                 foreign_keys = [self._table.c.halo_to_id]),
                       "source_id" : self._table.c.source_id,
                       "weight" : self._table.c.weight,
                       "nhops" : self._table.c.nhops
                       }

        return type(class_name,class_base,class_attrs)


    def _generate_query(self, halo_ids_only):
        if halo_ids_only:
            query = self.session.query(self._link_orm_class.halo_to_id)
        else:
            query = self.session.query(self._link_orm_class)

        if not self._include_startpoint:
            query = query.filter(self._table.c.nhops>0)

        query = self._filter_query_for_target(query, self._target)

        return query

    def _generate_order_arg_from_name(self, name, halo_alias, timestep_alias):
        if name == 'nhops':
            return self._link_orm_class.c.nhops
        else:
            return super()._generate_order_arg_from_name(name, halo_alias, timestep_alias)
