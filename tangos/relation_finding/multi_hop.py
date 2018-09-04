from __future__ import absolute_import
import contextlib
import random
import string
import sys

import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.orm
import sqlalchemy.orm.dynamic
import sqlalchemy.orm.query
from sqlalchemy import and_, Table, Index, Column, Integer, Float, ForeignKey
from sqlalchemy.orm import relationship

from .. import core
from .. import temporary_halolist
from .one_hop import HopStrategy

from ..config import num_multihops_max_default as NHOPS_MAX_DEFAULT
from six.moves import range

# Fractional increase or decrease in time to represent future or past when making hops
# (prevents numerical accuracy issues making the db misunderstand a contemporaneous step
# as being in the future or past)
SMALL_FRACTION = 1e-4

class MultiHopStrategy(HopStrategy):
    """An extension of the HopStrategy class that takes multiple hops across
    HaloLinks, up to a specified maximum, before finding the target halo."""

    halo_old = sqlalchemy.orm.aliased(core.halo.Halo, name="halo_old")
    halo_new = sqlalchemy.orm.aliased(core.halo.Halo, name="halo_new")
    timestep_old = sqlalchemy.orm.aliased(core.timestep.TimeStep, name="timestep_old")
    timestep_new = sqlalchemy.orm.aliased(core.timestep.TimeStep, name="timestep_new")

    def __init__(self, halo_from, nhops_max=NHOPS_MAX_DEFAULT, directed=None, target=None,
                 order_by=None, combine_routes=True, min_aggregated_weight=0.0,
                 min_onehop_weight=0.0, min_onehop_reverse_weight=None,
                 include_startpoint=False, one_simulation=None):
        """Construct a strategy for finding Halos via multiple "hops" along HaloLinks

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
        super(MultiHopStrategy, self).__init__(halo_from, target, order_by)
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

    def temp_table(self):
        """Execute the strategy and return results as a temp_table (see temporary_halolist module)"""
        tt = self._manage_temp_table()
        tt.__enter__()
        try:
            self._generate_multihop_results()
        except:
            tt.__exit__(*sys.exc_info())
            raise
        thl = temporary_halolist.temporary_halolist_table(self.session,
                                                          self._query_ordered.from_self(
                                                              self._link_orm_class.halo_to_id),
                                                          callback=lambda: tt.__exit__(None, None, None)
                                                          )
        return thl

    def _generate_multihop_results(self):
        self._generate_query()
        self._seed_temp_table()
        self._filter_query_for_target(self._target)
        self._make_hops()

    def _execute_query(self):
        with self._manage_temp_table():
            self._generate_multihop_results()
            try:
                results = self._query_ordered.all()
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
                recursion_filter &= (timestep_new.time_gyr < timestep_old.time_gyr*(1.0-SMALL_FRACTION))
            elif directed == 'forwards':
                recursion_filter &= (timestep_new.time_gyr > timestep_old.time_gyr*(1.0+SMALL_FRACTION))
            elif directed == 'across':
                existing_timestep_ids = self.session.query(core.Halo.timestep_id).\
                    select_from(self._link_orm_class).join(self._link_orm_class.halo_to).distinct()
                recursion_filter &= ~timestep_new.id.in_(existing_timestep_ids)
                recursion_filter &= sqlalchemy.func.abs(timestep_new.time_gyr - timestep_old.time_gyr) < SMALL_FRACTION
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

        multi_hop_link_table = Table(
            'multihoplink_final_' + rstr,
            core.Base.metadata,
            Column('id', Integer, primary_key=True),
            Column('source_id', Integer),
            Column('halo_from_id', Integer, ForeignKey('halos.id')),
            Column('halo_to_id', Integer, ForeignKey('halos.id')),
            Column('weight', Float),
            Column('nhops', Integer),
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
            prefixes=['TEMPORARY']
        )

        self._table_index = Index('temp.source_id_index_' + rstr, multi_hop_link_table.c.source_id, multi_hop_link_table.c.nhops)

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
                                    weight=1.0, nhops=0)

        self._connection.execute(insert_statement)

    def _make_hops(self):
        for i in range(0, self.nhops_max):
            generated_count = self._generate_next_level_prelim_links(i)
            if generated_count != 0:
                filtered_count = self._filter_prelim_links_into_final()
            else:
                filtered_count = 0

            if self._hopping_finished(filtered_count):
                break
        self._nhops_taken = i

    def _hopping_finished(self, filtered_count):
        return filtered_count==0

    def _generate_next_level_prelim_links(self, from_nhops=0):

        new_weight = self._table.c.weight * core.halo_data.HaloLink.weight

        if self._combine_routes:
            new_weight = sqlalchemy.func.max(new_weight)

        recursion_query = \
            self.session.query(
                core.halo_data.HaloLink.halo_from_id,
                core.halo_data.HaloLink.halo_to_id.label("halo_to_id"),
                new_weight,
                (self._table.c.nhops + sqlalchemy.literal(1)).label("nhops"),
                self._table.c.source_id). \
                select_from(self._table). \
                join(core.halo_data.HaloLink, and_(self._table.c.nhops == from_nhops,
                                                           self._table.c.halo_to_id == core.halo_data.HaloLink.halo_from_id)). \
                filter(core.halo_data.HaloLink.weight > self._min_onehop_weight
                       )

        if self._combine_routes:
            recursion_query = recursion_query.group_by(core.halo_data.HaloLink.halo_to_id, self._table.c.source_id)

        insert = self._prelim_table.insert().from_select(
            ['halo_from_id', 'halo_to_id', 'weight', 'nhops', 'source_id'],
            recursion_query)

        result = self._connection.execute(insert)

        return result.rowcount

    def _filter_prelim_links_into_final(self):

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
                       "halo_from": relationship(core.halo.Halo, primaryjoin=self._table.c.halo_from_id == core.halo.Halo.id),
                       "halo_to"  : relationship(core.halo.Halo, primaryjoin=(self._table.c.halo_to_id == core.halo.Halo.id)),
                       "source_id" : self._table.c.source_id,
                       "nhops" : self._table.c.nhops
                       }

        return type(class_name,class_base,class_attrs)


    def _generate_query(self):
        self._link_orm_class = self._construct_orm_class()
        self.query = self.session.query(self._link_orm_class)

        if not self._include_startpoint:
            self.query = self.query.filter(self._table.c.nhops>0)

    def _generate_order_arg_from_name(self, name, halo_alias, timestep_alias):
        if name == 'nhops':
            return self._link_orm_class.c.nhops
        else:
            return super(MultiHopStrategy, self)._generate_order_arg_from_name(name, halo_alias, timestep_alias)
