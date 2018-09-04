from __future__ import absolute_import
from .. import core
from ..util import consistent_collection

from .multi_hop import MultiHopStrategy
from .one_hop import HopStrategy
from six.moves import range
from six.moves import zip

import sqlalchemy
from sqlalchemy import func, orm

class MultiSourceMultiHopStrategy(MultiHopStrategy):
    """A variant of MultiHopStrategy that finds halos corresponding to multiple start points.

    Note that the behaviour is necessarily somewhat different to the other classes which start from a single
    halo. Specifically, a target *must* be specified, and the direction of the hops to follow is inferred
    from the nature of the target.

    Additionally, as soon as any halo is "matched" in the target, the entire query is stopped. In other words,
    this class assumes that the number of hops is the same to reach all target halos."""

    def __init__(self, halos_from, target, **kwargs):
        """Construct a strategy for finding Halos via multiple "hops" along HaloLinks from multiple start-points

        :param halos_from: a list of all halos to start from.
        :param target: a TimeStep or Simulation object to target.
        :param one_match_per_input: if True (default), return one halo per starting point in order.
                                  The returned halo in each case should be the one with the
                                  highest weight link (i.e. the major progenitor or similar)

                                  if False, *all* linked halos are returned and the caller has to figure out
                                  which one belongs to which starting halo, e.g. by calling sources()

        Other parameters are passed onto an underlying MultiHopStrategy. However note that the order_by parameter
        has no effect unless one_match_per_input is False.
        """
        directed = kwargs.get("directed", self._infer_direction(halos_from, target))
        kwargs["directed"] = directed
        kwargs["target"] = target

        self._return_only_highest_weights = kwargs.pop('one_match_per_input', True)

        # For 'backwards' or 'forwards' searches (basically major progenitors or descendants), keep only the
        # strongest link at each _step_ rather than waiting to the end to select the highest weight.
        # This makes sure one never "hops" from one branch to another (see
        # test_hop_strategy.test_major_progenitor_from_minor_progenitor for an example that exposes
        # this former bug). The actual implementation of the per-step restriction is in the override to
        # _supplement_halolink_query_with_filter, below.
        self._keep_only_highest_weights_per_hop = (directed == "forwards" or directed == "backwards")
        self._keep_only_highest_weights_per_hop&=self._return_only_highest_weights | (target is None)

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
        insert_dictionaries = []
        for i,halo_from in enumerate(self._all_halo_from):
            insert_dictionaries.append({'halo_from_id': halo_from.id, 'halo_to_id': halo_from.id, 'weight': 1.0,
                                        'nhops': 0, 'source_id': i})

        self._connection.execute(self._table.insert(), insert_dictionaries)

    def _generate_next_level_prelim_links(self, from_nhops=0):
        if self._should_halt():
            return 0
        else:
            return super(MultiSourceMultiHopStrategy, self)._generate_next_level_prelim_links(from_nhops)

    def _supplement_halolink_query_with_filter(self, query, table=None):
        query = super(MultiSourceMultiHopStrategy, self)._supplement_halolink_query_with_filter(query,table)

        if self._keep_only_highest_weights_per_hop:

            query = self._extract_max_weight_rows_from_query(query, table)

        return query

    def _extract_max_weight_rows_from_query(self, query, table):
        if table is None:
            table = core.halo_data.HaloLink.__table__

        # return only the highest weight link from each halo
        # (the following join is basically a work-around for the lack of an 'argmax' type functionality in sql)
        subq = query.add_columns(func.max(table.c.weight).label("max_weight")). \
            group_by(table.c.halo_from_id, table.c.source_id).subquery()
        query = query.join(subq, sqlalchemy.and_(table.c.halo_from_id == subq.c.halo_from_id,
                                                 table.c.weight == subq.c.max_weight))

        # there may be rare occasions where two links have exactly the same weight, in which case the above query
        # currently generates more than one row. Avoid by grouping. Note in this case SQL will return
        # basically a random choice of which row to return, so this is not a perfect solution - TODO.
        query = query.group_by(table.c.source_id)

        return query

    def _should_halt(self):
        return self.query.count()>0

    def _order_by_clause(self, halo_alias, timestep_alias):
        return [self._link_orm_class.source_id] \
               + super(MultiSourceMultiHopStrategy, self)._order_by_clause(halo_alias, timestep_alias)

    def all(self):
        results = self._get_query_all()
        if self._return_only_highest_weights:
            # query results include a source_id column which we now wish to ignore (see
            # _query_ordered below for more information)
            return [x[1].halo_to if x[1] is not None else None
                    for x in results]
        else:
            return [x.halo_to for x in results]

    def sources(self):
        """Returns the offset in the original list that generated each result returned by all().

        For example, if the class is constructed for two halos, but the results have two results for
        the first halo, sources() will return [0,0,1]."""
        results = self._get_query_all()
        if self._return_only_highest_weights:
            return [x[0] for x in results]
        else:
            return [x.source_id for x in results]

    @property
    def _query_ordered(self):
        query = super(MultiSourceMultiHopStrategy, self)._query_ordered
        if self._return_only_highest_weights:
            query = self._extract_max_weight_rows_from_query(query, self._table)

            # we now need to restructure the results such that they appear in the same order as the input
            # halo list, and a NULL result is returned for 'missing' halos. This way, the result is
            # guaranteed to be in 1-1 correspondence with the input.
            #
            # We do this by joining to the initial seeds in the temp table
            #
            # the query has to explicitly include the source_id, otherwise the sqlalchemy dedup
            # process removes duplicates rows (e.g. if there are several null results, only the
            # first will be returned!) resulting in a query result that is too short and unrecoverable
            # errors in functions that rely on getting back a 1-1 mapping

            original_query_alias = query.subquery()
            original_orm_alias = orm.aliased(self._link_orm_class, original_query_alias)
            source_ids = orm.aliased(self._link_orm_class)

            sources_query = self.session.query(source_ids.source_id,original_orm_alias).\
                select_from(source_ids).\
                filter(source_ids.nhops==0).order_by(source_ids.source_id)

            query = sources_query.outerjoin(original_orm_alias,
                                            source_ids.source_id==original_orm_alias.source_id)
            query = query.options(orm.joinedload(original_orm_alias.halo_to))



        return query


class MultiSourceAllMajorProgenitorsStrategy(MultiSourceMultiHopStrategy):

    def __init__(self, halos_from, **kwargs):
        super(MultiSourceAllMajorProgenitorsStrategy, self).__init__(halos_from, None, one_match_per_input=False,
                                                                     directed='backwards', include_startpoint=True)

    def _should_halt(self):
        return False