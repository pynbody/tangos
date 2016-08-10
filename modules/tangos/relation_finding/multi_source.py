from .. import core
from ..util import consistent_collection

from .multi_hop import MultiHopStrategy
from .one_hop import HopStrategy

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

    def _should_halt(self):
        return self.query.count()>0

    def _order_by_clause(self, halo_alias, timestep_alias):
        return [self._link_orm_class.source_id, self._table.c.weight]

    def _ordering_requires_join(self):
        # Always return True, as our all() function will do post-processing based on halo_to
        # Without the join, this leads to a large number of SELECTs being emitted (v slow)
        return True

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


