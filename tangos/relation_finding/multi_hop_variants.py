from ..config import num_multihops_max_default as NHOPS_MAX_DEFAULT
from .multi_hop import MultiHopStrategy


class MultiHopAllProgenitorsStrategy(MultiHopStrategy):
    """Finds all progenitors for a halo at every step"""
    def __init__(self, halo_from, nhops_max=NHOPS_MAX_DEFAULT, include_startpoint=False, target='auto',
                 combine_routes=True, order_by=None, one_simulation=None):
        if order_by is None:
            order_by = ['time_desc', 'halo_number_asc']
        self.sim_id = halo_from.timestep.simulation_id
        if target=='auto':
            target = halo_from.timestep.simulation
        super().__init__(halo_from, nhops_max,
                                                               directed='backwards',
                                                               include_startpoint=include_startpoint,
                                                               target=target,
                                                               order_by=order_by,
                                                               combine_routes=combine_routes,
                                                             min_onehop_reverse_weight=0.1,
                                                             one_simulation=one_simulation)

    def _supplement_halolink_query_with_filter(self, query, table):
        query = super()._supplement_halolink_query_with_filter(query, table)
        if self._target is None:
            return query
        else:
            return query.filter(self.timestep_new.simulation_id == self.sim_id)


class MultiHopMajorProgenitorsStrategy(MultiHopAllProgenitorsStrategy):
    """Finds the major progenitor for a halo at every step"""

    def _supplement_halolink_query_with_filter(self, query, table):
        query = super()._supplement_halolink_query_with_filter(query, table)
        return query.order_by(self.timestep_new.time_gyr.desc(), table.c.weight.desc(), self.halo_new.halo_number). \
            limit(1)

class MultiHopMostRecentMergerStrategy(MultiHopAllProgenitorsStrategy):
    """Finds the halos involved in the most recent merger into the major progenitor branch of the halo"""

    def _hopping_finished(self, filtered_count):
        self._last_filtered_count = filtered_count
        return filtered_count != 1

    def _generate_query(self, halo_ids_only):
        query = super()._generate_query(halo_ids_only)
        if self._last_filtered_count>1:
            query = query.filter(self._table.c.nhops==self._nhops_taken+1)
        else:
            # no merger was found
            query = query.filter(0==1)
        return query



class MultiHopMajorDescendantsStrategy(MultiHopStrategy):
    """Suggests the major descendant for a halo at every step"""

    def __init__(self, halo_from, nhops_max=NHOPS_MAX_DEFAULT, include_startpoint=False, **kwargs):
        self.sim_id = halo_from.timestep.simulation_id
        super().__init__(halo_from, nhops_max,
                                                               directed='forwards',
                                                               include_startpoint=include_startpoint,
                                                               target=halo_from.timestep.simulation,
                                                               **kwargs)

    def _supplement_halolink_query_with_filter(self, query, table):
        query = super()._supplement_halolink_query_with_filter(query, table)
        return query.filter(self.timestep_new.simulation_id == self.sim_id). \
            order_by(self.timestep_new.time_gyr, table.c.weight.desc(), self.halo_new.halo_number). \
            limit(1)
