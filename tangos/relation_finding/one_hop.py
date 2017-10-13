from __future__ import absolute_import
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.orm
import sqlalchemy.orm.dynamic
import sqlalchemy.orm.query
from sqlalchemy.orm import Session, contains_eager
from .. import core, temporary_halolist

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

        results = [x for x in results if x is not None]

        self._all = results

    def _get_query_all(self):
        if self._all is None:
            self._execute_query()
        return self._all

    def temp_table(self):
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
            raise ValueError("Unknown ordering method %r" % name)

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
            query = query.options(contains_eager("halo_to", alias=halo_alias))
            query = query.options(contains_eager("halo_to", "timestep", alias=timestep_alias))
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