from .. import core

class TimestepObjectCache(object):
    """A temporary store for all objects in a timestep, allowing objects to be resolved without a further database query"""
    def __init__(self, timestep):
        """Query the database for all objects in this timestep

        :type timestep: core.TimeStep"""

        self.session = core.Session.object_session(timestep)
        self._timestep_id = timestep.id

    def _initialise_cache(self):
        all_objects = self.session.query(core.Halo).filter_by(timestep_id=self._timestep_id).all()

        self._map_catalog = {}
        self._map_finder = {}
        for obj in all_objects:
            typetag = obj.object_typetag_from_code(obj.object_typecode)
            if typetag not in self._map_catalog:
                self._map_catalog[typetag] = {}
            if typetag not in self._map_finder:
                self._map_finder[typetag] = {}
            self._map_catalog[typetag][obj.catalog_index] = obj
            self._map_finder[typetag][obj.finder_id] = obj

    def _ensure_cache(self, type):
        if not hasattr(self, "_map_"+type):
            self._initialise_cache()

    def resolve(self, identifier, typetag, type='catalog'):
        self._ensure_cache(type)
        try:
            return getattr(self, '_map_'+type)[typetag][identifier]
        except KeyError:
            return None