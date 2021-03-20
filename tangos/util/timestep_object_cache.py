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

        self._map_finder_offset = {}
        self._map_finder_id = {}
        for obj in all_objects:
            typetag = obj.object_typetag_from_code(obj.object_typecode)
            if typetag not in self._map_finder_offset:
                self._map_finder_offset[typetag] = {}
            if typetag not in self._map_finder_id:
                self._map_finder_id[typetag] = {}
            self._map_finder_offset[typetag][obj.finder_offset] = obj
            self._map_finder_id[typetag][obj.finder_id] = obj

    def _ensure_cache(self):
        if not hasattr(self, "_map_finder_id") or not hasattr(self, "_map_finder_offset"):
            self._initialise_cache()

    def resolve_from_finder_offset(self, finder_offset, typetag):
        self._ensure_cache()
        try:
            return self._map_finder_offset[typetag][finder_offset]
        except KeyError:
            return None

    def resolve_from_finder_id(self, finder_id, typetag):
        self._ensure_cache()
        try:
            return self._map_finder_id[typetag][finder_id]
        except KeyError:
            return None