class ConsistentCollection(object):
    def __init__(self, objects):
        self._objects = set(objects)

    def __getitem__(self, item):
        values = [x[item] for x in self._objects]
        return self._ensure_consistent(values)

    def get(self, item, default=None):
        values = [x.get(item, default) for x in self._objects]
        return self._ensure_consistent(values)

    def _ensure_consistent(self, values):
        if any([v!=values[0] for v in values]):
            raise ValueError, "Item %r is not consistent between members of the collection"
        return values[0]


def consistent_simulation_from_halos(halos):
    return ConsistentCollection([x.timestep.simulation for x in halos])