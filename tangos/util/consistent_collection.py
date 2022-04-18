class ConsistentCollection(object):
    """Access attributes of an underlying collection of objects, ensuring they are consistent.

    This class wraps a list of objects and, when getting attributes or values, behaves transparently as though it is
    just one of those objects provided that they all actually return the same. If any individual in the collection
    returns a different value, a ValueError is raised."""

    def __init__(self, objects):
        if len(objects)==0:
            raise ValueError("Cannot create a ConsistentCollection from an empty list")
        self._objects = set(objects)

    def __getitem__(self, item):
        values = [x[item] for x in self._objects]
        return self._ensure_consistent(values, item)

    def __getattr__(self, item):
        values = [getattr(x, item) for x in self._objects]
        return self._ensure_consistent(values, item)

    def get(self, item, default=None):
        values = [x.get(item, default) for x in self._objects]
        return self._ensure_consistent(values, item)

    def _ensure_consistent(self, values, item_name):
        if any([v!=values[0] for v in values]):
            raise ValueError("Item %r is not consistent between members of the collection"%item_name)
        return values[0]

def consistent_simulation_from_halos(halos):
    return ConsistentCollection([x.timestep.simulation for x in halos])