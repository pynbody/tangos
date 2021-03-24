"""The proxy_object module provides a way to point to objects in the database without actually retrieving them.

This is used during property calculations to minimise the number of queries made to the database, and possibly to
point to objects that have not yet been created in the database at the time they are referred to."""

import abc
import six
from .. import core

class ProxyResolutionException(Exception):
    """Unified exception raised when a proxy cannot be translated into an actual database object"""
    pass


@six.add_metaclass(abc.ABCMeta)
class ProxyObjectBase(object):
    """A proxy for an object (i.e. halo, group, BH etc) in the database"""

    @abc.abstractmethod
    def __init__(self):
        pass

    @abc.abstractmethod
    def resolve(self, session):
        """Turn this ProxyObject into an actual object using the specified sqlalchemy session

        :type session: sqlalchemy.orm.Session
        """
        pass

    def relative_to_timestep_id(self, existing_timestep_id):
        """Return a proxy object resolved relative to an existing timestep, specified by database ID"""
        return self

    def relative_to_timestep_cache(self, timestep_cache):
        """Return a proxy object resolved using a cache of the timestep's objects.

        This results in more efficient database queries when a large number of objects are to be retrieved from the same
        timestep

        :type timestep_cache: tangos.util.timestep_object_cache.TimestepObjectCache"""
        return self

class ProxyObjectFromDatabaseId(ProxyObjectBase):
    """A proxy object that resolves into a database object with specified ID"""
    def __init__(self, dbid):
        super(ProxyObjectFromDatabaseId, self).__init__()
        self._dbid = dbid

    def resolve(self, session):
        return session.query(core.Halo).filter_by(id=self._dbid).first()

class ProxyObjectFromFinderIdAndTimestep(ProxyObjectBase):
    """A proxy object that resolves into the object with given finder ID in the specified timestep"""
    def __init__(self, finder_id, typetag, timestep_id):
        super(ProxyObjectFromFinderIdAndTimestep, self).__init__()
        self._finder_id = finder_id
        self._typetag = typetag
        self._timestep_id = timestep_id

    def resolve(self, session):
        return session.query(core.Halo).filter_by(timestep_id=self._timestep_id, finder_id=self._finder_id,
                                                  object_typecode=core.Halo.object_typecode_from_tag(self._typetag)).first()

class ProxyObjectFromFinderIdAndTimestepCache(ProxyObjectBase):
    """A proxy object that resolves into the object with given finder ID in the specified timestep cache"""
    def __init__(self, finder_id, typetag, timestep_cache):
        super(ProxyObjectFromFinderIdAndTimestepCache, self).__init__()
        self._finder_id = finder_id
        self._typetag = typetag
        self._timestep_cache = timestep_cache

    def resolve(self, session):
        if session!=self._timestep_cache.session:
            raise ProxyResolutionException("The session for the cache must match the session for the object resolution")
        return self._timestep_cache.resolve(self._finder_id, self._typetag)

class IncompleteProxyObjectFromFinderId(ProxyObjectBase):
    """A proxy object that stores an object's finder ID and type, but requires the timestep still to be specified.

    By calling the method relative_to_timestep_id or relative_to_timestep_cache, a fully specified proxy object will
    be returned."""
    def __init__(self, finder_id, typetag):
        super(IncompleteProxyObjectFromFinderId, self).__init__()
        self._finder_id = finder_id
        self._typetag = typetag

    def resolve(self, session):
        raise ProxyResolutionException("Incomplete proxy object; the context must be specified")

    def relative_to_timestep_id(self, existing_timestep_id):
        return ProxyObjectFromFinderIdAndTimestep(self._finder_id, self._typetag, existing_timestep_id)

    def relative_to_timestep_cache(self, timestep_cache):
        return ProxyObjectFromFinderIdAndTimestepCache(self._finder_id, self._typetag, timestep_cache)
