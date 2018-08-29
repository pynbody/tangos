"""Routines for getting halo properties and links, and data derived from them, starting with a Halo or other object
"""

from __future__ import absolute_import
import sqlalchemy
from . import data_attribute_mapper


class HaloPropertyGetter(object):
    """HaloPropertyGetter and its subclasses implement efficient methods for retrieving data from sqlalchemy ORM objects.

    The key features are
     * being able to flexibly use a pre-queried cache of ORM objects with data, or issue fresh SQL queries as appropriate
     * being able to flexibly call 'reassembly' on the data
     * speed when used repeatedly on multiple rows of similar data

    Different classes get different types of data from the ORM and/or process it differently.

    This base class is used to retrieve the actual HaloProperty objects.
    """
    def use_fixed_cache(self, halo):
        return 'all_properties' not in sqlalchemy.inspect(halo).unloaded

    def get(self, halo, property_id, session):
        """Get the specified property, from the in-memory cache if it exists otherwise from the database
        using the specified session

        :type halo: Halo
        :type property_id: int
        :type session: sqlalchemy.orm.session.Session
        """
        if self.use_fixed_cache(halo):
            return self.get_from_cache(halo, property_id)
        else:
            return self.get_from_session(halo, property_id, session)

    def keys(self, halo, session):
        """Get a list of keys, from the in-memory cache if it exists otherwise from the database
            using the specified session

        :type halo: Halo
        :type session: sqlalchemy.orm.session.Session
        """
        if self.use_fixed_cache(halo):
            return self.keys_from_cache(halo)
        else:
            return self.keys_from_session(halo, session)

    def get_from_cache(self, halo, property_id):
        """Get the specified property from an existing in-memory cache

        :type halo: Halo
        :type property_id: int"""

        return_vals = []

        for x in halo.all_properties:
            if x.name_id == property_id:
                return_vals.append(x)

        return self.postprocess_data_objects(return_vals)


    def get_from_session(self, halo, property_id, session):
        """Get the specified property from the database using the specified session

        :type halo: Halo
        :type property_id: int
        :type session: sqlalchemy.orm.session.Session"""
        from . import halo_data
        query_properties = session.query(halo_data.HaloProperty).filter_by(name_id=property_id, halo_id=halo.id,
                                                                                        deprecated=False).order_by(
            halo_data.HaloProperty.id.desc())

        return self.postprocess_data_objects(query_properties.all())


    def keys_from_cache(self, halo):
        """Return a list of keys from an existing in-memory cache"""
        return [x.name.text for x in halo.all_properties]

    def keys_from_session(self, halo, session):
        from . import halo_data
        query_properties = session.query(halo_data.HaloProperty).filter_by(halo_id=halo.id,
                                                                                        deprecated=False)
        return [x.name.text for x in query_properties.all()]

    def cache_contains(self, halo, property_id):
        """Return True if the existing in-memory cache has the specified property

        :type halo: Halo
        :type property_id: int"""

        for x in halo.all_properties:
            if x.name_id == property_id:
                return True

        return False

    def postprocess_data_objects(self, objects):
        """Post-process the ORM data objects to pull out the data in the form required"""
        return objects



class HaloPropertyValueGetter(HaloPropertyGetter):
    """As HaloPropertyGetter, but return the data value (including automatic reassembly of the data if appropriate)"""
    def __init__(self):
        self._options = []
        self._providing_class = None
        self._mapper = None

    def postprocess_data_objects(self, outputs):
        return [self._postprocess_one_result(o) for o in outputs]

    def _setup_data_mapper(self, property_object):
        if self._mapper is None:
            # Optimisation: figure out a mapper for the first output and assume it's ok for all of them
            self._mapper = data_attribute_mapper.DataAttributeMapper(property_object)

    def _infer_property_class(self, property_object):
        if self._providing_class is None:
            # Optimisation: figure out a providing class for the first output and assume it's ok for all of them
            try:
                self._providing_class = property_object.name.providing_class(property_object.halo.handler_class)
            except NameError:
                pass

    def _postprocess_one_result(self, property_object):
        self._infer_property_class(property_object)

        if hasattr(self._providing_class, 'reassemble'):
            instance = self._providing_class(property_object.halo.timestep.simulation)
            return instance.reassemble(property_object, *self._options)
        else:
            self._setup_data_mapper(property_object)
            return self._mapper.get(property_object)



class HaloPropertyValueWithReassemblyOptionsGetter(HaloPropertyValueGetter):
    """As HaloPropertyValueGetter, but allow options to be passed to the property reassembler"""
    def __init__(self, *options):
        super(HaloPropertyValueWithReassemblyOptionsGetter, self).__init__()
        self._options = options

class HaloPropertyRawValueGetter(HaloPropertyValueGetter):
    """As HaloPropertyValueGetter, but never invoke an automatic reassembly; always retrieve the raw data"""
    def _postprocess_one_result(self, property_object):
        self._setup_data_mapper(property_object)
        return self._mapper.get(property_object)



class HaloLinkGetter(HaloPropertyGetter):
    """As HaloPropertyGetter, but retrieve HaloLinks instead of HaloProperties"""
    def get_from_cache(self, halo, property_id):
        return_vals = []

        for x in halo.all_links:
            if x.relation_id == property_id:
                return_vals.append(x)

        return self.postprocess_data_objects(return_vals)

    def get_from_session(self, halo, property_id, session):
        from . import halo_data
        query_links = session.query(halo_data.HaloLink).filter_by(relation_id=property_id, halo_from_id=halo.id).order_by(
            halo_data.HaloLink.id)
        return self.postprocess_data_objects(query_links.all())

    def cache_contains(self, halo, property_id):
        for x in halo.all_links:
            if x.relation_id == property_id:
                return True

        return False

    def keys_from_cache(self, halo):
        """Return a list of keys from an existing in-memory cache"""
        return [x.relation.text for x in halo.all_links]

    def keys_from_session(self, halo, session):
        from . import halo_data
        query_properties = session.query(halo_data.HaloLink).filter_by(halo_from_id=halo.id)
        return [x.relation.text for x in query_properties.all()]
    



class HaloLinkTargetGetter(HaloLinkGetter):
    """As HaloLinkGetter, but retrieve the target of the links instead of the HaloLink objects themselves"""
    def postprocess_data_objects(self, outputs):
        return [o.halo_to for o in outputs]

