"""Logic for getting halo properties and links from a Halo object.

Commonly-used patterns are effectively static (i.e. they have no options), and default instances for these patterns
are provided.

e.g.
 - getting a HaloProperty (HaloPropertyGetter -> default instance halo_property_getter);
 - getting a HaloProperty's value (HaloPropertyValueGetter -> default instance halo_property_value_getter);
 - getting a HaloProperty's value without calling reassemble (HaloPropertyRawValueGetter -> halo_property_raw_value_getter);
 - getting a HaloLink (HaloLinkGetter -> halo_link_getter) ;
 - getting a HaloLink target (HaloLinkTargetGetter -> halo_link_target_getter).

However, in one case -- where one wants to pass options to the data reassembly process -- a static implementation is
not possible. So the HaloPropertyValueWithReassemblyOptionsGetter has no "default" instance and has to be
instantiated when required.
"""

from __future__ import absolute_import
import sqlalchemy




class HaloPropertyGetter(object):
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

        return self._postprocess(return_vals)


    def get_from_session(self, halo, property_id, session):
        """Get the specified property from the database using the specified session

        :type halo: Halo
        :type property_id: int
        :type session: sqlalchemy.orm.session.Session"""
        from . import halo_data
        query_properties = session.query(halo_data.HaloProperty).filter_by(name_id=property_id, halo_id=halo.id,
                                                                                        deprecated=False).order_by(
            halo_data.HaloProperty.id.desc())

        return self._postprocess(query_properties.all())

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


    def _postprocess(self, outputs):
        return outputs

halo_property_getter = HaloPropertyGetter()

class HaloPropertyValueGetter(HaloPropertyGetter):
    def _postprocess(self, outputs):
        return [o.get_data_with_reassembly_options() for o in outputs]
    
halo_property_value_getter = HaloPropertyValueGetter()


class HaloPropertyValueWithReassemblyOptionsGetter(HaloPropertyGetter):
    def __init__(self, *options):
        self._options = options

    def _postprocess(self, outputs):
        return [o.get_data_with_reassembly_options(*self._options) for o in outputs]


class HaloPropertyRawValueGetter(HaloPropertyGetter):
    def _postprocess(self, outputs):
        return [o.data_raw for o in outputs]

halo_property_raw_value_getter=HaloPropertyRawValueGetter()


class HaloLinkGetter(HaloPropertyGetter):
    def get_from_cache(self, halo, property_id):
        return_vals = []

        for x in halo.all_links:
            if x.relation_id == property_id:
                return_vals.append(x)

        return self._postprocess(return_vals)

    def get_from_session(self, halo, property_id, session):
        from . import halo_data
        query_links = session.query(halo_data.HaloLink).filter_by(relation_id=property_id, halo_from_id=halo.id).order_by(
            halo_data.HaloLink.id)
        return self._postprocess(query_links.all())

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
    
halo_link_getter = HaloLinkGetter()


class HaloLinkTargetGetter(HaloLinkGetter):
    def _postprocess(self, outputs):
        return [o.halo_to for o in outputs]

halo_link_target_getter = HaloLinkTargetGetter()


class ReverseHaloLinkGetter(HaloLinkGetter):
    def get_from_cache(self, halo, property_id):
        return_vals = []

        for x in halo.all_reverse_links:
            if x.relation_id == property_id:
                return_vals.append(x)

        return self._postprocess(return_vals)

    def get_from_session(self, halo, property_id, session):
        from . import halo_data
        query_links = session.query(halo_data.HaloLink).filter_by(relation_id=property_id,
                                                                               halo_to_id=halo.id)
        return self._postprocess(query_links.all())

    def cache_contains(self, halo, property_id):
        for x in halo.all_reverse_links:
            if x.relation_id == property_id:
                return True

        return False


class ReverseHaloLinkSourceGetter(ReverseHaloLinkGetter):
    def _postprocess(self, outputs):
        return [o.halo_from for o in outputs]