class HaloPropertyGetter(object):
    @classmethod
    def get_from_cache(cls, halo, property_id):
        """Get the specified property from an existing in-memory cache

        :type halo: Halo
        :type property_id: int"""

        return_vals = []

        for x in halo.all_properties:
            if x.name_id == property_id:
                return_vals.append(x)

        return cls._postprocess(return_vals)

    @classmethod
    def get_from_session(cls, halo, property_id, session):
        """Get the specified property from the database using the specified session

        :type halo: Halo
        :type property_id: int
        :type session: sqlalchemy.orm.session.Session"""
        from . import core
        query_properties = session.query(core.HaloProperty).filter_by(name_id=property_id, halo_id=halo.id,
                                                                 deprecated=False).order_by(core.HaloProperty.id.desc())

        return cls._postprocess(query_properties.all())

    @classmethod
    def cache_contains(cls, halo, property_id):
        """Return True if the existing in-memory cache has the specified property

        :type halo: Halo
        :type property_id: int"""

        for x in halo.all_properties:
            if x.name_id == property_id:
                return True

        return False


    @classmethod
    def _postprocess(cls, outputs):
        return outputs


class HaloPropertyValueGetter(HaloPropertyGetter):
    @classmethod
    def _postprocess(cls, outputs):
        return [o.data for o in outputs]

class HaloPropertyRawValueGetter(HaloPropertyGetter):
    @classmethod
    def _postprocess(cls, outputs):
        return [o.data_raw for o in outputs]



class HaloLinkGetter(HaloPropertyGetter):
    @classmethod
    def get_from_cache(cls, halo, property_id):
        return_vals = []

        for x in halo.all_links:
            if x.relation_id == property_id:
                return_vals.append(x)

        return cls._postprocess(return_vals)

    @classmethod
    def get_from_session(cls, halo, property_id, session):
        from . import core
        query_links = session.query(core.HaloLink).filter_by(relation_id=property_id, halo_from_id=halo.id)
        return cls._postprocess(query_links.all())

    @classmethod
    def cache_contains(cls, halo, property_id):
        for x in halo.all_links:
            if x.relation_id == property_id:
                return True

        return False

class HaloLinkTargetGetter(HaloLinkGetter):
    @classmethod
    def _postprocess(cls, outputs):
        return [o.halo_to for o in outputs]

class ReverseHaloLinkGetter(HaloLinkGetter):
    @classmethod
    def get_from_cache(cls, halo, property_id):
        return_vals = []

        for x in halo.all_reverse_links:
            if x.relation_id == property_id:
                return_vals.append(x)

        return cls._postprocess(return_vals)

    @classmethod
    def get_from_session(cls, halo, property_id, session):
        from . import core
        query_links = session.query(core.HaloLink).filter_by(relation_id=property_id,
                                                             halo_to_id=halo.id)
        return cls._postprocess(query_links.all())

    @classmethod
    def cache_contains(cls, halo, property_id):
        for x in halo.all_reverse_links:
            if x.relation_id == property_id:
                return True

        return False


class ReverseHaloLinkSourceGetter(ReverseHaloLinkGetter):
    @classmethod
    def _postprocess(cls, outputs):
        return [o.halo_from for o in outputs]