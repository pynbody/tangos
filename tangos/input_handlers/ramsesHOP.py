from .pynbody import PynbodyInputHandler

class RamsesHOPInputHandler(PynbodyInputHandler):
    """ Handling Ramses outputs with HOP halo finding (Eisenstein and Hut 1998)"""
    patterns = ["output_0????"]
    auxiliary_file_patterns = ["grp*.tag"]

    def match_objects(self, ts1, ts2, halo_min, halo_max, dm_only=False, threshold=0.005,
                      object_typetag="halo", output_handler_for_ts2=None):

        f1 = self.load_timestep(ts1).dm
        h1 = self._construct_halo_cat(ts1, object_typetag)

        if output_handler_for_ts2 is None:
            f2 = self.load_timestep(ts2).dm
            h2 = self._construct_halo_cat(ts2, object_typetag)
        else:
            f2 = output_handler_for_ts2.load_timestep(ts2).dm
            h2 = output_handler_for_ts2._construct_halo_cat(ts2, object_typetag)

        bridge = pynbody.bridge.OrderBridge(f1, f2, monotonic=False)

        return bridge.fuzzy_match_catalog(halo_min, halo_max, threshold=threshold, only_family=pynbody.family.dm,
                                          groups_1=h1, groups_2=h2)


def RamsesAdaptaHOPInputHandler(RamsesHOPInputHandler):
    """ Handling Ramses outputs with AdaptaHOP halo and subhalo finding """

    patterns = ["output_0????"]
    auxiliary_file_patterns = ["tree_bricks???"]

    def available_object_property_names_for_timestep(self, ts_extension, object_typetag):
        f = self._load_timestep(ts_extension)
        h = self._construct_halo_cat(ts_extension, object_typetag)

        halo_attributes = list(h._halo_attributes)
        if h._read_contamination:
            halo_attributes.extend(h._halo_attributes_contam)

        attrs = chain.from_iterable(
            tuple(always_iterable(attr)) for (attr, _len, _dtype) in halo_attributes)

        # We return all properties but the ids of the particles contained in the halo
        return [attr for attr in attrs if attr != "members"]


    # def _construct_group_cat(self, ts_extension):
    #     f = self.load_timestep(ts_extension)
    #     h = _loaded_halocats.get(id(f)+1, lambda: None)()
    #     if h is None:
    #         h = f.halos()
    #         assert isinstance(h, pynbody.halo.SubfindCatalogue)
    #         _loaded_halocats[id(f)+1] = weakref.ref(h)
    #         f._db_current_groupcat = h  # keep alive for lifetime of simulation
    #     return h



    # def _construct_halo_cat(self, ts_extension, object_typetag):
    #     if object_typetag== 'halo':
    #         return super(RamsesHOPInputHandler, self)._construct_halo_cat(ts_extension, object_typetag)
    #     elif object_typetag== 'group':
    #         return self._construct_group_cat(ts_extension)
    #     else:
    #         raise ValueError("Unknown halo type %r" % object_typetag)


    # def available_object_property_names_for_timestep(self, ts_extension, object_typetag):
    #     if object_typetag=='halo':
    #         return ["CM","HalfMassRad","VMax","VMaxRad","mass","pos","spin","vel","veldisp","parent"]
    #     elif object_typetag=='group':
    #         return ["mass","mcrit_200","mmean_200","mtop_200","rcrit_200","rmean_200","rtop_200","child"]
    #     else:
    #         raise ValueError("Unknown object typetag %r"%object_typetag)

