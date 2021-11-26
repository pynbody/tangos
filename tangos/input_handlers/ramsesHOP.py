from more_itertools import always_iterable
from ..util import proxy_object
from itertools import chain
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


class RamsesAdaptaHOPInputHandler(RamsesHOPInputHandler):
    """ Handling Ramses outputs with AdaptaHOP halo and subhalo finding """

    patterns = ["output_0????"]
    auxiliary_file_patterns = ["tree_bricks???"]

    def _get_halo_children(self, ts_extension):
        # TODO
        h = self._construct_halo_cat(ts_extension, 'halo')
        halo_children = {}
        for i in range(len(h)):
            halo_props = h.get_halo_properties(i,with_unit=False)
            if 'next_subhalo_id' in halo_props:
                parent = halo_props['sub_parent']
                if parent not in halo_children:
                    halo_children[parent] = []
                halo_children[parent].append(i)
        return halo_children

    def available_object_property_names_for_timestep(self, ts_extension, object_typetag):
        h = self._construct_halo_cat(ts_extension, object_typetag)

        halo_attributes = list(h._halo_attributes)
        if h._read_contamination:
            halo_attributes.extend(h._halo_attributes_contam)

        attrs = chain.from_iterable(tuple(always_iterable(attr)) for (attr, _len, _dtype) in halo_attributes)

        # We return all properties but the ids of the particles contained in the halo
        return [attr for attr in attrs if attr != "members"]
    
    def iterate_object_properties_for_timestep(self, ts_extension, object_typetag, property_names):
        h = self._construct_halo_cat(ts_extension, object_typetag)

        for halo_i in range(1, len(h)+1):  # AdaptaHOP catalogues start at 1
            all_data = [halo_i, halo_i]
            for k in property_names:
                pynbody_properties = h[halo_i].properties

                if k in pynbody_properties:
                    data = pynbody_properties[k]
                    # Strip the unit as Tangos expects it to be a raw number
                    data = float(data)

                all_data.append(data)
            yield all_data
