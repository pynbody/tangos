from more_itertools import always_iterable
from ..util import proxy_object
from itertools import chain
from .pynbody import PynbodyInputHandler
import numpy as np

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

    def _exclude_adaptahop_precalculated_properties(self):
        return ["members", "timestep", "level", "host_id", "first_subhalo_id", "next_subhalo_id",
                "x", "y", "z", "vx", "vy", "vz", "lx", "ly", "lz",
                "contaminated", "m_contam", "mtot_contam", "n_contam", "ntot_contam"]

    def _include_additional_properties_derived_from_adaptahop(self):
        # I don't know what actually are vx, vy, vz and lx, ly, lz, but they could be added as reformatted 3D arrays here
        return ["parent", "child", "shrink_center", "contamination_fraction"]

    @staticmethod
    def _reformat_center(adaptahop_halo):
        return np.array([adaptahop_halo.properties['x'], adaptahop_halo.properties['y'], adaptahop_halo.properties['z']])

    @staticmethod
    def _compute_contamination_fraction(adaptahop_halo):
        return float(adaptahop_halo.properties['ntot_contam'] / adaptahop_halo.properties['ntot'])

    def available_object_property_names_for_timestep(self, ts_extension, object_typetag):
        h = self._construct_halo_cat(ts_extension, object_typetag)

        halo_attributes = list(h._halo_attributes)
        if h._read_contamination:
            halo_attributes.extend(h._halo_attributes_contam)

        attrs = chain.from_iterable(tuple(always_iterable(attr)) for (attr, _len, _dtype) in halo_attributes)

        # Import all precalculated properties except conflicting ones with Tangos syntax
        property_list = [attr for attr in attrs if attr not in self._exclude_adaptahop_precalculated_properties()]
        # Add additional properties that are baisc to most Tangos databases derived
        property_list += self._include_additional_properties_derived_from_adaptahop()
        return property_list

    @staticmethod
    def _resolve_units(value):
        import pynbody
        # TODO Solve the fact that AdapataHOP stores distances in Mpc, when Tangos physical units are kpc
        if (pynbody.units.is_unit(value)):
            return float(value)
        else:
            return value

    def _get_map_child_subhalos(self, ts_extension):
        h = self._construct_halo_cat(ts_extension, 'halo')
        halo_children = {}
        for halo_i in range(1, len(h)+1):  # AdaptaHOP catalogues start at 1
            halo_props = h[halo_i].properties
           
            if halo_props['host_id'] != halo_i: # If halo isn't its own host, it is a subhalo
                parent = halo_props['host_id']
                if parent not in halo_children:
                    halo_children[parent] = []
                halo_children[parent].append(halo_i)
        return halo_children

    def iterate_object_properties_for_timestep(self, ts_extension, object_typetag, property_names):
        h = self._construct_halo_cat(ts_extension, object_typetag)

        if "child" in property_names:
            # Construct the mapping between parent and subhalos
            map_child_parent = self._get_map_child_subhalos(ts_extension)
            print(map_child_parent)
    
        for halo_i in range(1, len(h)+1):  # AdaptaHOP catalogues start at 1

            # Tangos expects data to have a finder offset, and a finder id following the stat file logic
            # I think these are irrelevant for AdaptaHOP catalogues which are derived directly from pynbody
            # Putting the finder ID twice seems to produce consistent results
            all_data = [halo_i, halo_i]

            # Loop over all properties we wish to import
            for k in property_names:

                adaptahop_halo = h[halo_i]
                precalculated_properties = h[halo_i].properties

                if k in self._include_additional_properties_derived_from_adaptahop():
                    if k == "parent":
                        data = proxy_object.IncompleteProxyObjectFromFinderId(precalculated_properties['host_id'], 'halo')
                    if k == "child":
                        data = self._get_map_child_subhalos(ts_extension)

                        # Determine whether halo has childs and create halo objects to it
                        try:
                            list_of_child = data[halo_i]
                            data = [proxy_object.IncompleteProxyObjectFromFinderId(data_i, 'halo') for data_i in list_of_child]
                        except KeyError:
                            data = None

                    if k == "shrink_center": data = self._reformat_center(adaptahop_halo)
                    if k == "contamination_fraction": data = self._compute_contamination_fraction(adaptahop_halo)
                elif k in precalculated_properties:
                    data = precalculated_properties[k]
                    # Strip the unit as Tangos expects it to be a raw number
                    data = self._resolve_units(data)
                else:
                    data = None

                all_data.append(data)
            yield all_data

