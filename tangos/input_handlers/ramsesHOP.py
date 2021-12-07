from more_itertools import always_iterable
from ..util import proxy_object
from itertools import chain
from .pynbody import PynbodyInputHandler
import numpy as np
from ..log import logger
from .. import config

class RamsesHOPInputHandler(PynbodyInputHandler):
    """ Handling Ramses outputs with HOP halo finding (Eisenstein and Hut 1998)"""
    patterns = ["output_0????"]
    auxiliary_file_patterns = ["grp*.tag"]

    def match_objects(self, ts1, ts2, halo_min, halo_max, dm_only=False, threshold=0.005,
                      object_typetag="halo", output_handler_for_ts2=None):
        import pynbody
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

    def _is_able_to_load(self, ts_extension):
        import pynbody
        filepath = self._extension_to_filename(ts_extension)
        try:
            f = pynbody.load(filepath)
            if self.quicker:
                logger.warn("Pynbody was able to load %r, but because 'quicker' flag is set we won't check whether it can also load the halo files", filepath)
            else:
                h = pynbody.halo.hop.HOPCatalogue(f)
            return True
        except (IOError, RuntimeError):
            return False



class RamsesAdaptaHOPInputHandler(PynbodyInputHandler):
    """ Handling Ramses outputs with AdaptaHOP halo and subhalo finding """

    patterns = ["output_0????"]
    auxiliary_file_patterns = ["tree_bricks???"]

    _excluded_adaptahop_precalculated_properties = (
        "members", "timestep", "level", "host_id", "first_subhalo_id", "next_subhalo_id",
        "pos_x", "pos_y", "pos_z", "pos", "vel_x", "vel_y", "vel_z",
        "angular_momentum_x", "angular_momentum_y", "angular_momentum_z",
        "contaminated", "m_contam", "mtot_contam", "n_contam", "ntot_contam"
    )
    _included_adaptahop_additional_properties = (
        "parent", "child", "shrink_center", "bulk_velocity", "contamination_fraction"
    )

    def match_objects(self, ts1, ts2, halo_min, halo_max, dm_only=False, threshold=0.005,
                      object_typetag="halo", output_handler_for_ts2=None):
        import pynbody
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

    def _is_able_to_load(self, ts_extension):
        import pynbody
        filepath = self._extension_to_filename(ts_extension)
        try:
            f = pynbody.load(filepath)
            if self.quicker:
                logger.warn("Pynbody was able to load %r, but because 'quicker' flag is set we won't check whether it can also load the halo files", filepath)
            else:
                h = f.halos(index_parent=False)
                return isinstance(h, pynbody.halo.adaptahop.BaseAdaptaHOPCatalogue)
            return True
        except (IOError, RuntimeError):
            return False


    @staticmethod
    def _compute_contamination_fraction(adaptahop_halo):
        # Deal with the potential case that contamination fraction has not been computed through AdaptaHOP
        try:
            contamination_fraction = float(adaptahop_halo.properties['ntot_contam'] / adaptahop_halo.properties['ntot'])
        except KeyError:
            logger.warn("Ignoring import of contamination fraction which has not been stored on disk by AdaptaHOP")
            contamination_fraction = None
        return contamination_fraction

    def available_object_property_names_for_timestep(self, ts_extension, object_typetag):
        h = self._construct_halo_cat(ts_extension, object_typetag)

        halo_attributes = list(h._halo_attributes)
        if h._read_contamination:
            halo_attributes.extend(h._halo_attributes_contam)

        attrs = chain.from_iterable(tuple(always_iterable(attr)) for (attr, _len, _dtype) in halo_attributes)

        # Import all precalculated properties except conflicting ones with Tangos syntax
        property_list = [attr for attr in attrs if attr not in self._excluded_adaptahop_precalculated_properties]
        # Add additional properties that can be derived from adaptahop
        property_list += self._included_adaptahop_additional_properties
        return property_list

    @staticmethod
    def _resolve_units(value):
        import pynbody
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

    def iterate_object_properties_for_timestep(self, ts_extension, object_typetag, property_names, index_parent=True):
        h = self._construct_halo_cat(ts_extension, object_typetag)
        h._index_parent = index_parent

        if "child" in property_names:
            # Construct the mapping between parent and subhalos
            map_child_parent = self._get_map_child_subhalos(ts_extension)

        for halo_i in range(1, len(h)+1):  # AdaptaHOP catalogues start at 1

            # Tangos expects data to have a finder offset, and a finder id following the stat file logic
            # I think these are irrelevant for AdaptaHOP catalogues which are derived directly from pynbody
            # Putting the finder ID twice seems to produce consistent results
            all_data = [halo_i, halo_i]

            adaptahop_halo = h[halo_i]

            if not self._select_halo(adaptahop_halo, min_halo_particles=0):
                continue
            precalculated_properties = h[halo_i].properties
            adaptahop_halo.physical_units() # make sure all units are physical before storing to database

            # Loop over all properties we wish to import
            for k in property_names:
                if k in self._included_adaptahop_additional_properties:
                    if k == "parent":
                        data = proxy_object.IncompleteProxyObjectFromFinderId(precalculated_properties['host_id'], 'halo')
                    if k == "child":
                        # Determine whether halo has childs and create halo objects to it
                        try:
                            list_of_child = map_child_parent[halo_i]
                            data = [proxy_object.IncompleteProxyObjectFromFinderId(data_i, 'halo') for data_i in list_of_child]
                        except KeyError:
                            data = None

                    # Avoid naming confusions with already defined PynbodyProperties
                    if k == "shrink_center": data = (adaptahop_halo.properties['pos']).view(np.ndarray)
                    if k == "bulk_velocity": data = (adaptahop_halo.properties['vel']).view(np.ndarray)
                    if k == "contamination_fraction": data = self._compute_contamination_fraction(adaptahop_halo)
                elif k in precalculated_properties:
                    data = precalculated_properties[k]
                    # Strip the unit as Tangos expects it to be a raw number
                    data = self._resolve_units(data)
                else:
                    data = None

                all_data.append(data)
            logger.info("Done with Halo %i" % halo_i)
            yield all_data

    def enumerate_objects(self, ts_extension, object_typetag="halo", min_halo_particles=config.min_halo_particles):
        if self._can_enumerate_objects_from_statfile(ts_extension, object_typetag):
            for X in self._enumerate_objects_from_statfile(ts_extension, object_typetag):
                yield X
        else:
            logger.warn("No halo statistics file found for timestep %r",ts_extension)

            try:
                h = self._construct_halo_cat(ts_extension, object_typetag)
                h._index_parent = False
            except:
                logger.warn("Unable to read %ss using pynbody; assuming step has none", object_typetag)
                return

            logger.warn(" => enumerating %ss directly using pynbody", object_typetag)
            istart = 1

            for i in range(istart, len(h)+istart):
                try:
                    hi = h[i]
                    hi.properties

                    if self._select_halo(hi, min_halo_particles):
                        yield i, i, hi.properties["npart"], 0, 0
                except (ValueError, KeyError) as e:
                    pass

    @staticmethod
    def _select_halo(halo, min_halo_particles):
        properties = halo.properties
        n_contam = properties.get("n_contam", 0)
        npart = properties["npart"]

        # Filter out halos that are 100% contaminated
        return (npart > min_halo_particles) and (npart > n_contam)
