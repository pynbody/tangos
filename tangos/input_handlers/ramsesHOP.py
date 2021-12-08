from more_itertools import always_iterable
from ..util import proxy_object
from itertools import chain
from .pynbody import PynbodyInputHandler
import numpy as np
from ..log import logger
from .. import config

class RamsesCatalogueMixin:
    def create_bridge(self, f1, f2):
        import pynbody
        # Ensure that f1.dm and f2.dm are not garbage-collected
        self._f1dm = f1.dm
        self._f2dm = f2.dm

        return pynbody.bridge.OrderBridge(self._f1dm, self._f2dm, monotonic=False)

    def match_objects(self, ts1, ts2, halo_min, halo_max, dm_only=True, threshold=0.005,
                      object_typetag="halo", output_handler_for_ts2=None):
        import pynbody
        if not dm_only:
            logger.warn(
                "`match_objects` was called with dm_only=%s, but %s only supports DM-only"
                " catalogues at the moment. Falling back to DM-only.", dm_only, self.__class__.__name__
            )
            dm_only = True

        return super().match_objects(
            ts1,
            ts2,
            halo_min,
            halo_max,
            dm_only=dm_only,
            threshold=threshold,
            object_typetag=object_typetag,
            output_handler_for_ts2=output_handler_for_ts2,
            fuzzy_match_kwa={"use_family": pynbody.family.dm}
        )

class RamsesHOPInputHandler(RamsesCatalogueMixin, PynbodyInputHandler):
    """ Handling Ramses outputs with HOP halo finding (Eisenstein and Hut 1998)"""
    patterns = ["output_0????"]
    auxiliary_file_patterns = ["grp*.tag"]

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



class RamsesAdaptaHOPInputHandler(RamsesCatalogueMixin, PynbodyInputHandler):
    """ Handling Ramses outputs with AdaptaHOP halo and subhalo finding """

    patterns = ["output_0????"]
    auxiliary_file_patterns = ["tree_bricks???"]

    _excluded_adaptahop_precalculated_properties = (
        "members", "timestep", "level", "host_id", "first_subhalo_id", "next_subhalo_id",
        "pos_x", "pos_y", "pos_z", "pos", "vel_x", "vel_y", "vel_z",
        "angular_momentum_x", "angular_momentum_y", "angular_momentum_z",
        "contaminated", "m_contam", "mtot_contam", "n_contam", "ntot_contam",
    )
    _included_adaptahop_additional_properties = (
        "parent", "child", "shrink_center", "bulk_velocity", "contamination_fraction"
    )

    def _is_able_to_load(self, ts_extension):
        import pynbody
        filepath = self._extension_to_filename(ts_extension)
        try:
            f = pynbody.load(filepath)
            if self.quicker:
                logger.warn("Pynbody was able to load %r, but because 'quicker' flag is set we won't check whether it can also load the halo files", filepath)
            else:
                h = f.halos()
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

    def iterate_object_properties_for_timestep(self, ts_extension, object_typetag, property_names):
        h = self._construct_halo_cat(ts_extension, object_typetag)
        # For AdaptaHOP handler, we do not need to load the entire snaphshot to enumerate
        # properties in the halo catalogue. If pynbody supports this, ask it to do so.
        h._index_parent = False
        h.physical_units()  # make sure all units are physical before storing to database

        if "child" in property_names:
            # Construct the mapping between parent and subhalos
            map_child_parent = self._get_map_child_subhalos(ts_extension)

        for halo_i in range(1, len(h)+1):  # AdaptaHOP catalogues start at 1

            # Tangos expects data to have a finder offset, and a finder id following the stat file logic
            # I think these are irrelevant for AdaptaHOP catalogues which are derived directly from pynbody
            # Putting the finder ID twice seems to produce consistent results
            all_data = [halo_i, halo_i]

            adaptahop_halo = h[halo_i]
            precalculated_properties = adaptahop_halo.properties

            # Loop over all properties we wish to import
            for k in property_names:
                if k in self._included_adaptahop_additional_properties:
                    if k == "parent":
                        data = proxy_object.IncompleteProxyObjectFromFinderId(precalculated_properties['host_id'], 'halo')
                    elif k == "child":
                        # Determine whether halo has childs and create halo objects to it
                        try:
                            list_of_child = map_child_parent[halo_i]
                            data = [proxy_object.IncompleteProxyObjectFromFinderId(data_i, 'halo') for data_i in list_of_child]
                        except KeyError:
                            data = None
                    elif k == "shrink_center":
                        # Avoid naming confusions with already defined PynbodyProperties
                        data = (adaptahop_halo.properties['pos']).view(np.ndarray)
                    elif k == "bulk_velocity":
                        data = (adaptahop_halo.properties['vel']).view(np.ndarray)
                    elif k == "contamination_fraction":
                        data = self._compute_contamination_fraction(adaptahop_halo)
                    else:
                        raise NotImplementedError(
                            "Cannot handle property %s for halo catalogue %r" % (
                                k, self
                            )
                        )
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
            logger.warn("No halo statistics file found for timestep %r", ts_extension)

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
                    if hi.properties["npart"] > min_halo_particles:
                        yield i, i, hi.properties["npart"], 0, 0
                except (ValueError, KeyError) as e:
                    pass
