from __future__ import annotations

import glob
import os
import os.path
import pathlib
import time
import weakref
from collections import defaultdict

import numpy as np
from packaging.version import Version

from ..util import proxy_object

pynbody = None # deferred import; occurs when a PynbodyInputHandler is constructed

from typing import TYPE_CHECKING

from .. import config
from ..log import logger
from . import HandlerBase, finding

if TYPE_CHECKING:
    import pynbody

_loaded_halocats = {}

class DummyTimeStep:
    def __init__(self, filename):
        self.filename = filename


    def __repr__(self):
        return self.filename

    pass


class PynbodyInputHandler(finding.PatternBasedFileDiscovery, HandlerBase):
    pynbody_halo_class_name = None

    def __new__(cls, *args, **kwargs):
        import pynbody as pynbody_local

        min_version = "2.0.0-beta.8"

        if Version(pynbody_local.__version__) < Version(min_version):
            raise ImportError(f"Using tangos with pynbody requires pynbody {min_version} or later")

        global pynbody
        pynbody = pynbody_local

        return object.__new__(cls)

    @classmethod
    def _construct_pynbody_halos(cls, sim, *args, **kwargs):
        if cls.pynbody_halo_class_name is not None:
            kwargs['priority'] = [cls.pynbody_halo_class_name]

        return sim.halos(*args, **kwargs)

    def _is_able_to_load(self, ts_extension):
        filepath = self._extension_to_filename(ts_extension)
        try:
            f = pynbody.load(filepath)
            if self.quicker:
                logger.warning("Pynbody was able to load %r, but because 'quicker' flag is set we won't check whether it can also load the halo files", filepath)
            else:
                h = self._construct_pynbody_halos(f)

            return True
        except (OSError, RuntimeError):
            return False

    def get_timestep_properties(self, ts_extension):
        ts_filename =  self._extension_to_filename(ts_extension)
        f = pynbody.load(ts_filename)
        try:
            time_gyr = f.properties['time'].in_units("Gyr",**f.conversion_context())
        except:
            time_gyr = -1

        results = {'time_gyr': time_gyr, 'redshift': float(f.properties['z']),
                   'available': True}
        return results

    def load_timestep_without_caching(self, ts_extension, mode=None) -> pynbody.snapshot.simsnap.SimSnap:
        if mode=='partial' or mode is None:
            f = pynbody.load(self._extension_to_filename(ts_extension))
            f.physical_units()
            return f
        elif mode in ('server', 'server-partial', 'server-shared-mem'):
            from ..parallel_tasks import pynbody_server as ps
            return ps.RemoteSnapshotConnection(self, ts_extension,
                                               shared_mem = (mode == 'server-shared-mem'))
        else:
            raise NotImplementedError("Load mode %r is not implemented"%mode)

    def _build_kdtree(self, timestep, mode):
        timestep.build_tree()

    def load_region(self, ts_extension, region_specification, mode=None, expected_number_of_queries=None) -> pynbody.snapshot.simsnap.SimSnap:
        timestep = self.load_timestep(ts_extension, mode)

        timestep._tangos_cached_regions = getattr(timestep, '_tangos_cached_regions', {})

        key = (region_specification, mode)

        # we store a cache in the timestep object, so that it is automatically cleared when the timestep is
        if key in timestep._tangos_cached_regions:
            return timestep._tangos_cached_regions[key]

        result = self._load_region_uncached(timestep, ts_extension, region_specification, mode, expected_number_of_queries)
        timestep._tangos_cached_regions[key] = result

        return result

    def _load_region_uncached(self, timestep, ts_extension, region_specification, mode=None, expected_number_of_queries=None):
        if expected_number_of_queries is not None and expected_number_of_queries>config.pynbody_build_kdtree_threshold_count:
            self._build_kdtree(timestep, mode)
        if mode is None:
            return timestep[region_specification]
        elif mode=='server':
            return timestep.get_view(region_specification)
        elif mode=='server-shared-mem':
            from ..parallel_tasks import pynbody_server as ps
            simsnap = timestep.shared_mem_view
            return simsnap[region_specification].get_copy_on_access_simsnap()
        elif mode=='server-partial':
            load_index = timestep.get_index_list(region_specification)
            logger.info("Partial load %r, taking %d particles",ts_extension,len(load_index))
            f = pynbody.load(self._extension_to_filename(ts_extension), take=load_index)
            f.physical_units()
            return f
        elif mode=='partial':
            raise NotImplementedError("For partial loading to work with custom regions, you need load-mode=server-partial (instead of load-mode=partial)")
        else:
            raise NotImplementedError("Load mode %r is not implemented"%mode)

    def load_object(self, ts_extension, finder_id, finder_offset, object_typetag='halo', mode=None) -> pynbody.snapshot.simsnap.SimSnap:
        if mode=='partial':
            h = self.get_catalogue(ts_extension, object_typetag)
            h_file = h.load_copy(finder_id)
            h_file.physical_units()
            return h_file
        elif mode=='server' :
            timestep = self.load_timestep(ts_extension, mode)
            from ..parallel_tasks import pynbody_server as ps
            return timestep.get_view(
                ps.snapshot_queue.ObjectSpecification(finder_id, finder_offset, object_typetag))
        elif mode=='server-partial':
            timestep = self.load_timestep(ts_extension, mode)
            from ..parallel_tasks import pynbody_server as ps
            load_index = timestep.get_index_list(
                ps.snapshot_queue.ObjectSpecification(finder_id, finder_offset, object_typetag)
            )

            logger.info("Partial load %r, taking %d particles", ts_extension, len(load_index))
            f = pynbody.load(self._extension_to_filename(ts_extension), take=load_index)
            f.physical_units()
            return f
        elif mode=='server-shared-mem':
            timestep = self.load_timestep(ts_extension, mode)
            from ..parallel_tasks import pynbody_server as ps
            index_list = timestep.get_index_list(
                ps.snapshot_queue.ObjectSpecification(finder_id, finder_offset, object_typetag)
            )
            return timestep.shared_mem_view[index_list].get_copy_on_access_simsnap()

        elif mode is None:
            h = self.get_catalogue(ts_extension, object_typetag)
            return h[finder_id]
        else:
            raise NotImplementedError("Load mode %r is not implemented"%mode)

    def load_tracked_region(self, ts_extension, track_data, mode=None) -> pynbody.snapshot.simsnap.SimSnap:
        from ..parallel_tasks import pynbody_server as ps

        timestep = self.load_timestep(ts_extension, mode)
        if mode is None:
            indices = self._get_indices_for_snapshot(timestep, track_data)
            return timestep[indices]
        elif mode=='partial':
            indices = self._get_indices_for_snapshot(timestep, track_data)
            return pynbody.load(timestep.filename, take=indices)
        elif mode=='server':
            return timestep.get_view(
                ps.snapshot_queue.TrackingSpecification(track_data.id, track_data)
            )
        elif mode=='server-shared-mem':
            indices = self._get_indices_for_snapshot(timestep.shared_mem_view, track_data)
            return timestep.shared_mem_view[indices].get_copy_on_access_simsnap()
        else:
            raise NotImplementedError("Load mode %r is not implemented for trackers"%mode)


    def _get_indices_for_snapshot(self, f, track_data):
        if track_data is None:
            return np.array([], dtype=np.intp)
        pt = track_data.particles
        if track_data.use_iord is True:

            dm_part = f.dm[np.isin(f.dm['iord'], pt)]

            try:
                star_part = f.star[np.isin(f.star['iord'], pt)]
            except KeyError:
                star_part = f[0:0]

            try:
                gas_part = f.gas[np.isin(f.gas['iord'], pt)]
            except KeyError:
                gas_part = f[0:0]

            # fx = dm_part.union(star_part)
            # fx = fx.union(gas_part)
            # return fx
            ilist = np.hstack((dm_part.get_index_list(f),
                               star_part.get_index_list(f),
                               gas_part.get_index_list(f)))
            ilist = np.sort(ilist)
            return ilist
        else:
            return pt



    def get_catalogue(self, ts_extension, object_typetag) -> pynbody.halo.HaloCatalogue:
        if object_typetag!= 'halo':
            raise ValueError("Unknown object type %r" % object_typetag)
        f = self.load_timestep(ts_extension)
        h = _loaded_halocats.get(id(f), lambda: None)()
        if h is None:
            h = self._construct_pynbody_halos(f)
            if isinstance(h, pynbody.halo.subfind.SubfindCatalogue) or isinstance(h, pynbody.halo.subfindhdf.SubFindHDFHaloCatalogue):
                # ugly fix - loads groups by default, wanted halos
                h = self._construct_pynbody_halos(f, subs=True)
            if hasattr(h, 'precalculate'):
                # speeds up getting individual halos etc:
                h.precalculate()
            _loaded_halocats[id(f)] = weakref.ref(h)
            f._db_current_halocat = h # keep alive for lifetime of simulation
        return h  # pynbody.halo.AmigaGrpCatalogue(f)


    def match_objects(self, ts1, ts2, halo_min, halo_max,
                      dm_only=False, threshold=0.005, object_typetag='halo',
                      output_handler_for_ts2=None,
                      fuzzy_match_kwa={}):
        if dm_only:
            only_family=pynbody.family.dm
        else:
            only_family=None

        f1 = self.load_timestep(ts1)
        h1 = self.get_catalogue(ts1, object_typetag)

        if output_handler_for_ts2:
            assert isinstance(output_handler_for_ts2, PynbodyInputHandler)
            f2 = output_handler_for_ts2.load_timestep(ts2)
            h2 = output_handler_for_ts2.get_catalogue(ts2, object_typetag)
        else:
            f2 = self.load_timestep(ts2)
            h2 = self.get_catalogue(ts2, object_typetag)

        if halo_max is None:
            halo_max = max(len(h2), len(h1))

        matches = self.create_bridge(f1, f2).fuzzy_match_halos(
            h1, h2, threshold=threshold, use_family=only_family,
            **fuzzy_match_kwa,
        )

        del_keys = []
        for k in matches:
            if k < halo_min or k > halo_max:
                del_keys.append(k)

        for k in del_keys:
            del matches[k]

        return matches


    @classmethod
    def create_bridge(cls, f1, f2):
        return f1.bridge(f2)

    def enumerate_objects(self, ts_extension, object_typetag="halo", min_halo_particles=config.min_halo_particles):
        if self._can_enumerate_objects_from_statfile(ts_extension, object_typetag):
            yield from self._enumerate_objects_from_statfile(ts_extension, object_typetag)
        else:
            logger.warning("No %s statistics file found for timestep %r", object_typetag, ts_extension)

            snapshot_keep_alive = self.load_timestep(ts_extension)

            try:
                h = self.get_catalogue(ts_extension, object_typetag)
            except Exception as e:
                logger.warning("Unable to read %ss using pynbody; assuming step has none", object_typetag)
                return

            logger.warning(" => enumerating %ss directly using pynbody", object_typetag)


            h.load_all()


            for hi in h:
                try:
                    i = hi.properties['halo_number']
                    if len(hi.dm) + len(hi.star) + len(hi.gas) >= min_halo_particles:
                        yield i, i, len(hi.dm), len(hi.star), len(hi.gas)
                except (ValueError, KeyError) as e:
                    pass

    def get_properties(self):
        timesteps = self.enumerate_timestep_extensions()
        try:
            f = self.load_timestep_without_caching(next(timesteps))
            if self.quicker:
                res_kpc = self._estimate_spatial_resolution_quicker(f)
                res_msol = self._estimate_mass_resolution_quicker(f)
            else:
                res_kpc = self._estimate_spatial_resolution(f)
                res_msol = self._estimate_mass_resolution(f)
            return {'approx_resolution_kpc': res_kpc, 'approx_resolution_Msol': res_msol}

        except StopIteration:
            return {}

    def _estimate_spatial_resolution(self, f):
        f.physical_units()
        if "eps" in f.dm.loadable_keys():
            # Interpret the eps array as a softening, and assume that it is not a comoving softening (as the
            # pynbody units system might naively tell us) but actually already a physical softening. Note that
            # whether or not this is a correct interpretation depends on the code in use, and the flags passed
            # to that code.
            return float(f.dm['eps'].in_units('kpc a').min())
        else:
            # There is no softening information available, so take a best guess as to what a reasonable
            # softening might be as 1/100th of the mean interparticle distance (in the deepest zoom level)
            tot_box_mass = f.dm['mass'].sum()
            min_mass = f.dm['mass'].min()
            frac_mass = min_mass/tot_box_mass
            frac_length = frac_mass ** (1. / 3)
            estimated_eps = 0.01 * frac_length * f.properties['boxsize'].in_units('kpc a', **f.conversion_context())
            return float(estimated_eps)

    def _estimate_spatial_resolution_quicker(self, f):
        interparticle_distance = float(f.properties['boxsize'].in_units("kpc a",**f.conversion_context()))/(float(len(f))**(1./3))
        res = 0.01*interparticle_distance
        logger.warning("Because 'quicker' flag is set, estimating res %.2g kpc from the file size; this could be inaccurate",
                    res)
        logger.warning(" -- it will certainly be wrong for e.g. zoom simulations")
        return res

    def _estimate_mass_resolution(self, f):
        f.physical_units()
        if "mass" in f.dm.loadable_keys():
            min_simulation_mass = f.dm['mass'].min()
            return float(min_simulation_mass)

    def _estimate_mass_resolution_quicker(self, f):
        import pynbody.analysis.cosmology as cosmo
        rho_m = cosmo.rho_M(f, z=0, unit="Msol kpc**-3 a**-3")
        volume_box = float(f.properties['boxsize'].in_units("kpc a",**f.conversion_context()) ** 3)
        estimated_part_mass = rho_m * volume_box / len(f)
        logger.warning("Because 'quicker' flag is set, estimating mass res %.2g msol from the file size; this could be inaccurate",
                    estimated_part_mass)
        logger.warning(" -- it will certainly be wrong for e.g. zoom simulations")
        return estimated_part_mass


class GadgetSubfindInputHandler(PynbodyInputHandler):
    patterns = ["snapshot_???"]
    auxiliary_file_patterns =["groups_???"]

    snap_class_name = "pynbody.snapshot.gadget.GadgetSnap" # annoyingly, has to be string because pynbody isn't imported at module import time
    catalogue_class_name = "pynbody.halo.subfind.SubfindCatalogue"

    _property_prefix_for_type = {'halo': 'sub_'}

    _sub_parent_names = ['sub_groupNr']

    _hidden_properties = ['group_len', 'group_off', 'Nsubs', 'groupNr', 'len', 'off']

    def _is_able_to_load(self, filepath):
        try:
            f = eval(self.snap_class_name)(pathlib.Path(filepath))
            h = f.halos()
            if isinstance(h, eval(self.catalogue_class_name)):
                return True
        except (OSError, RuntimeError):
            return False

    def load_object(self, ts_extension, finder_id, finder_offset, object_typetag='halo', mode=None):
        if mode=='subfind-properties':
            h = self.get_catalogue(ts_extension, object_typetag)
            return h.get_properties_one_halo(finder_id)
        else:
            return super().load_object(ts_extension, finder_id, finder_offset, object_typetag, mode)

    def _construct_group_cat(self, ts_extension):
        f = self.load_timestep(ts_extension)
        h = _loaded_halocats.get(id(f)+1, lambda: None)()
        if h is None:
            h = self._construct_pynbody_halos(f)
            assert isinstance(h, eval(self.catalogue_class_name))
            _loaded_halocats[id(f)+1] = weakref.ref(h)
            f._db_current_groupcat = h  # keep alive for lifetime of simulation
        return h

    def get_catalogue(self, ts_extension, object_typetag):
        if object_typetag== 'halo':
            return super().get_catalogue(ts_extension, object_typetag)
        elif object_typetag== 'group':
            return self._construct_group_cat(ts_extension)
        else:
            raise ValueError("Unknown halo type %r" % object_typetag)

    def available_object_property_names_for_timestep(self, ts_extension, object_typetag):
        cat = self.get_catalogue(ts_extension, object_typetag)
        properties = list(cat.get_properties_one_halo(0).keys())
        pynbody_prefix = self._property_prefix_for_type.get(object_typetag, '')
        for i,p in enumerate(properties):
            if p.startswith(pynbody_prefix):
                new_p = p[len(pynbody_prefix):]
                if new_p.startswith("_"):
                    new_p = new_p[1:]
                properties[i] = new_p
            if p in self._sub_parent_names:
                properties[i] = 'parent'
            if p == 'children':
                properties[i] = 'child'

        properties = [p for p in properties if p not in self._hidden_properties]
        return properties


    def iterate_object_properties_for_timestep(self, ts_extension, object_typetag, property_names):
        h = self.get_catalogue(ts_extension, object_typetag)

        pynbody_prefix = self._property_prefix_for_type.get(object_typetag, '')

        all_properties = h.get_properties_all_halos(with_units=False)

        for i in range(len(h)):
            all_data = [i, i]
            for k in property_names:
                if k=='parent':
                    for adapted_k in self._sub_parent_names:
                        if adapted_k in all_properties.keys():
                            break
                else:
                    adapted_k = pynbody_prefix + k
                    if adapted_k not in all_properties:
                        adapted_k = pynbody_prefix + "_" + k

                if adapted_k in all_properties:
                    data = all_properties[adapted_k][i]
                    if adapted_k in self._sub_parent_names and data is not None:
                        # turn into a link
                        data = proxy_object.IncompleteProxyObjectFromFinderId(data, 'group')
                elif k=='child' and "children" in all_properties:
                    # subfind does not actually store a list of children; but pynbody infers it from
                    # the parent data in the halo catalogue. Note children are always halos, even if
                    # the parent is group.
                    data = [proxy_object.IncompleteProxyObjectFromFinderId(data_i, 'halo')
                            for data_i in all_properties['children'][i]]
                else:
                    data = None

                all_data.append(data)
            yield all_data

class Gadget4HDFSubfindInputHandler(GadgetSubfindInputHandler):
    patterns = ["snapshot_???.hdf5", "snapshot_???.0.hdf5", "snap_???.hdf5", "snap_???.0.hdf5"]
    auxiliary_file_patterns =["fof_subhalo_tab_???.hdf5", "fof_subhalo_tab_???.0.hdf5"]
    snap_class_name = "pynbody.snapshot.gadgethdf.GadgetHDFSnap"
    catalogue_class_name = "pynbody.halo.subfindhdf.Gadget4SubfindHDFCatalogue"

    _property_prefix_for_type = {'halo': 'Subhalo', 'group': 'Group'}

    _sub_parent_names = ['SubhaloGroupNr', 'SubhaloGrNr']

    _hidden_properties = ['Len', 'LenType', 'OffsetType', 'ParentRank', 'RankInGr', 'Nr', 'Ascale', 'FirstSub',
                          'OffsetType']

    def _transform_extension(self, extension_name):
        if extension_name.endswith(".0.hdf5"):
            return extension_name[:-7]
        else:
            return extension_name

class Gadget4HBTPlusInputHandler(Gadget4HDFSubfindInputHandler):
    auxiliary_file_patterns = ["SubSnap_???.hdf5", "SubSnap_???.0.hdf5"]
    catalogue_class_name = "pynbody.halo.hbtplus.HBTPlusCatalogueWithGroups"
    _sub_parent_names = [] # although HBTplus stores this as 'HostHaloId', pynbody already translates it to 'parent'
    _property_prefix_for_type = {'group': 'Group'}

    @classmethod
    def _construct_pynbody_halos(cls, sim, *args, **kwargs):
        if kwargs.pop('subs', False):
            h = pynbody.halo.hbtplus.HBTPlusCatalogue(sim)
            h.load_all()
            return h
        else:
            return super()._construct_pynbody_halos(sim, *args, **kwargs)

    def _construct_group_cat(self, ts_extension):
        sim = self.load_timestep(ts_extension)
        groups = super()._construct_pynbody_halos(sim, subs=False)
        # can't call super()._construct_group_cat because that verifies the type of the catalogue, which is wrong
        # until we do the modification below

        hbt_halos = self._construct_pynbody_halos(sim, subs=True)
        return hbt_halos.with_groups_from(groups)

    def _is_able_to_load(self, filepath):
        try:
            f = eval(self.snap_class_name)(pathlib.Path(filepath))
            h = pynbody.halo.hbtplus.HBTPlusCatalogue(f)
            return True
        except (OSError, RuntimeError):
            return False

    def match_objects(self, ts1, ts2, halo_min, halo_max,
                      dm_only=False, threshold=0.005, object_typetag='halo',
                      output_handler_for_ts2=None,
                      fuzzy_match_kwa={}):

        if object_typetag=='halo' and output_handler_for_ts2 is self:
            # specialised case
            f1 = self.load_timestep(ts1)
            h1 = self.get_catalogue(ts1, 'halo')
            f2 = self.load_timestep(ts2)
            h2 = self.get_catalogue(ts2, 'halo')

            id1_to_number1 = h1.number_mapper.index_to_number
            id2_to_number2 = h2.number_mapper.index_to_number

            props1 = h1.get_properties_all_halos()
            props2 = h2.get_properties_all_halos()

            id1_to_trackid = props1['TrackId']
            id2_to_trackid = props2['TrackId']

            trackid_to_id2 = {trackid: id2 for id2,trackid in enumerate(id2_to_trackid)}
            number1_to_number2 = {id1_to_number1(id1): [(id2_to_number2(trackid_to_id2[trackid]), 1.0)]
                                  if trackid in trackid_to_id2 else []
                                  for id1, trackid in enumerate(id1_to_trackid)}

            return number1_to_number2


        else:
            return super().match_objects(ts1, ts2, halo_min, halo_max, dm_only, threshold, object_typetag,
                                         output_handler_for_ts2, fuzzy_match_kwa)



class GadgetRockstarInputHandler(PynbodyInputHandler):
    patterns = ["snapshot_???"]
    auxiliary_file_patterns = ["halos_*.bin"]

    def _is_able_to_load(self, filepath):
        try:
            f = pynbody.load(filepath)
            h = pynbody.halo.rockstar.RockstarCatalogue(f)
            return True
        except (OSError, RuntimeError):
            return False

class RamsesCatalogueMixin:
    def create_bridge(self, f1, f2):
        import pynbody

        # Ensure that f1.dm and f2.dm are not garbage-collected
        self._f1dm = f1.dm
        self._f2dm = f2.dm

        return pynbody.bridge.OrderBridge(self._f1dm, self._f2dm, monotonic=False)

    def match_objects(self, ts1, ts2, halo_min, halo_max, dm_only=True, threshold=0.005,
                      object_typetag="halo", output_handler_for_ts2=None):
        if not dm_only:
            logger.warning(
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
            output_handler_for_ts2=output_handler_for_ts2
        )


class AHFInputHandler(PynbodyInputHandler):
    pynbody_halo_class_name = "AHFCatalogue"

    _included_additional_properties = (
        "parent", "child", "shrink_center", "bulk_velocity",
    )

    _excluded_precalculated_properties = (
        "boxsize", "time", "hostHalo", "Xc", "Yc", "Zc",
        "VXc", "VYc", "VZc",
    )

    def available_object_property_names_for_timestep(self, ts_extension, object_typetag):
        h = self.get_catalogue(ts_extension, object_typetag)

        return (
            [
                key for key in h[0].properties.keys()
                if key not in self._excluded_precalculated_properties
            ] + list(self._included_additional_properties)
        )


    def iterate_object_properties_for_timestep(self, ts_extension, object_typetag, property_names):
        h = self.get_catalogue(ts_extension, object_typetag)

        h.physical_units()

        # Manually mapping IDs etc used to be setup here, but should no longer be required with pynbody v2

        for halo in h:
            # Tangos expect us to yield first the finder offset (index in the pynbody catalogue, halo_id in our case)
            # and second the finder_id (unique number associated to the halo, ID in our case).
            all_data = [halo.properties["halo_id"], halo.properties["ID"]]

            halo_props = halo.properties
            for k in property_names:
                if k == "parent":
                    parent_ID = halo_props['hostHalo']
                    if parent_ID != -1:
                        # If halo is not its own parent, link to parent
                        data = proxy_object.IncompleteProxyObjectFromFinderId(
                            parent_ID,
                            'halo'
                        )
                    else:
                        # Otherwise link to itself
                       data = proxy_object.IncompleteProxyObjectFromFinderId(
                            halo_props['halo_id'],
                            'halo'
                        )
                elif k == "child":
                    data = [
                        proxy_object.IncompleteProxyObjectFromFinderId(ichild, 'halo')
                        for ichild in halo_props['children']
                    ]
                elif k == "shrink_center":
                    data = np.array([halo_props[k] for k in ("Xc", "Yc", "Zc")])
                elif k == "bulk_velocity":
                    data = np.array([halo_props[k] for k in ("VXc", "VYc", "VZc")])
                else:
                    data = halo_props[k]
                all_data.append(data)

            yield all_data


class GadgetAHFInputHandler(AHFInputHandler):
    patterns = ["snapshot_???"]
    auxiliary_file_patterns = ["*.AHF_particlesSTARDUST"]


class RamsesAHFInputHandler(RamsesCatalogueMixin, AHFInputHandler):
    patterns = ["output_?????"]
    auxiliary_file_patterns = ["output_?????*z*AHF_halos"]

    _excluded_precalculated_properties = (
        "boxsize", "time", "hostHalo", "Xc", "Yc", "Zc",
        "VXc", "VYc", "VZc",
        "npart","n_gas", "n_star", # will be accessible through NDM()
        "a", "omegaM0", "omegaL0","h", "fstart", "ovdens", "nbins",
        "halo_id", "ID" # will be accesisble through finder_id()
    )

class ChangaInputHandler(PynbodyInputHandler):
    flags_include = ["dPhysDenMin", "dCStar", "dTempMax",
                     "dESN", "bLowTCool", "bSelfShield", "dExtraCoolShutoff",
                     "dHubble0", "dOmega0", "dLambda", "dMsolUnit", "dKpcUnit"]

    patterns = ["*.00???","*.00????","*.0????"]


    def get_properties(self):
        parent_prop_dict = super().get_properties()

        pfile = self._get_paramfile_path()

        if pfile is None:
            logger.warning("Param file cannot be found - no simulation properties will be available")
            return {}
        else:
            logger.info("Param file is %s", pfile)

        pfile_dict = self._param_file_to_dict(pfile)
        log_path, prop_dict = self._get_log_path(pfile, pfile_dict)

        if log_path:
            prop_dict.update(self._get_properties_from_log(log_path))

        prop_dict.update(self._filter_paramfile_properties(pfile_dict))
        prop_dict.update(parent_prop_dict)

        return prop_dict

    def _get_paramfile_path(self):
        try:
            pfile = self._get_param_file_for_output(self._extension_to_filename(""))
        except RuntimeError:
            pfile = None
        return pfile

    def _get_log_path(self, paramfile_name, paramfile_dict):
        prop_dict = {}
        log_fn = paramfile_dict.get("achOutName","") + ".log"
        log_path = paramfile_name.split("/")[:-1]
        log_path.append(log_fn)
        log_path = "/".join(log_path)
        if os.path.exists(log_path):
            logger.info("Log file is %s", log_path)
        else:
            logger.warning("Cannot find log file (%s)", log_path)
            log_path = None
        return log_path, prop_dict

    def _filter_paramfile_properties(self, pfile_dict):
        filtered_pfile_dict = {}
        for f in self.flags_include:
            if f in pfile_dict:
                filtered_pfile_dict[f] = pfile_dict[f]
        return filtered_pfile_dict

    def _get_properties_from_log(self, log_path):
        prop_dict = {}
        with open(log_path) as f:
            for l in f:
                if "# Code compiled:" in l:
                    prop_dict["compiled"] = time.strptime(
                        l.split(": ")[1].strip(), "%b %d %Y %H:%M:%S")
                if "# Preprocessor macros: " in l:
                    prop_dict["macros"] = l.split(": ")[1].strip()
                    break
        return prop_dict

    @staticmethod
    def _get_param_file_for_output(output_file):
        """Work out the param file corresponding to the
        specified output"""

        q = "/".join(output_file.split("/")[:-1])
        if len(q) != 0:
            path = "/".join(output_file.split("/")[:-1]) + "/"
        else:
            path = ""

        candidates = glob.glob(path + "*.param")

        if len(candidates) == 0:
            candidates = glob.glob(path + "../*.param")

        if len(candidates) == 0:
            raise RuntimeError("No .param file in " + path + \
                                " (or parent) -- please supply or create tipsy.info manually")

        candidates = [x for x in candidates if "direct" not in x and "mpeg_encode" not in x]

        if len(candidates) > 1:
            raise RuntimeError("Can't resolve ambiguity -- too many param files matching " + \
                                path)

        return candidates[0]

    @staticmethod
    def _param_file_to_dict(param_file):
        f = open(param_file)
        out = {}

        for line in f:
            try:
                s = line.split()
                if s[1] == "=" and "#" not in s[0]:
                    key = s[0]
                    v = s[2]

                    if key[0] == "d":
                        v = float(v)
                    elif key[0] == "i" or key[0] == "n" or key[0] == "b":
                        v = int(v)

                    out[key] = v
            except (IndexError, ValueError):
                pass
        return out

class ChangaIgnoreIDLInputHandler(ChangaInputHandler):
    pynbody_halo_class_name = "AHFCatalogue"
    halo_stat_file_class_name = "AHFStatFile"

    enable_autoselect = False

class ChangaUseIDLInputHandler(ChangaInputHandler):
    pynbody_halo_class_name = "AmigaGrpCatalogue"
    halo_stat_file_class_name = "AmigaIDLStatFile"
    auxiliary_file_patterns = ["*.amiga.grp"]

    enable_autoselect = False

class ChangaAHFv1InputHandler(ChangaInputHandler):
    patterns=[] #emtpy so that this is only used when explicitly asked for
    @classmethod
    def _construct_pynbody_halos(cls, sim, *args, **kwargs):
        kwargs['halo_numbers'] = 'v1'
        return super()._construct_pynbody_halos(sim, *args, **kwargs)


from . import caterpillar, eagle, ramsesHOP

RamsesHOPInputHandler = ramsesHOP.RamsesHOPInputHandler
