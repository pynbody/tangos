import glob
import os
import os.path
import time
import weakref
from collections import defaultdict
from itertools import chain

import numpy as np
from more_itertools import always_iterable

from ..util import proxy_object

pynbody = None # deferred import; occurs when a PynbodyInputHandler is constructed

from .. import config
from ..log import logger
from . import HandlerBase, finding

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

        if pynbody_local.__version__<"1.2.2":
            raise ImportError("Using tangos with pynbody requires pynbody 1.2.2 or later")

        global pynbody
        pynbody = pynbody_local

        return object.__new__(cls)

    @classmethod
    def _construct_pynbody_halos(cls, sim, *args, **kwargs):
        if cls.pynbody_halo_class_name is None:
            return sim.halos(*args, **kwargs)
        else:
            halo_class = getattr(pynbody.halo, cls.pynbody_halo_class_name)
            return halo_class(sim, *args, **kwargs)

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

    def load_timestep_without_caching(self, ts_extension, mode=None):
        if mode=='partial' or mode is None:
            f = pynbody.load(self._extension_to_filename(ts_extension))
            f.physical_units()
            return f
        elif mode=='server' or mode=='server-partial':
            from ..parallel_tasks import pynbody_server as ps
            return ps.RemoteSnapshotConnection(self,ts_extension)
        else:
            raise NotImplementedError("Load mode %r is not implemented"%mode)

    def load_region(self, ts_extension, region_specification, mode=None):
        if mode is None:
            timestep = self.load_timestep(ts_extension, mode)
            return timestep[region_specification]
        elif mode=='server':
            timestep = self.load_timestep(ts_extension, mode)
            return timestep.get_view(region_specification)
        elif mode=='server-partial':
            timestep = self.load_timestep(ts_extension, mode)
            view = timestep.get_view(region_specification)
            load_index = view['remote-index-list']
            logger.info("Partial load %r, taking %d particles",ts_extension,len(load_index))
            f = pynbody.load(self._extension_to_filename(ts_extension), take=load_index)
            f.physical_units()
            return f
        elif mode=='partial':
            raise NotImplementedError("For partial loading to work with custom regions, you need load-mode=server-partial (instead of load-mode=partial)")
        else:
            raise NotImplementedError("Load mode %r is not implemented"%mode)

    def load_object(self, ts_extension, finder_id, finder_offset, object_typetag='halo', mode=None):
        if mode=='partial':
            h = self._construct_halo_cat(ts_extension, object_typetag)
            h_file = h.load_copy(finder_offset)
            h_file.physical_units()
            return h_file
        elif mode=='server':
            timestep = self.load_timestep(ts_extension, mode)
            from ..parallel_tasks import pynbody_server as ps
            return timestep.get_view(ps.ObjectSpecification(finder_id, finder_offset, object_typetag))
        elif mode=='server-partial':
            timestep = self.load_timestep(ts_extension, mode)
            from ..parallel_tasks import pynbody_server as ps
            view = timestep.get_view(ps.ObjectSpecification(finder_id, finder_offset, object_typetag))
            load_index = view['remote-index-list']
            logger.info("Partial load %r, taking %d particles", ts_extension, len(load_index))
            f = pynbody.load(self._extension_to_filename(ts_extension), take=load_index)
            f.physical_units()
            return f
        elif mode is None:
            h = self._construct_halo_cat(ts_extension, object_typetag)
            return h[finder_offset]
        else:
            raise NotImplementedError("Load mode %r is not implemented"%mode)

    def load_tracked_region(self, ts_extension, track_data, mode=None):
        f = self.load_timestep(ts_extension, mode)
        indices = self._get_indices_for_snapshot(f, track_data)
        if mode=='partial':
            return pynbody.load(f.filename, take=indices)
        elif mode is None:
            return f[indices]
        else:
            raise NotImplementedError("Load mode %r is not implemented"%mode)


    def _get_indices_for_snapshot(self, f, track_data):
        pt = track_data.particles
        if track_data.use_iord is True:

            dm_part = f.dm[np.in1d(f.dm['iord'], pt)]

            try:
                star_part = f.star[np.in1d(f.star['iord'], pt)]
            except KeyError:
                star_part = f[0:0]

            try:
                gas_part = f.gas[np.in1d(f.gas['iord'], pt)]
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



    def _construct_halo_cat(self, ts_extension, object_typetag):
        if object_typetag!= 'halo':
            raise ValueError("Unknown object type %r" % object_typetag)
        f = self.load_timestep(ts_extension)
        h = _loaded_halocats.get(id(f), lambda: None)()
        if h is None:
            h = self._construct_pynbody_halos(f)
            if isinstance(h, pynbody.halo.SubfindCatalogue) or isinstance(h, pynbody.halo.SubFindHDFHaloCatalogue):
                # ugly fix - loads groups by default, wanted halos
                h = self._construct_pynbody_halos(f, subs=True)
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
        h1 = self._construct_halo_cat(ts1, object_typetag)

        if output_handler_for_ts2:
            assert isinstance(output_handler_for_ts2, PynbodyInputHandler)
            f2 = output_handler_for_ts2.load_timestep(ts2)
            h2 = output_handler_for_ts2._construct_halo_cat(ts2, object_typetag)
        else:
            f2 = self.load_timestep(ts2)
            h2 = self._construct_halo_cat(ts2, object_typetag)

        if halo_max is None:
            halo_max = max(len(h2), len(h1))

        return self.create_bridge(f1, f2).fuzzy_match_catalog(
            halo_min,
            halo_max,
            threshold=threshold,
            only_family=only_family,
            groups_1=h1,
            groups_2=h2,
            **fuzzy_match_kwa,
        )

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
                h = self._construct_halo_cat(ts_extension, object_typetag)
            except:
                logger.warning("Unable to read %ss using pynbody; assuming step has none", object_typetag)
                return

            logger.warning(" => enumerating %ss directly using pynbody", object_typetag)

            istart = 1

            if isinstance(h, pynbody.halo.SubfindCatalogue) \
                or isinstance(h, pynbody.halo.SubFindHDFHaloCatalogue) \
                or isinstance(h, pynbody.halo.HOPCatalogue):
                istart = 0 # indexes from zero

            if hasattr(h, 'precalculate'):
                h.precalculate()


            for i in range(istart, len(h)+istart):
                try:
                    hi = h[i]
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
    catalogue_class_name = "pynbody.halo.SubfindCatalogue"

    _property_prefix_for_type = {'halo': 'sub_'}

    _sub_parent_names = ['sub_groupNr']

    _hidden_properties = ['children', 'group_len', 'group_off', 'Nsubs', 'groupNr', 'len', 'off']

    def _is_able_to_load(self, filepath):
        try:
            f = eval(self.snap_class_name)(filepath)
            h = f.halos()
            if isinstance(h, eval(self.catalogue_class_name)):
                return True
        except (OSError, RuntimeError):
            return False

    def load_object(self, ts_extension, finder_id, finder_offset, object_typetag='halo', mode=None):
        if mode=='subfind-properties':
            h = self._construct_halo_cat(ts_extension, object_typetag)
            return h.get_halo_properties(finder_offset,with_unit=False)
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

    def _construct_halo_cat(self, ts_extension, object_typetag):
        if object_typetag== 'halo':
            return super()._construct_halo_cat(ts_extension, object_typetag)
        elif object_typetag== 'group':
            return self._construct_group_cat(ts_extension)
        else:
            raise ValueError("Unknown halo type %r" % object_typetag)

    @staticmethod
    def _resolve_units(value):
        if (pynbody.units.is_unit(value)):
            return float(value)
        else:
            return value

    def available_object_property_names_for_timestep(self, ts_extension, object_typetag):
        cat = self._construct_halo_cat(ts_extension, object_typetag)
        properties = list(cat.get_halo_properties(0, False).keys())
        pynbody_prefix = self._property_prefix_for_type.get(object_typetag, '')
        for i,p in enumerate(properties):
            if p.startswith(pynbody_prefix):
                new_p = p[len(pynbody_prefix):]
                if new_p.startswith("_"):
                    new_p = new_p[1:]
                properties[i] = new_p
            if p in self._sub_parent_names:
                properties[i] = 'parent'
            if p == 'children': # NB 'children' is generated by pynbody for both Subfind and SubfindHDF catalogues
                properties[i] = 'child'

        properties = [p for p in properties if p not in self._hidden_properties]
        return properties


    def iterate_object_properties_for_timestep(self, ts_extension, object_typetag, property_names):
        h = self._construct_halo_cat(ts_extension, object_typetag)

        pynbody_prefix = self._property_prefix_for_type.get(object_typetag, '')

        for i in range(len(h)):
            all_data = [i, i]
            for k in property_names:
                pynbody_properties = h.get_halo_properties(i,with_unit=False)

                if k=='parent':
                    for adapted_k in self._sub_parent_names:
                        if adapted_k in pynbody_properties.keys():
                            break
                else:
                    adapted_k = pynbody_prefix + k
                    if adapted_k not in pynbody_properties:
                        adapted_k = pynbody_prefix + "_" + k

                if adapted_k in pynbody_properties:
                    data = self._resolve_units(pynbody_properties[adapted_k])
                    if adapted_k in self._sub_parent_names and data is not None:
                        # turn into a link
                        data = proxy_object.IncompleteProxyObjectFromFinderId(data, 'group')
                elif k=='child' and "children" in pynbody_properties:
                    # subfind does not actually store a list of children; but pynbody infers it from
                    # the parent data in the halo catalogue. Note children are always halos, even if
                    # the parent is group.
                    data = [proxy_object.IncompleteProxyObjectFromFinderId(data_i, 'halo')
                            for data_i in pynbody_properties['children']]
                else:
                    data = None

                all_data.append(data)
            yield all_data

class Gadget4HDFSubfindInputHandler(GadgetSubfindInputHandler):
    patterns = ["snapshot_???.hdf5", "snapshot_???.0.hdf5", "snap_???.hdf5", "snap_???.0.hdf5"]
    auxiliary_file_patterns =["fof_subhalo_tab_???.hdf5", "fof_subhalo_tab_???.0.hdf5"]
    snap_class_name = "pynbody.snapshot.gadgethdf.GadgetHDFSnap"
    catalogue_class_name = "pynbody.halo.Gadget4SubfindHDFCatalogue"

    _property_prefix_for_type = {'halo': 'Subhalo', 'group': 'Group'}

    _sub_parent_names = ['SubhaloGroupNr', 'SubhaloGrNr']

    _hidden_properties = ['Len', 'LenType', 'OffsetType', 'ParentRank', 'RankInGr', 'Nr', 'Ascale', 'FirstSub',
                          'OffsetType']

    def _transform_extension(self, extension_name):
        if extension_name.endswith(".0.hdf5"):
            return extension_name[:-7]
        else:
            return extension_name



class GadgetRockstarInputHandler(PynbodyInputHandler):
    patterns = ["snapshot_???"]
    auxiliary_file_patterns = ["halos_*.bin"]

    def _is_able_to_load(self, filepath):
        try:
            f = pynbody.load(filepath)
            h = pynbody.halo.RockstarCatalogue(f)
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
        import pynbody
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
            output_handler_for_ts2=output_handler_for_ts2,
            fuzzy_match_kwa={"use_family": pynbody.family.dm}
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
        h = self._construct_halo_cat(ts_extension, object_typetag)

        return (
            [
                key for key in h[1].properties.keys()
                if key not in self._excluded_precalculated_properties
            ] + list(self._included_additional_properties)
        )


    def _get_map_child_subhalos(self, ts_extension):
        h = self._construct_halo_cat(ts_extension, 'halo')
        halo_children = defaultdict(list)
        for halo in h:
            iparent = halo.properties["hostHalo"] # Returns the unique ID (NOT the halo_id index) of the host
            if iparent == -1: # If halo is its own host (i.e. -1), move on to next object
                continue

            halo_children[iparent].append(halo.properties["halo_id"]) # Otherwise store the halo_id index of the host
        return halo_children


    def _get_map_IDs_to_halo_ids(self, ts_extension):
        # IDs and halo_ids are usually related by ID = halo_id - 1,
        # but they can be entirely independent when using specific AHF options or running with MPI
        # This allows to map between one and the other
        h = self._construct_halo_cat(ts_extension, 'halo')
        return {
            halo.properties["ID"]: halo.properties["halo_id"] for halo in h
        }

    def iterate_object_properties_for_timestep(self, ts_extension, object_typetag, property_names):
        h = self._construct_halo_cat(ts_extension, object_typetag)

        h.physical_units()

        if "child" in property_names:
            map_child_parent = self._get_map_child_subhalos(ts_extension)

        if "parent" in property_names:
            map_ID_to_halo_id = self._get_map_IDs_to_halo_ids(ts_extension)

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
                            map_ID_to_halo_id[parent_ID],
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
                        for ichild in map_child_parent[halo_props["ID"]]
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
                     "dESN", "bLowTCool", "bSelfShield", "dExtraCoolShutoff"]

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

class ChangaUseIDLInputHandler(ChangaInputHandler):
    pynbody_halo_class_name = "AmigaGrpCatalogue"
    halo_stat_file_class_name = "AmigaIDLStatFile"
    auxiliary_file_patterns = ["*.amiga.grp"]

from . import caterpillar, eagle, ramsesHOP

RamsesHOPInputHandler = ramsesHOP.RamsesHOPInputHandler
