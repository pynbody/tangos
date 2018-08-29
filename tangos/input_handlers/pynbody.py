from __future__ import absolute_import

import glob
import os
import os.path
import time
import weakref
import re
import numpy as np
from ..util import proxy_object

pynbody = None # deferred import; occurs when a PynbodyInputHandler is constructed

from . import halo_stat_files, finding
from . import HandlerBase
from .. import config
from ..log import logger
from six.moves import range


_loaded_halocats = {}

class DummyTimeStep(object):
    def __init__(self, filename):
        self.filename = filename


    def __repr__(self):
        return self.filename

    pass


class PynbodyInputHandler(finding.PatternBasedFileDiscovery, HandlerBase):
    def __new__(cls, *args, **kwargs):
        import pynbody as pynbody_local

        global pynbody
        pynbody = pynbody_local

        return object.__new__(cls)

    def __init__(self, *args, **kwargs):
        super(PynbodyInputHandler, self).__init__(*args, **kwargs)


        # old versions of pynbody have no __version__!
        pynbody_version = getattr(pynbody, "__version__","0.00")
        assert pynbody_version>="0.46", "Tangos requires pynbody 0.46 or later"

    def _is_able_to_load(self, ts_extension):
        filepath = self._extension_to_filename(ts_extension)
        try:
            f = pynbody.load(filepath)
            if self.quicker:
                logger.warn("Pynbody was able to load %r, but because 'quicker' flag is set we won't check whether it can also load the halo files", filepath)
            else:
                h = f.halos()
            return True
        except (IOError, RuntimeError):
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

    def load_object(self, ts_extension, halo_number, object_typetag='halo', mode=None):
        if mode=='partial':
            h = self._construct_halo_cat(ts_extension, object_typetag)
            h_file = h.load_copy(halo_number)
            h_file.physical_units()
            return h_file
        elif mode=='server':
            timestep = self.load_timestep(ts_extension, mode)
            from ..parallel_tasks import pynbody_server as ps
            return timestep.get_view(ps.ObjectSpecification(halo_number, object_typetag))
        elif mode=='server-partial':
            timestep = self.load_timestep(ts_extension, mode)
            from ..parallel_tasks import pynbody_server as ps
            view = timestep.get_view(ps.ObjectSpecification(halo_number, object_typetag))
            load_index = view['remote-index-list']
            logger.info("Partial load %r, taking %d particles", ts_extension, len(load_index))
            f = pynbody.load(self._extension_to_filename(ts_extension), take=load_index)
            f.physical_units()
            return f
        elif mode is None:
            h = self._construct_halo_cat(ts_extension, object_typetag)
            return h[halo_number]
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
            h = f.halos()
            if isinstance(h, pynbody.halo.SubfindCatalogue):
                h = f.halos(subs=True)
            _loaded_halocats[id(f)] = weakref.ref(h)
            f._db_current_halocat = h # keep alive for lifetime of simulation
        return h  # pynbody.halo.AmigaGrpCatalogue(f)




    def match_objects(self, ts1, ts2, halo_min, halo_max,
                      dm_only=False, threshold=0.005, object_typetag='halo',
                      output_handler_for_ts2=None):
        if dm_only:
            only_family='dm'
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

        return f1.bridge(f2).fuzzy_match_catalog(halo_min, halo_max, threshold=threshold,
                                                 only_family=only_family, groups_1=h1, groups_2=h2)

    def enumerate_objects(self, ts_extension, object_typetag="halo", min_halo_particles=config.min_halo_particles):
        if self._can_enumerate_objects_from_statfile(ts_extension, object_typetag):
            for X in self._enumerate_objects_from_statfile(ts_extension, object_typetag):
                yield X
        else:
            logger.warn("No halo statistics file found for timestep %r",ts_extension)

            try:
                h = self._construct_halo_cat(ts_extension, object_typetag)
            except:
                logger.warn("Unable to read %ss using pynbody; assuming step has none", object_typetag)
                raise StopIteration

            logger.warn(" => enumerating %ss directly using pynbody", object_typetag)

            istart = 1

            if isinstance(h, pynbody.halo.SubfindCatalogue) or isinstance(h, pynbody.halo.HOPCatalogue):
                istart = 0 # indexes from zero

            if hasattr(h, 'precalculate'):
                h.precalculate()


            for i in range(istart, len(h)+istart):
                try:
                    hi = h[i]
                    if len(hi.dm) > min_halo_particles:
                        yield i, len(hi.dm), len(hi.star), len(hi.gas)
                except (ValueError, KeyError) as e:
                    pass

    def get_properties(self):
        timesteps = list(self.enumerate_timestep_extensions())
        if len(timesteps)>0:
            f = self.load_timestep_without_caching(sorted(timesteps)[-1])
            if self.quicker:
                res = self._estimate_resolution_quicker(f)
            else:
                res = self._estimate_resolution(f)

            return {'approx_resolution_kpc': res}

        else:
            return {}

    def _estimate_resolution(self, f):
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

    def _estimate_resolution_quicker(self, f):
        interparticle_distance = float(f.properties['boxsize'].in_units("kpc a",**f.conversion_context()))/(float(len(f))**(1./3))
        res = 0.01*interparticle_distance
        logger.warn("Because 'quicker' flag is set, estimating res %.2g kpc from the file size; this could be inaccurate",
                    res)
        logger.warn(" -- it will certainly be wrong for e.g. zoom simulations")
        return res


class RamsesHOPInputHandler(PynbodyInputHandler):
    patterns = ["output_0????"]

    def match_objects(self, ts1, ts2, halo_min, halo_max,
                      dm_only=False, threshold=0.005, object_typetag='halo',
                      output_handler_for_ts2=None):

        f1 = self.load_timestep(ts1).dm
        h1 = self._construct_halo_cat(ts1, object_typetag)

        if output_handler_for_ts2 is None:
            f2 = self.load_timestep(ts2).dm
            h2 = self._construct_halo_cat(ts2, object_typetag)
        else:
            f2 = output_handler_for_ts2.load_timestep(ts2).dm
            h2 = output_handler_for_ts2._construct_halo_cat(ts2, object_typetag)

        bridge = pynbody.bridge.OrderBridge(f1,f2, monotonic=False)

        return bridge.fuzzy_match_catalog(halo_min, halo_max, threshold=threshold,
                                          only_family=pynbody.family.dm, groups_1=h1, groups_2=h2)




class GadgetSubfindInputHandler(PynbodyInputHandler):
    patterns = ["snapshot_???"]
    auxiliary_file_patterns =["groups_???"]

    def _is_able_to_load(self, filepath):
        try:
            f = pynbody.load(filepath)
            h = pynbody.halo.SubfindCatalogue(f)
            return True
        except (IOError, RuntimeError):
            return False

    def load_object(self, ts_extension, halo_number, object_typetag='halo', mode=None):
        if mode=='subfind_properties':
            h = self._construct_halo_cat(ts_extension, object_typetag)
            return h.get_halo_properties(halo_number,with_unit=False)
        else:
            return super(GadgetSubfindInputHandler, self).load_object(ts_extension, halo_number, object_typetag, mode)

    def _construct_group_cat(self, ts_extension):
        f = self.load_timestep(ts_extension)
        h = _loaded_halocats.get(id(f)+1, lambda: None)()
        if h is None:
            h = f.halos()
            assert isinstance(h, pynbody.halo.SubfindCatalogue)
            _loaded_halocats[id(f)+1] = weakref.ref(h)
            f._db_current_groupcat = h  # keep alive for lifetime of simulation
        return h

    def _construct_halo_cat(self, ts_extension, object_typetag):
        if object_typetag== 'halo':
            return super(GadgetSubfindInputHandler, self)._construct_halo_cat(ts_extension, object_typetag)
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
        if object_typetag=='halo':
            return ["CM","HalfMassRad","VMax","VMaxRad","mass","pos","spin","vel","veldisp","parent"]
        elif object_typetag=='group':
            return ["mass","mcrit_200","mmean_200","mtop_200","rcrit_200","rmean_200","rtop_200","child"]
        else:
            raise ValueError("Unknown object typetag %r"%object_typetag)

    def _get_group_children(self,ts_extension):
        h = self._construct_halo_cat(ts_extension, 'halo')
        group_children = {}
        for i in range(len(h)):
            halo_props = h.get_halo_properties(i,with_unit=False)
            if 'sub_parent' in halo_props:
                parent = halo_props['sub_parent']
                if parent not in group_children:
                    group_children[parent] = []
                group_children[parent].append(i)
        return group_children


    def iterate_object_properties_for_timestep(self, ts_extension, object_typetag, property_names):
        h = self._construct_halo_cat(ts_extension, object_typetag)

        if object_typetag=='halo':
            pynbody_prefix = 'sub_'
        elif object_typetag=='group':
            pynbody_prefix = ""
        else:
            raise ValueError("Unknown object typetag %r"%object_typetag)

        if 'child' in property_names and object_typetag=='group':
            child_map = self._get_group_children(ts_extension)

        for i in range(len(h)):
            all_data = [i]
            for k in property_names:
                pynbody_properties = h.get_halo_properties(i,with_unit=False)
                if pynbody_prefix+k in pynbody_properties:
                    data = self._resolve_units(pynbody_properties[pynbody_prefix+k])
                    if k == 'parent' and data is not None:
                        # turn into a link
                        data = proxy_object.IncompleteProxyObjectFromFinderId(data, 'group')
                elif k=='child' and object_typetag=='group':
                    # subfind does not actually store a list of children; we infer it from the parent
                    # data in the halo catalogue
                    data = child_map.get(i,None)
                    if data is not None:
                        data = [proxy_object.IncompleteProxyObjectFromFinderId(data_i, 'halo') for data_i in data]
                else:
                    data = None

                all_data.append(data)
            yield all_data




class GadgetRockstarInputHandler(PynbodyInputHandler):
    patterns = ["snapshot_???"]
    auxiliary_file_patterns = ["halos_*.bin"]

    def _is_able_to_load(self, filepath):
        try:
            f = pynbody.load(filepath)
            h = pynbody.halo.RockstarCatalogue(f)
            return True
        except (IOError, RuntimeError):
            return False




class ChangaInputHandler(PynbodyInputHandler):
    flags_include = ["dPhysDenMin", "dCStar", "dTempMax",
                     "dESN", "bLowTCool", "bSelfShield", "dExtraCoolShutoff"]

    patterns = ["*.00???","*.00????"]


    def get_properties(self):
        parent_prop_dict = super(ChangaInputHandler, self).get_properties()

        pfile = self._get_paramfile_path()

        if pfile is None:
            logger.warn("Param file cannot be found - no simulation properties will be available")
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
            logger.warn("Cannot find log file (%s)", log_path)
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
        with open(log_path, 'r') as f:
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

from . import caterpillar, eagle
