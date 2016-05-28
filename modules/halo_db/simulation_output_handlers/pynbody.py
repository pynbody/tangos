from __future__ import absolute_import

from . import SimulationOutputSetHandler
from .. import halo_stat_files
from .. import config
import os, os.path
import pynbody
import time
import logging
import magic_amiga
import sim_output_finder
import weakref
import numpy as np

logger = logging.getLogger(__name__)

_loaded_halocats = {}

class DummyTimeStep(object):
    pass

class PynbodyOutputSetHandler(SimulationOutputSetHandler):
    def get_timestep_properties(self, ts_extension):
        ts_filename =  self._extension_to_filename(ts_extension)
        f = pynbody.load(ts_filename)
        try:
            time_gyr = f.properties['time'].in_units("Gyr")
        except:
            time_gyr = -1

        results = {'time_gyr': time_gyr, 'redshift': float(f.properties['z']),
                   'available': True}
        return results

    def load_timestep_without_caching(self, ts_extension):
        f = pynbody.load(self._extension_to_filename(ts_extension))
        f.physical_units()
        return f

    def load_halo(self, ts_extension, halo_number, partial=False):
        h = self._construct_halo_cat(ts_extension)
        if partial:
            h_file = h.load_copy(halo_number)
            h_file.physical_units()
            return h_file
        else:
            return h[halo_number]


    def load_tracked_region(self, ts_extension, track_data, partial=False):
        f = self.load_timestep(ts_extension)
        indices = self._get_indices_for_snapshot(f, track_data)
        if partial:
            return pynbody.load(f.filename, take=indices)
        else:
            return f[indices]


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



    def _construct_halo_cat(self, ts_extension):
        f = self.load_timestep(ts_extension)
        # amiga grp halo
        h = _loaded_halocats.get(id(f), lambda: None)()
        if h is None:
            h = f.halos()
            _loaded_halocats[id(f)] = weakref.ref(h)
        return h  # pynbody.halo.AmigaGrpCatalogue(f)

    def match_halos(self, f1, f2, halo_min, halo_max, dm_only=False, threshold=0.005):
        if dm_only:
            only_family='dark'
        else:
            only_family=None

        return f1.bridge(f2).fuzzy_match_catalog(halo_min, halo_max, threshold=threshold, only_family=only_family)

    def enumerate_halos(self, ts_extension):
        ts = DummyTimeStep()
        ts.filename = self._extension_to_filename(ts_extension)
        ts.redshift = self.get_timestep_properties(ts_extension)['redshift']

        try:
            statfile = halo_stat_files.HaloStatFile(ts)
            logger.info("Reading halos for timestep %r using a stat file",ts)
            for X in statfile.iter_rows("n_dm", "n_star", "n_gas"):
                yield X
        except IOError:
            logger.warn("No halo statistics file found for timestep %r",ts)
            logger.warn("Reading halos directly using pynbody")
            f = ts.load()
            h = f.halos()
            if hasattr(h, 'precalculate'):
                h.precalculate()
            if type(h)==pynbody.halo.RockstarIntermediateCatalogue:
                istart = 0
            else:
                istart = 1

            for i in xrange(istart, len(h)-istart):
                try:
                    hi = h[i]
                    if (not ts.halos.filter_by(halo_type=0, halo_number=i).count()>0) and len(hi.dm) > 1000:
                        yield i, len(hi.dm), len(hi.star), len(hi.gas)
                except (ValueError, KeyError) as e:
                    pass

class ChangaOutputSetHandler(PynbodyOutputSetHandler):
    flags_include = ["dPhysDenMin", "dCStar", "dTempMax",
                     "dESN", "bLowTCool", "bSelfShield", "dExtraCoolShutoff"]

    def enumerate_timestep_extensions(self):
        base = os.path.join(config.base, self.basename)
        extensions = sim_output_finder.find(basename=base+"/")
        for e in extensions:
            yield e[len(base)+1:]

    def get_properties(self):
        pfile = self._get_paramfile_path()

        if pfile is None:
            logger.warn("Param file cannot be found - no simulation properties will be available")
            return {}
        else:
            logger.info("Param file is %s", pfile)

        pfile_dict = magic_amiga.param_file_to_dict(pfile)
        log_path, prop_dict = self._get_log_path(pfile, pfile_dict)

        if log_path:
            prop_dict.update(self._get_properties_from_log(log_path))

        prop_dict.update(self._filter_paramfile_properties(pfile_dict))

        return prop_dict

    def _get_paramfile_path(self):
        try:
            pfile = magic_amiga.get_param_file(self._extension_to_filename(""))
        except RuntimeError:
            pfile = None
        return pfile

    def _get_log_path(self, paramfile_name, paramfile_dict):
        prop_dict = {}
        log_fn = paramfile_dict["achOutName"] + ".log"
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
            if pfile_dict.has_key(f):
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