from __future__ import absolute_import

import yt
import logging
import os, os.path

from . import halo_stat_files, finding
from . import SimulationOutputSetHandler
from .. import config
from ..log import logger


logger = logging.getLogger(__name__)


class DummyTimeStep(object):
    pass

class YtOutputSetHandler(SimulationOutputSetHandler):
    def get_timestep_properties(self, ts_extension):
        ts_filename =  self._extension_to_filename(ts_extension)
        f = yt.load(ts_filename)
        time_gyr = float(f.current_time.in_units("Gyr"))
        redshift = f.current_redshift
        results = {'time_gyr': time_gyr, 'redshift': redshift,
                   'available': True}
        return results

    def load_timestep_without_caching(self, ts_extension):
        f = yt.load(self._extension_to_filename(ts_extension))
        return f

    def load_halo(self, ts_extension, halo_number, partial=False):
        raise NotImplementedError, "Halos not yet implemented for yt"


    def load_tracked_region(self, ts_extension, track_data, partial=False):
        raise NotImplementedError, "Tracked regions not implemented for yt"


    def match_halos(self, f1, f2, halo_min, halo_max, dm_only=False, threshold=0.005):
        raise NotImplementedError, "Matching halos still needs to be implemented for yt"

    def enumerate_halos(self, ts_extension):
        # TODO: clean this up - it's a direct copy of something in the pynbody simulation output handler
        #       and so the statfile handling needs to be factored out one way or another

        ts = DummyTimeStep()
        ts.filename = self._extension_to_filename(ts_extension)
        ts.redshift = self.get_timestep_properties(ts_extension)['redshift']

        try:
            statfile = halo_stat_files.HaloStatFile(ts)
            logger.info("Reading halos for timestep %r using a stat file",ts)
            for X in statfile.iter_rows("n_dm", "n_star", "n_gas"):
                yield X
        except IOError:
            raise NotImplementedError, "Direct enumeration of halos still needs to be implemented for yt"

    def get_properties(self):
        return {}

    def enumerate_timestep_extensions(self):
        base = os.path.join(config.base, self.basename)
        extensions = finding.find(basename=base + "/")
        for e in extensions:
            yield e[len(base) + 1:]