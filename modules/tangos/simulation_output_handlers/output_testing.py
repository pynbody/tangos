import glob
import os, os.path
from .. import config
from . import SimulationOutputSetHandler

class DummyTimestepData(object):
    def __init__(self, message, time, max_halos, halo=None):
        self.message = message
        self.halo = halo
        self.max_halos = max_halos
        self.time = time

    def __str__(self):
        return self.message

class TestOutputSetHandler(SimulationOutputSetHandler):
    def get_properties(self):
        result = {}
        with open(os.path.join(config.base, self.basename, "sim_info"),'r') as f:
            for line in f:
                line_split = line.split()
                result[line_split[0]] = " ".join(line_split[1:])
        return result

    def enumerate_timestep_extensions(self):
        pre_extension_length = len(os.path.join(config.base, self.basename))
        steps = glob.glob(os.path.join(config.base, self.basename, "step.*"))
        for i in steps:
            print i, i[pre_extension_length:], self.strip_slashes(i[pre_extension_length:])
            yield self.strip_slashes(i[pre_extension_length:])

    def get_timestep_properties(self, ts_extension):
        result = {}
        result['time_gyr'] = float(self._get_ts_property(ts_extension, 'time'))
        result['redshift'] = float(self._get_ts_property(ts_extension, 'redshift'))
        return result

    def enumerate_halos(self, ts_extension):
        nhalos = int(self._get_ts_property(ts_extension, 'halos'))
        for i in xrange(nhalos):
            yield i+1, 2000, 0, 0

    def match_halos(self, f1, f2, halo_min, halo_max, dm_only=False, threshold=0.005):
        """Test implementation of match halos always links halo i->i, and a 0.05 mass transfer from i->i+1"""
        halo_max = min((halo_max,f1.max_halos,f2.max_halos))
        return_matches = [tuple()]
        for i in xrange(1,halo_max+1):
            if i>=halo_min:
                return_matches.append(((i, 1.0),(i+1,0.05),))
            else:
                return_matches.append(tuple())
        return return_matches

    def load_timestep(self, ts_extension):
        return DummyTimestepData("Test string - this would contain the data for "+ts_extension,
                                 float(self._get_ts_property(ts_extension, 'time')),
                                 int(self._get_ts_property(ts_extension, 'halos')))

    def load_halo(self, ts_extension, halo_number, partial=False):
        return DummyTimestepData("Test string - this would contain the data for %s halo %d"%(ts_extension ,halo_number),
                                 float(self._get_ts_property(ts_extension, 'time')),
                                 int(self._get_ts_property(ts_extension, 'halos')),
                                 halo_number)

    def _get_ts_property(self, ts_extension, property):
        ts_filename = self._extension_to_filename(ts_extension)
        with open(ts_filename, 'r') as f:
            for line in f:
                line_split = line.split()
                if line_split[0].lower() == property.lower():
                    return " ".join(line_split[1:])
