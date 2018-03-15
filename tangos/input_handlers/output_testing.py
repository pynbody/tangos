from __future__ import absolute_import
from __future__ import print_function
import glob
import os, os.path
from .. import config
from . import HandlerBase
from six.moves import range

class DummyTimestepData(object):
    def __init__(self, message, time, max_halos, halo=None):
        self.message = message
        self.halo = halo
        self.max_halos = max_halos
        self.time = time

    def __str__(self):
        return self.message

class TestInputHandler(HandlerBase):
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
            print(i, i[pre_extension_length:], self.strip_slashes(i[pre_extension_length:]))
            yield self.strip_slashes(i[pre_extension_length:])

    def get_timestep_properties(self, ts_extension):
        result = {}
        result['time_gyr'] = float(self._get_ts_property(ts_extension, 'time'))
        result['redshift'] = float(self._get_ts_property(ts_extension, 'redshift'))
        return result

    def enumerate_objects(self, ts_extension, object_typetag='halo', min_halo_particles=config.min_halo_particles):
        nhalos_string = self._get_ts_property(ts_extension, object_typetag+"s")
        nhalos = 0 if nhalos_string is None else int(nhalos_string)
        for i in range(nhalos):
            yield i+1, 2000-i, 0, 0

    def match_objects(self, ts1, ts2, halo_min, halo_max, dm_only=False, threshold=0.005,
                      object_typetag='halo', output_handler_for_ts2=None):
        """Test implementation of match halos always links halo i->i, and a 0.05 mass transfer from i->i+1"""
        assert object_typetag=='halo' # currently only handle halos
        f1 = self.load_timestep(ts1)
        f2 = (output_handler_for_ts2 or self).load_timestep(ts2)
        if halo_max is None:
            halo_max = f1.max_halos
        halo_max = min((halo_max,f1.max_halos,f2.max_halos))
        return_matches = [tuple()]
        for i in range(1,halo_max+1):
            if i>=halo_min:
                return_matches.append(((i, 1.0),(i+1,0.05),))
            else:
                return_matches.append(tuple())
        return return_matches

    def load_timestep(self, ts_extension, mode=None):
        return DummyTimestepData("Test string - this would contain the data for "+ts_extension,
                                 float(self._get_ts_property(ts_extension, 'time')),
                                 int(self._get_ts_property(ts_extension, 'halos')))

    def load_region(self, ts_extension, region_specification, mode=None):
        data = self.load_timestep(ts_extension)
        data.message = data.message[region_specification]
        return data

    def load_object(self, ts_extension, halo_number, object_typetag='halo', mode=None):
        assert object_typetag=='halo'
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


class TestInputHandlerReverseHaloNDM(TestInputHandler):
    def enumerate_objects(self, ts_extension, object_typetag='halo', min_halo_particles=config.min_halo_particles):
        nhalos_string = self._get_ts_property(ts_extension, object_typetag+"s")
        nhalos = 0 if nhalos_string is None else int(nhalos_string)
        for i in range(nhalos):
            yield i+1, 2000+i, 0, 0