from __future__ import absolute_import

yt = None # deferred import; occurs when a YtInputHandler is constructed

from . import finding
from . import HandlerBase
from .. import config
from ..log import logger
from six.moves import range

class YtInputHandler(finding.PatternBasedFileDiscovery, HandlerBase):
    def __init__(self, *args, **kwargs):
        super(YtInputHandler, self).__init__(*args, **kwargs)
        global yt
        import yt as yt_local
        yt = yt_local
        assert "3.4.0" <= yt.__version__, "Tangos requires yt version 3.4.0 or later"

    def get_timestep_properties(self, ts_extension):
        ts_filename =  self._extension_to_filename(ts_extension)
        f = yt.load(ts_filename)
        time_gyr = float(f.current_time.in_units("Gyr"))
        redshift = f.current_redshift
        results = {'time_gyr': time_gyr, 'redshift': redshift,
                   'available': True}
        return results

    def load_timestep_without_caching(self, ts_extension, mode=None):
        if mode!=None:
            raise ValueError("Custom load modes are not supported with yt")
        f = yt.load(self._extension_to_filename(ts_extension))
        return f

    def load_object(self, ts_extension, object_number, object_typetag='halo', mode=None):
        f = self.load_timestep(ts_extension, mode)
        cat, cat_dat = self._load_halo_cat(ts_extension, object_typetag)
        center = cat_dat["halos","particle_position"][object_number]
        center+=f.domain_left_edge-cat.domain_left_edge
        radius = cat_dat["halos","virial_radius"][object_number]
        return f.sphere(center.in_cgs(), radius.in_cgs())


    def load_tracked_region(self, ts_extension, track_data, mode=None):
        raise NotImplementedError("Tracked regions not implemented for yt")

    def match_objects(self, ts1, ts2, halo_min, halo_max, dm_only=False, threshold=0.005, object_typetag='halo'):
        raise NotImplementedError("Matching halos still needs to be implemented for yt")

    def enumerate_objects(self, ts_extension, object_typetag="halo", min_halo_particles=config.min_halo_particles):
        if object_typetag!="halo":
            raise StopIteration
        if self._can_enumerate_objects_from_statfile(ts_extension, object_typetag):
            for X in self._enumerate_objects_from_statfile(ts_extension, object_typetag):
                yield X
        else:
            logger.warn("No halo statistics file found for timestep %r", ts_extension)
            logger.warn(" => enumerating %ss directly using yt", object_typetag)

            catalogue, catalogue_data = self._load_halo_cat(ts_extension, object_typetag)
            num_objects = len(catalogue_data["halos", "virial_radius"])

            for i in range(num_objects):
                obj = self.load_object(ts_extension, i, object_typetag)
                NDM = len(obj["DarkMatter","Mass"])
                NGas = len(obj["Gas","Mass"])
                NStar = len(obj["Stars","Mass"])
                if NDM > min_halo_particles:
                    yield i, NDM, NStar, NGas


    def _load_halo_cat(self, ts_extension, object_typetag):
        snapshot_file = self.load_timestep(ts_extension)
        if object_typetag== 'halo':
            if not hasattr(snapshot_file, "_tangos_halos"):
                snapshot_file._tangos_halos = self._load_halo_cat_without_caching(ts_extension, snapshot_file)
            return snapshot_file._tangos_halos
        else:
            raise ValueError("Unknown halo type %r" % object_typetag)

    def _load_halo_cat_without_caching(self, ts_extension, snapshot_file):
        raise NotImplementedError("You need to select a subclass of YtInputHandler")

    def get_properties(self):
        return {}


class YtChangaAHFInputHandler(YtInputHandler):
    patterns = ["*.00???", "*.00????"]

    def _load_halo_cat_without_caching(self, ts_extension, snapshot_file):
        cat = yt.frontends.ahf.AHFHalosDataset(self._extension_to_filename("halos/"+ts_extension)+".AHF_param",
                                                hubble_constant = snapshot_file.hubble_constant)
        cat_data = cat.all_data()
        return cat, cat_data

