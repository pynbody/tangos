from __future__ import absolute_import

import yt
import logging
import os, os.path

from . import finding
from . import SimulationOutputSetHandler
from .. import config
from ..log import logger
from six.moves import range

class YtOutputSetHandler(finding.PatternBasedFileDiscovery, SimulationOutputSetHandler):
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
        cat = self._load_halo_cat(ts_extension, object_typetag)
        cat_dat = cat.all_data()
        center = cat_dat["halos","particle_position"][object_number]
        pos_units_f = f.domain_left_edge.units

        # problem: both simulations are in code_units -- but different code_units.
        # pretty sure there must be a better way to do the following
        Mpc = yt.units.unit_object.Unit("Mpc")
        conv_factor = pos_units_f.get_conversion_factor(Mpc)[0]/center.units.get_conversion_factor(Mpc)[0]
        logger.info("conv_factor %d", conv_factor)
        conv_factor = 1.0

        center = (center-cat.domain_left_edge)*conv_factor + f.domain_left_edge
        radius = cat_dat["halos","virial_radius"][object_number]
        #radius = radius*conv_factor
        logger.info("yt pos %r radius %r", center, radius)
        return f.sphere(center, radius)


    def load_tracked_region(self, ts_extension, track_data, mode=None):
        raise NotImplementedError("Tracked regions not implemented for yt")

    def match_halos(self, ts1, ts2, halo_min, halo_max, dm_only=False, threshold=0.005, object_typetag='halo'):
        raise NotImplementedError("Matching halos still needs to be implemented for yt")

    def enumerate_objects(self, ts_extension, object_typetag="halo", min_halo_particles=config.min_halo_particles):
        catalogue = self._load_halo_cat(ts_extension, object_typetag)
        num_objects = len(catalogue.all_data()["halos", "virial_radius"])

        for i in range(num_objects):
            obj = self.load_object(ts_extension, i, object_typetag)
            print("yt obj %r", obj)
            NDM = len(obj["DarkMatter","Mass"])
            NGas = len(obj["Gas","Mass"])
            NStar = len(obj["Stars","Mass"])
            logger.info("yt id %d %d %d %d", i, NDM, NStar, NGas)
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
        raise NotImplementedError("You need to select a subclass of YtOutputSetHandler")

    def get_properties(self):
        return {}


class YtChangaAHFOutputSetHandler(YtOutputSetHandler):
    patterns = ["*.00???", "*.00????"]

    def _load_halo_cat_without_caching(self, ts_extension, snapshot_file):
        return yt.frontends.ahf.AHFHalosDataset(self._extension_to_filename("halos/"+ts_extension)+".AHF_param",
                                                hubble_constant = snapshot_file.hubble_constant)

