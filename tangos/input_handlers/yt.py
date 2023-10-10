yt = None # deferred import; occurs when a YtInputHandler is constructed

import glob
import os

import numpy as np

from .. import config
from ..log import logger
from ..util.read_datasets_file import read_datasets
from . import HandlerBase, finding


class YtInputHandler(finding.PatternBasedFileDiscovery, HandlerBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
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

    def load_object(self, ts_extension, finder_id, finder_offset, object_typetag='halo', mode=None):
        f = self.load_timestep(ts_extension, mode)
        cat, cat_dat = self._load_halo_cat(ts_extension, object_typetag)
        center = cat_dat["halos","particle_position"][finder_offset]
        center+=f.domain_left_edge-cat.domain_left_edge
        radius = cat_dat["halos","virial_radius"][finder_offset]
        return f.sphere(center.in_cgs(), radius.in_cgs())


    def load_tracked_region(self, ts_extension, track_data, mode=None):
        raise NotImplementedError("Tracked regions not implemented for yt")

    def match_objects(self, ts1, ts2, halo_min, halo_max, dm_only=False, threshold=0.005, object_typetag='halo'):
        raise NotImplementedError("Matching halos still needs to be implemented for yt")

    def enumerate_objects(self, ts_extension, object_typetag="halo", min_halo_particles=config.min_halo_particles):
        if object_typetag!="halo":
            return
        if self._can_enumerate_objects_from_statfile(ts_extension, object_typetag):
            yield from self._enumerate_objects_from_statfile(ts_extension, object_typetag)
        else:
            logger.warning("No halo statistics file found for timestep %r", ts_extension)
            logger.warning(" => enumerating %ss directly using yt", object_typetag)

            catalogue, catalogue_data = self._load_halo_cat(ts_extension, object_typetag)
            num_objects = len(catalogue_data["halos", "virial_radius"])

            for i in range(num_objects):
                obj = self.load_object(ts_extension, i, i, object_typetag)
                NDM = len(obj["dark_matter","particle_mass"])
                NGas = len(obj["gas","mass"])
                NStar = len(obj["stars","particle_mass"])
                if NDM + NGas + NStar> min_halo_particles:
                    yield i, i, NDM, NStar, NGas


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
        try:
            # yt 4
            from yt.frontends.ahf.api import AHFHalosDataset
        except ImportError:
            # yt 3
            from yt.frontends.ahf import AHFHalosDataset
        cat = AHFHalosDataset(self._extension_to_filename("halos/"+ts_extension)+".AHF_param",
                                                hubble_constant = snapshot_file.hubble_constant)
        cat_data = cat.all_data()
        return cat, cat_data


class YtEnzoRockstarInputHandler(YtInputHandler):
    patterns = ["RD????/RD????", "DD????/DD????"]
    auxiliary_file_patterns = ["halos_*.bin"]

    def load_timestep_without_caching(self, ts_extension, mode=None):
        from yt.data_objects.particle_filters import add_particle_filter
        if mode is not None:
            raise ValueError("Custom load modes are not supported with yt")
        f = yt.load(self._extension_to_filename(ts_extension))

        def Stars(pfilter, data):
            filter = data[("all", "particle_type")] == 2 # DM = 1, Stars = 2
            return filter

        add_particle_filter("stars", function=Stars, filtered_type='all', \
                            requires=["particle_type"])

        def AllDarkMatter(pfilter, data):
            filter = np.logical_or(data[("all", "particle_type")] == 4,data[("all", "particle_type")] == 1) # DM = 1, Stars = 2
            return filter

        add_particle_filter("dark_matter", function=AllDarkMatter, filtered_type='all', \
                            requires=["particle_type"])

        def MustRefineParticles(pfilter, data):
            filter = data[("all", "particle_type")] == 4
            return filter

        add_particle_filter("mrp_dark_matter", function=MustRefineParticles, filtered_type='all', \
                            requires=["particle_type"])

        f.add_particle_filter("stars")
        f.add_particle_filter("dark_matter")
        f.add_particle_filter("mrp_dark_matter")
        return f

    def _load_halo_cat_without_caching(self, ts_extension, snapshot_file):
        # Check whether datasets.txt exists (i.e., if rockstar was run with yt)
        if os.path.exists(self._extension_to_filename("datasets.txt")):
            fnum = read_datasets(self._extension_to_filename(""),ts_extension)
        else: # otherwise, assume a one-to-one correspondence
            overdir = self._extension_to_filename("")
            snapfiles = glob.glob(overdir+ts_extension[:2]+len(ts_extension[2:].split('/')[0])*'?')
            rockfiles = glob.glob(overdir+"out_*.list")
            sortind = np.array([int(rname.split('.')[0].split('_')[-1]) for rname in rockfiles])
            sortord = np.argsort(sortind)
            snapfiles.sort()
            rockfiles = np.array(rockfiles)[sortord]
            timestep_ind = np.argwhere(np.array([s.split('/')[-1] for s in snapfiles])==ts_extension.split('/')[0])[0]
            fnum = int(rockfiles[timestep_ind][0].split('.')[0].split('_')[-1])
        cat = yt.load(self._extension_to_filename("halos_"+str(fnum)+".0.bin"))
        cat_data = cat.all_data()
        # Check whether rockstar was run with Behroozi's distribution or Wise's
        if np.any(cat_data["halos","particle_identifier"]<0):
            del cat
            del cat_data
            cat = yt.load(self._extension_to_filename("halos_"+str(fnum)+".0.bin"))
            cat.parameters['format_revision'] = 2 #
            cat_data = cat.all_data()
        return cat, cat_data

    def enumerate_objects(self, ts_extension, object_typetag="halo", min_halo_particles=config.min_halo_particles):
        if object_typetag!="halo":
            return
        if self._can_enumerate_objects_from_statfile(ts_extension, object_typetag):
            yield from self._enumerate_objects_from_statfile(ts_extension, object_typetag)
        else:
            logger.warn("No halo statistics file found for timestep %r", ts_extension)
            logger.warn(" => enumerating %ss directly using yt", object_typetag)

            catalogue, catalogue_data = self._load_halo_cat(ts_extension, object_typetag)
            num_objects = len(catalogue_data["halos", "virial_radius"])

            for i in range(num_objects):
                obj = self.load_object(ts_extension, int(catalogue_data["halos","particle_identifier"][i]), i, object_typetag)
                NDM = len(obj["dark_matter","particle_mass"])
                NGas = 0 # cells
                NStar = len(obj["stars","particle_mass"])
                if NDM + NGas + NStar> min_halo_particles:
                    yield i, int(catalogue_data["halos","particle_identifier"][i]), NDM, NStar, NGas

    def load_object(self, ts_extension, finder_id, finder_offset, object_typetag='halo', mode=None):
        f = self.load_timestep(ts_extension, mode)
        cat, cat_dat = self._load_halo_cat(ts_extension, object_typetag)
        center = cat_dat["halos","particle_position"][cat_dat["halos","particle_identifier"]==finder_id][0]
        center+=f.domain_left_edge-cat.domain_left_edge
        radius = cat_dat["halos","virial_radius"][cat_dat["halos","particle_identifier"]==finder_id][0]
        return f.sphere(center.in_cgs(), radius.in_cgs())
