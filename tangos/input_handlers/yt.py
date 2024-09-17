yt = None # deferred import; occurs when a YtInputHandler is constructed

import glob
import os
from typing import List, Tuple

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

    def match_objects(
        self,
        ts1: str,
        ts2: str,
        halo_min: int,
        halo_max: int,
        dm_only: bool=False,
        threshold:float =0.005,
        object_typetag: str="halo",
        output_handler_for_ts2=None,
        fuzzy_match_kwa={},
    ) -> Tuple[List[int], List[List[Tuple[int, int]]]]:
        if output_handler_for_ts2 is None:
            raise NotImplementedError(
                "Alternative output_handler_for_ts2 is not implemented for yt."
            )
        if fuzzy_match_kwa:
            raise NotImplementedError(
                "Fuzzy matching is not implemented for yt."
            )

        if halo_min is None:
            halo_min = 0
        if halo_max is None:
            halo_max = np.inf

        h1, _ = self._load_halo_cat(ts1, object_typetag)
        if output_handler_for_ts2 is None:
            h2, _ = self._load_halo_cat(ts2, object_typetag)
        else:
            h2, _ = output_handler_for_ts2._load_halo_cat(ts2, object_typetag)

        # Compute the sets of particle ids in each halo
        members2 = np.concatenate([
            h2.halo("halos", i).member_ids
            for i in h2.r["particle_identifier"].astype(int)
            if halo_min <= i <= halo_max
        ])

        members2halo2 = np.concatenate([
            np.repeat(itangos, len(h2.halo("halos", irockstar).member_ids))
            for itangos, irockstar in enumerate(h2.r["particle_identifier"].astype(int))
            if halo_min <= itangos <= halo_max
        ])

        # Compute size of intersection of all sets in h1 with those in h2
        cat = []
        for ihalo1_tangos, ihalo1_rockstar in enumerate(h1.r["particle_identifier"].astype(int)):
            if not (halo_min <= ihalo1_tangos <= halo_max):
                continue

            ids1 = h1.halo("halos", ihalo1_rockstar).member_ids
            #mask = np.in1d(ids1, members2)
            mask = np.in1d(members2, ids1)
            if mask.sum() == 0:
                cat.append([])
                continue

            # Get the halo ids of the particles in the other snapshot
            idhalo2 = members2halo2[mask]

            # Count the number of particles in each halo
            idhalo2, counts = np.unique(idhalo2, return_counts=True)
            weights = counts / len(ids1)

            # Sort the links by decreasing number of particles
            _order = np.argsort(weights)[::-1]
            idhalo2 = idhalo2[_order]
            weights = weights[_order]

            # Keep only the links with a significant number of particles
            mask = weights > threshold
            if mask.sum() == 0:
                cat.append(
                    []
                )
                continue

            idhalo2 = idhalo2[mask]
            weights = weights[mask]

            cat.append(list(zip(idhalo2, weights)))

        return cat

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

    def available_object_property_names_for_timestep(self, ts_extension, object_typetag):
        h, _ = self._load_halo_cat(ts_extension, object_typetag)
        return [fn for ft, fn in h.field_list if ft == "halos"]


    def iterate_object_properties_for_timestep(self, ts_extension, object_typetag, property_names):
        try:
            yield from super().iterate_object_properties_for_timestep(ts_extension, object_typetag, property_names)
            return
        except OSError:
            pass
        h, ad = self._load_halo_cat(ts_extension, object_typetag)

        props_with_ftype = [
            ("halos", name) for name in property_names
        ]

        ad.get_data(props_with_ftype)

        Nhalo = len(ad["halos", "particle_identifier"])
        yield from zip(range(Nhalo), range(Nhalo), *(
            ad[_] for _ in props_with_ftype
        ))


class YtRamsesRockstarInputHandler(YtInputHandler):
    patterns = ["output_0????"]
    auxiliary_file_patterns = ["halos_*.bin"]

    def load_timestep_without_caching(self, ts_extension, mode=None):
        if mode is not None:
            raise ValueError("Custom load modes are not supported with yt")
        return yt.load(self._extension_to_filename(ts_extension))

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
        cat = yt.load(self._extension_to_filename(f"halos_{fnum}.0.bin"))
        cat_data = cat.all_data()
        # Check whether rockstar was run with Behroozi's distribution or Wise's
        if np.any(cat_data["halos","particle_identifier"]<0):
            cat = yt.load(self._extension_to_filename(f"halos_{fnum}.0.bin"))
            cat.parameters['format_revision'] = 2 #
            cat_data = cat.all_data()
        return cat, cat_data

    def enumerate_objects(self, ts_extension, object_typetag="halo", min_halo_particles=config.min_halo_particles):
        if object_typetag!="halo":
            return
        if self._can_enumerate_objects_from_statfile(ts_extension, object_typetag):
            yield from self._enumerate_objects_from_statfile(ts_extension, object_typetag)
        else:
            logger.warning("No halo statistics file found for timestep %r", ts_extension)
            logger.warning(" => enumerating %ss directly using yt", object_typetag)

            _catalogue, catalogue_data = self._load_halo_cat(ts_extension, object_typetag)
            num_objects = len(catalogue_data["halos", "virial_radius"])

            # Make sure this isn't garbage collected
            _f = self.load_timestep(ts_extension)

            for i in range(num_objects):
                obj = self.load_object(
                    ts_extension,
                    int(catalogue_data["halos","particle_identifier"][i]),
                    i,
                    object_typetag
                )
                NDM = len(obj["DM", "particle_ones"])
                NGas = 0 # cells
                NStar = len(obj["star", "particle_ones"])
                if NDM + NGas + NStar> min_halo_particles:
                    yield i, int(catalogue_data["halos","particle_identifier"][i]), NDM, NStar, NGas

    def load_object(self, ts_extension, finder_id, finder_offset, object_typetag='halo', mode=None):
        f = self.load_timestep(ts_extension, mode)
        cat, cat_dat = self._load_halo_cat(ts_extension, object_typetag)
        index = np.argwhere(cat_dat["halos", "particle_identifier"] == finder_id)[0, 0]
        center = cat_dat["halos","particle_position"][index]
        center += f.domain_left_edge - cat.domain_left_edge
        radius = cat_dat["halos", "virial_radius"][index]
        return f.sphere(center, radius)


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
        cat = yt.load(self._extension_to_filename(f"halos_{fnum}.0.bin"))
        cat_data = cat.all_data()
        # Check whether rockstar was run with Behroozi's distribution or Wise's
        if np.any(cat_data["halos","particle_identifier"]<0):
            cat = yt.load(self._extension_to_filename(f"halos_{fnum}.0.bin"))
            cat.parameters['format_revision'] = 2 #
            cat_data = cat.all_data()
        return cat, cat_data

    def enumerate_objects(self, ts_extension, object_typetag="halo", min_halo_particles=config.min_halo_particles):
        if object_typetag!="halo":
            return
        if self._can_enumerate_objects_from_statfile(ts_extension, object_typetag):
            yield from self._enumerate_objects_from_statfile(ts_extension, object_typetag)
            return

        logger.warning("No halo statistics file found for timestep %r", ts_extension)
        logger.warning(" => enumerating %ss directly using yt", object_typetag)

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
