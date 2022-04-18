"""Code to read Peter Behroozi's Rockstar/consistent-trees output"""
from __future__ import print_function

import os
import numpy as np
from six.moves import xrange
from ..log import logger

class ConsistentTrees(object):
    def __init__(self, path):
        self._path = self._infer_subpath(path)
        self._load_raw_trees()
        self._load_scale_to_snap_number()
        self._setup_map_to_original_finder_catalogues()

    def _infer_subpath(self, rootpath):
        if os.path.exists(os.path.join(rootpath,'trees')):
            return rootpath
        elif os.path.exists(os.path.join(rootpath, 'halos', 'trees')):
            return os.path.join(rootpath, 'halos')
        else:
            raise IOError("Cannot find the consistent-trees output")

    def _load_raw_trees(self):
        filename = os.path.join(self._path, "trees", "tree_0_0_0.dat")
        f = open(filename)
        while True:
            l = f.readline()
            if l.startswith("#tree"):
                break
        read_type = np.dtype([
                  ('id_this', np.int64),
                  ('id_desc', np.int64),
                  ('phantom', np.int8),
                  ('Mvir', np.float32)])
        results = np.loadtxt(f, usecols=(1,3,8,10), dtype=read_type)
        self.links = results

    def _load_scale_to_snap_number(self):
        filename = os.path.join(self._path, "outputs", "scales.txt")
        read_type = np.dtype([('snap_number', np.int32), ('scale', np.float32)])
        self.scale_to_snap = np.loadtxt(filename, dtype=read_type)
        self._snap_min = self.scale_to_snap['snap_number'].min()
        self._snap_max = self.scale_to_snap['snap_number'].max()


    def _load_original_catalogue(self, snapnum):
        filename = os.path.join(self._path, "outputs", "really_consistent_%d.list"%snapnum)
        read_cols = (0, 49)
        return np.loadtxt(filename, dtype=np.int64, usecols=read_cols, unpack=True)

    def _setup_map_to_original_finder_catalogues(self):
        maxval = self.links['id_this'].max()
        self._id_to_finder_id = np.zeros(maxval + 1, dtype=np.int64)
        self._id_to_snap_num = np.zeros(maxval + 1, dtype=np.int32) - 1
        for snapnum in xrange(self._snap_min, self._snap_max + 1):
            ctid, original_id = self._load_original_catalogue(snapnum)
            self._id_to_finder_id[ctid] = original_id
            self._id_to_snap_num[ctid] = snapnum

        self._sanity_check_snap_num_assignment()
        self._snap_nums = self._get_snapshot_nums(self.links['id_this'])

        ids = self.links['id_this']
        phantom = self.links['phantom']
        # any remaining IDs correspond to phantom halos
        for snapnum in range(self._snap_min, self._snap_max + 1):
            phantom_mask = ((self._snap_nums == snapnum) & (phantom != 0))
            num_phantoms = phantom_mask.sum()
            phantom_ids = -np.arange(1, num_phantoms + 1)
            self._id_to_finder_id[ids[phantom_mask]] = phantom_ids

    def _sanity_check_snap_num_assignment(self):
        snap_nums_this = self._get_snapshot_nums(self.links['id_this'])
        snap_nums_next = self._get_snapshot_nums(self.links['id_desc'])
        snap_nums_next[snap_nums_next == -1] = self._snap_max + 1
        snap_nums_diff = snap_nums_next-snap_nums_this
        assert (snap_nums_diff == 1).all()

    def _get_finder_ids(self, consistent_trees_ids):
        return self._id_to_finder_id[consistent_trees_ids]

    def _get_snapshot_nums(self, consistent_trees_ids):
        snapnums = self._id_to_snap_num[consistent_trees_ids]
        snapnums[consistent_trees_ids<0]=-1
        return snapnums

    def get_num_phantoms_in_snapshot(self, snapnum):
        finder_ids = self._id_to_finder_id[self._id_to_snap_num==snapnum]
        if len(finder_ids)==0:
            return 0
        else:
            return -finder_ids.min()

    def get_links_for_snapshot(self, snapnum):
        """Get the links from snapshot snapnum to its immediate successor.

        Returns a dictionary; keys are the finder IDs at the given snapnum, whereas the values are
        a tuple containing the ID at the subsequent snapshot and the merger ratio (or 1.0 for no merger).

        Negative values indicate phantom halos, i.e. halos that were not present in the finder output"""

        this_snap_mask = self._snap_nums == snapnum
        ids_this_snap = self._id_to_finder_id[self.links['id_this'][this_snap_mask]]
        ids_next_snap = self._id_to_finder_id[self.links['id_desc'][this_snap_mask]]

        merger_ratios = self._get_merger_ratio_array(ids_next_snap, snapnum)

        return dict(zip(ids_this_snap, zip(ids_next_snap, merger_ratios)))

    def get_finder_id_to_tree_id_for_snapshot(self, snapnum):
        """Get the internal consistent-trees ids for each original halo-finder ID"""
        this_snap_mask = (self._snap_nums == snapnum)
        internal_ids_this_snap = self.links['id_this'][this_snap_mask]
        finder_ids_this_snap = self._id_to_finder_id[internal_ids_this_snap]
        finder_id_to_id = dict(zip(finder_ids_this_snap, internal_ids_this_snap))
        return finder_id_to_id

    def _get_merger_ratio_array(self, ids_next_snap, snapnum):
        this_snap_mask = self._snap_nums == snapnum
        ratio = np.ones(len(ids_next_snap))
        num_occurences_next_snap = np.bincount(ids_next_snap[ids_next_snap >= 0])
        mergers_next_snap = np.where(num_occurences_next_snap > 1)[0]
        logger.info("Identified %d mergers between snapshot %d and %d", len(mergers_next_snap), snapnum, snapnum + 1)
        for merger in mergers_next_snap:
            contributor_offsets = np.where(ids_next_snap == merger)[0]
            contributing_masses = self.links['Mvir'][this_snap_mask][contributor_offsets]
            ratio[contributor_offsets] = contributing_masses / contributing_masses.sum()
        return ratio
