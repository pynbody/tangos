"""Code to read Peter Behroozi's Rockstar/consistent-trees output"""
from __future__ import print_function

import os
import numpy as np
from scipy.spatial import KDTree
from six.moves import xrange
from ..log import logger

class ConsistentTrees(object):
    def __init__(self, path):
        self._path = path
        self._load_raw_trees()
        self._load_scale_to_snap_number()
        self._setup_mapping_to_original_halos()


    def _load_raw_trees(self):
        filename = os.path.join(self._path, "trees", "tree_0_0_0.dat")
        f = open(filename)
        while True:
            l = f.readline()
            if l.startswith("#tree"):
                break
        read_type = np.dtype([('scale_this', np.float32),
                  ('id_this', np.int64),
                  ('scale_desc', np.float32),
                  ('id_desc', np.int64),
                  ('phantom', np.int8),
                  ('Mvir', np.float32),
                  ('pos', (np.float32, 3))])
        results = np.loadtxt(f, usecols=(0,1,2,3,8,10,17,18,19), dtype=read_type)
        self.links = results

    def _load_scale_to_snap_number(self):
        filename = os.path.join(self._path, "outputs", "scales.txt")
        read_type = np.dtype([('snap_number', np.int32), ('scale', np.float32)])
        self.scale_to_snap = np.loadtxt(filename, dtype=read_type)
        self._snap_min = self.scale_to_snap['snap_number'].min()
        self._snap_max = self.scale_to_snap['snap_number'].max()

    def _identify_snap_number(self, scalefactors):
        snap_scales = self.scale_to_snap['scale']*1.0001
        snaps =  self.scale_to_snap['snap_number'][np.searchsorted(snap_scales, scalefactors)]
        snaps[scalefactors<1e-10] = -1
        return snaps


    def _load_original_catalogue(self, snapnum):
        filename = os.path.join(self._path, "out_%d.list"%snapnum)
        read_type = np.dtype([('id', np.int64), ('pos', (np.float32, 3))])
        read_cols = (0, 8, 9, 10)
        return np.loadtxt(filename, dtype=read_type, usecols=read_cols)

    def _get_finder_ids(self, snapnum, pos):
        original_cat = self._load_original_catalogue(snapnum)
        kdtree = KDTree(original_cat['pos'])
        distance, index = kdtree.query(pos)
        error = (distance>0.01).sum()
        if error>0:
            raise ValueError("Cannot identify %d halos"%error)
        return original_cat['id'][index]

    def _setup_mapping_to_original_halos(self):
        num_ids = self.links['id_this'].max()+1

        self._id_to_original_snapshot = np.zeros(num_ids, dtype=int)-1
        self._id_to_finder_id = np.zeros(num_ids, dtype=int)-1

        ids = self.links['id_this']
        pos = self.links['pos']
        phantom = self.links['phantom']
        self._snap_nums = self._identify_snap_number(self.links['scale_this'])

        # sanity check:
        snap_nums_next = self._identify_snap_number(self.links['scale_desc'])
        snap_nums_next[snap_nums_next==-1] = self._snap_nums.max()+1
        snap_nums_diff = snap_nums_next-self._snap_nums
        assert (snap_nums_diff==1).all()

        self._id_to_original_snapshot[ids] = self._snap_nums

        for snapnum in range(self._snap_min, self._snap_max+1):
            logger.info("Matching consistent trees output onto original halo catalogue for snapshot %d",snapnum)
            this_snap_mask = (self._snap_nums==snapnum)&(phantom==0)
            ids_this_snap = ids[this_snap_mask]
            finder_ids = self._get_finder_ids(snapnum, pos[this_snap_mask])
            self._id_to_finder_id[ids_this_snap] = finder_ids

        # any remaining IDs correspond to phantom halos
        for snapnum in range(self._snap_min, self._snap_max+1):
            phantom_mask = ((self._snap_nums==snapnum)&(phantom!=0))
            num_phantoms = phantom_mask.sum()
            phantom_ids = -np.arange(1,num_phantoms+1)
            self._id_to_finder_id[ids[phantom_mask]] = phantom_ids

    def get_num_phantoms_in_snapshot(self, snapnum):
        return -self._id_to_finder_id[self._id_to_original_snapshot==snapnum].min()

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
