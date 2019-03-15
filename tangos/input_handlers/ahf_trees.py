""" Code to read AHF's mtree files """

from __future__ import absolute_import
from __future__ import print_function

import re
import os
import numpy as np
from .. import config
from six.moves import xrange
from ..log import logger

class AHFTree(object):
    def __init__(self, path, ts):
        self._path = self._AHF_path_from_snapdir_path(os.path.join(path,ts.extension))
        self._load_fid(ts.previous)
        self._load_raw_links()

    def _AHF_path_from_snapdir_path(cls, path):
        import glob
        ahf_path = path + '*.z*AHF_mtree'
        cat = glob.glob(ahf_path)
        if cat != []:
            return cat[0]
        else:    
            raise IOError("Cannot infer path of merger tree files")

    def _load_fid(self, ts):
        self._fid = np.array([x.finder_id for x in ts.halos.all()])

    def _load_raw_links(self):
        """
        read in the AHF mtree file containing the indices of halos and its progenitors as well as information about the shared particles.
        We establish symmetric links since for AHF any progenitor can have several descendants cause of mass transfer.
        """
        filename = os.path.join(self._path)
        results = {'id_this':np.asarray([],dtype=np.int64), 'id_desc':np.asarray([],dtype=np.int64), 'f_share':np.asarray([],dtype=np.float64)} #np.empty((0,), dtype=np.dtype([('id_this', np.int64), ('id_desc', np.int64), ('Mvir', np.float32)]))

        data = np.genfromtxt(filename,comments="#",dtype="int")
        i=0
        while i < len(data):
            for j in range(data[i][2]):
                idx = i+1+j
                if data[idx][1]-1 in self._fid: # check if this halo was loaded in case a minimum number of particles different to AHF was used to load halos into DB
                # keep in mind finder id and AHF id have an offset of 1
                    results['id_desc'] = np.append(results['id_desc'],data[i][0]+1)
                    results['id_this'] = np.append(results['id_this'],data[idx][1]+1)
                    results['f_share'] = np.append(results['f_share'], float(data[idx][0] * data[idx][0]) / (data[i][1] * data[idx][2]) )
            i += data[i][2] + 1
        
        self.links = results

    def _load_major_progenitor_branch(self):
        """
        load the major progenitor branch of the merger tree from the mtree_idx files.
        """
        NotImplementedError("Loading the major progenitor branch is not implemented")


    def get_links_for_snapshot(self):
        """Get the links from snapshot ts to its immediate successor.

        Returns a dictionary; keys are the finder IDs at the given snapnum, whereas the values are
        a tuple containing the ID at the subsequent snapshot and the fraction of shared particles.
        """
        ids_this_snap = self.links['id_this']
        ids_next_snap = self.links['id_desc']
        merger_ratios = self.links['f_share']

        return list(zip(ids_this_snap, zip(ids_next_snap, merger_ratios)))

