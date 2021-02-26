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
        There is at least two different file formats for the AHF mtree files. One includes information about the particles shared across haloes,
        the other not and simply orders the progenitors by a merit function.
        This little function is to decide which file format we are dealing with.
        """

        try:
            self._load_mtree_file_standard()
        except:
            logger.info("Could not load AHF mtree file in standard format. Trying the non-standard form.")
            try:
                self._load_mtree_file_cropped()
            except:
                logger.info("Could not load AHF mtree file in non-standard format either. Make sure mtree files exist.")
                raise IOError("Could not load merger tree files")


    def _load_mtree_file_standard(self):
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
                if data[idx][1] in self._fid: # check if this halo was loaded in case a minimum number of particles different to AHF was used to load halos into DB
                # keep in mind finder id and AHF id have an offset of 1
                    results['id_desc'] = np.append(results['id_desc'],data[i][0])
                    results['id_this'] = np.append(results['id_this'],data[idx][1])
                    results['f_share'] = np.append(results['f_share'], float(data[idx][0] * data[idx][0]) / (data[i][1] * data[idx][2]) )
            i += data[i][2] + 1
        
        self.links = results

    def _load_mtree_file_cropped(self):
        """
        read in the AHF mtree file containing only the indices of halos and its progenitors and assume progenitors are ordered in descending weight.
        """
        filename = self._path
        results = {'id_this':np.asarray([],dtype=np.int64), 'id_desc':np.asarray([],dtype=np.int64), 'f_share':np.asarray([],dtype=np.float64)} #'Mvir':np.asarray([],dtype=np.float64), 

        f = open(filename)
        lines = f.readlines()
        nhalos = int(lines[0])
        skip = 1 #skip lines already read
        i=0
        while i < nhalos:
            i += 1
            _tmp = np.fromstring(lines[skip], dtype=np.dtype(int,int), sep=' ')
            _id = int(_tmp[0])
            nprogen = int(_tmp[1])
            skip += 1 # increment the skip of lines for the line read above
            if nprogen > 0:
                for n in range(nprogen):
                    _this_id = int(lines[skip+n]) # rip off the timestep which is encoded as the first 3 digits
                    if _this_id in self._fid: # check if the halo exists in the database, this is needed if db was created with a minimum particle number per halo which does not agree with AHF definition 
                        results['id_desc'] = np.append(results['id_desc'],np.asarray([_id],dtype=np.int64))
                        results['id_this'] = np.append(results['id_this'],np.asarray([_this_id],dtype=np.int64))
                        results['f_share'] = np.append(results['f_share'], np.asarray([(nprogen-n)/nprogen],dtype=np.float64))
            skip += nprogen   # increment line skip by already read lines   
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

