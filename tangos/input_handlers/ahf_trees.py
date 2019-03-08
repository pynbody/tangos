"""Code to read AHF's mtree files """

from __future__ import absolute_import
from __future__ import print_function

import re
import os
import numpy as np
from six.moves import xrange
from ..log import logger

class AHFTree(object):
    def __init__(self, path, ts):
        self._path = self._AHF_path_from_snapdir_path(os.path.join(path,ts.extension))
        self._load_Mvir(ts.previous)
        self._load_raw_links()

    def _snap_id_from_snapdir_path(cls, path):
        match = re.match(".*snapdir_([0-9]{3})/?", path)
        if match:
            return int(match.group(1))
        else:
            return None

    def _AHF_path_from_snapdir_path(cls, path):
        snap_id = cls._snap_id_from_snapdir_path(path)
        if snap_id is not None:
            import glob
            ahf_path = path + '/*%.3d.z*AHF_mtree' % snap_id
            # first snapshot has no mtree file check for this
            cat = glob.glob(ahf_path)
            if cat != []:
            	return cat[0]
            else:
               raise IOError("First snapshot has no mtree file.")
        else:
            raise IOError("Cannot infer path of merger tree files")

    def _load_raw_links(self):
        """
        read in the AHF mtree file containing the indices of halos and its progenitors.
        logic is we read backwards in time which is the opposite to what is done when this function is actually called
        """
        filename = os.path.join(self._path)
        results = {'id_this':np.asarray([],dtype=np.int64), 'id_desc':np.asarray([],dtype=np.int64), 'Mvir':np.asarray([],dtype=np.float64)} #np.empty((0,), dtype=np.dtype([('id_this', np.int64), ('id_desc', np.int64), ('Mvir', np.float32)]))

        f = open(filename)
        lines = f.readlines()
        #mtree_data = np.genfromtxt(f, dtype=int)
        nhalos = int(lines[0])#np.fromstring(lines[0], dtype=np.int16) #mtree_data[0][0] #np.loadtxt(f, usecols=0, dtype=np.int64, max_rows=1) #read only first line
        skip = 1 #skip the first line
        i=0
        while i < nhalos:
            i += 1
            _tmp = np.fromstring(lines[skip], dtype=np.dtype(int,int), sep=' ') #mtree_data[skip][0] #np.loadtxt(f, usecols=(0, 1), dtype=np.dtype(np.int64,np.int64), max_rows=1)
            _id = int(_tmp[0]) 
            ndesc = int(_tmp[1])
            if ndesc > 0:
                skip += 1 # increment the skip of lines for the line read above

                #_this = np.loadtxt(f, usecols=0, dtype=str, skiprows=skip, max_rows=ndesc)
                for n in range(ndesc):
                    #_this = int(lines[skip+n]) #mtree_data[skip:ndesc+skip][0] 
                    results['id_desc'] = np.append(results['id_desc'],np.asarray([_id],dtype=np.int64))
                    _this_id = int(lines[skip+n]) #[int(x[4:]) for x in _this] # rip off the timestep which is encoded as the first 3 digits
                    results['id_this'] = np.append(results['id_this'],np.asarray([_this_id],dtype=np.int64))
                    results['Mvir'] = np.append(results['Mvir'], self._Mvir[self._fid == _this_id])

            skip += ndesc # increment line skip by already read lines   
        
        self.links = results


    def _load_Mvir(self, ts):
        Mvir = ts.calculate_all('Mvir')
        self._fid = np.array([x.finder_id for x in ts.halos.all()])
        self._Mvir = np.asarray(Mvir)[0]

    def get_links_for_snapshot(self):
        """Get the links from snapshot ts to its immediate successor.

        Returns a dictionary; keys are the finder IDs at the given snapnum, whereas the values are
        a tuple containing the ID at the subsequent snapshot and the merger ratio (or 1.0 for no merger).
        """
        ids_this_snap = self.links['id_this']
        ids_next_snap = self.links['id_desc']
        merger_ratios = self._get_merger_ratio_array(self.links['id_desc'])

        return dict(zip(ids_this_snap, zip(ids_next_snap, merger_ratios)))


    def _get_merger_ratio_array(self, ids_next_snap):

        ratio = np.ones(len(ids_next_snap))
        tmp_ids_next_snap = [int(str(x)[4:]) for x in ids_next_snap]
        num_occurences_next_snap = np.bincount(tmp_ids_next_snap)
        mergers_next_snap = np.where(num_occurences_next_snap > 1)[0]
        logger.info("Identified %d mergers between snapshots", len(mergers_next_snap))
        for merger in mergers_next_snap:
            contributor_offsets = np.where(ids_next_snap == merger)[0]
            contributing_masses = self.links['Mvir'][contributor_offsets]
            ratio[contributor_offsets] = contributing_masses / contributing_masses.sum()
        return ratio

