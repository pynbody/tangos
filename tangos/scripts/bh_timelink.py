#!/usr/bin/env python2.7

from __future__ import absolute_import
from __future__ import print_function
import tangos as db
import tangos.core.dictionary
import tangos.core.halo
import tangos.core.halo_data
import tangos.core.timestep
import tangos.core.tracking
import tangos.parallel_tasks as parallel_tasks
import tangos.parallel_tasks.database
import tangos.tracking
from tangos.log import logger
import numpy as np
import glob
import sys
import six
from six.moves import zip


def resolve_multiple_mergers(bh_map):
    for k,v in six.iteritems(bh_map):
        if v[0] in bh_map:
            old_target = v[0]
            old_weight = v[1]
            bh_map[k] = bh_map[old_target][0],v[1]*bh_map[old_target][1]
            print("Reassignment:",k,old_target,bh_map[k])
            resolve_multiple_mergers(bh_map)
            return

def generate_halolinks(session, fname, pairs):

    for ts1, ts2 in pairs:
        links = []
        mergers_links = []
        bh_map = {}
        logger.info("Gathering BH tracking information for steps %r and %r", ts1, ts2)
        with parallel_tasks.RLock("bh"):
            dict_obj = db.core.get_or_create_dictionary_item(session, "tracker")
            dict_obj_next = db.core.get_or_create_dictionary_item(session, "BH_merger_next")
            dict_obj_prev = db.core.get_or_create_dictionary_item(session, "BH_merger_prev")

        track_links_n, idf_n, idt_n = db.tracker.get_tracker_links(session, dict_obj_next)
        halos_1, nums1, id1 = db.tracker.get_tracker_halos(ts1)
        halos_2, nums2, id2 = db.tracker.get_tracker_halos(ts2)
        tracker_links, idf, idt = db.tracker.get_tracker_links(session,dict_obj)

        if len(nums1) == 0 or len(nums2) == 0:
            logger.info("No Tracker Halos found in either step %r or %r... moving on", ts1, ts2)
            continue

        logger.info("Collecting BH tracker links between steps %r and %r", ts1, ts2)
        o1 = np.where(np.in1d(nums1,nums2))[0]
        o2 = np.where(np.in1d(nums2,nums1))[0]
        if len(o1) == 0 or len(o2) == 0:
            continue
        with session.no_autoflush:
            for ii, jj in zip(o1,o2):
                if nums1[ii] != nums2[jj]:
                    raise RuntimeError("ERROR mismatch of BH iords")
                exists = np.where((idf==id1[ii])&(idt==id2[jj]))[0]
                if len(exists) == 0:
                    links.append(tangos.core.halo_data.HaloLink(halos_1[ii],halos_2[jj],dict_obj,1.0))
                    links.append(tangos.core.halo_data.HaloLink(halos_2[jj],halos_1[ii],dict_obj,1.0))
        logger.info("Found %d tracker links between steps %r and %r", len(links), ts1, ts2)

        logger.info("Gathering BH Merger information for steps %r and %r", ts1, ts2)
        for l in open(fname[0]):
            l_split = l.split()
            t = float(l_split[0])
            bh_dest_id = int(l_split[2])
            bh_src_id = int(l_split[3])
            ratio = float(l_split[4])

            if t>ts1.time_gyr and t<=ts2.time_gyr:
                bh_map[bh_src_id] = (bh_dest_id, ratio)

        resolve_multiple_mergers(bh_map)
        logger.info("Gathering BH Merger links for steps %r and %r", ts1, ts2)
        with session.no_autoflush:
            for src,(dest,ratio) in six.iteritems(bh_map):
                ob = np.where(nums1 == src)[0]
                oa = np.where(nums2 == dest)[0]
                if len(oa) == 0 or len(ob) == 0:
                    continue
                bh_src_before = halos_1[ob[0]]
                bh_dest_after = halos_2[oa[0]]

                if len(np.where((idf_n==id1[ob[0]])&(idt_n==id2[oa[0]]))[0]) == 0:
                    mergers_links.append(tangos.core.halo_data.HaloLink(bh_src_before,bh_dest_after,dict_obj_next,1.0))
                    mergers_links.append(tangos.core.halo_data.HaloLink(bh_dest_after,bh_src_before,dict_obj_prev,ratio))
        logger.info("Found %d BH Merger links for steps %r and %r", len(mergers_links), ts1, ts2)

        with parallel_tasks.RLock("bh"):
            logger.info("Committing total %d BH links for steps %r and %r", len(mergers_links)+len(links), ts1, ts2)
            session.add_all(links)
            session.add_all(mergers_links)
            session.commit()
            logger.info("Finished Committing BH links for steps %r and %r", ts1, ts2)

def run():
    parallel_tasks.database.synchronize_creator_object()
    session = db.core.get_default_session()
    query = db.sim_query_from_args(sys.argv, session)
    for sim in query.all():
        pairs = parallel_tasks.distributed(list(zip(sim.timesteps[:-1],sim.timesteps[1:])))
        fname = glob.glob(db.config.base+"/"+sim.basename+"/*.mergers")
        if len(fname)==0:
            print("No merger file for "+sim.basename)
            return
        elif len(fname)>1:
            print("Can't work out which is the merger file for "+sim.basename)
            print("Found: ",fname)
            return
        with session.no_autoflush:
            generate_halolinks(session, fname, pairs)


def main():
    parallel_tasks.launch(run)