#!/usr/bin/env python2.7

import tangos as db
import tangos.core.dictionary
import tangos.core.halo
import tangos.core.halo_data
import tangos.core.timestep
import tangos.core.tracking
import tangos.parallel_tasks as parallel_tasks
import tangos.tracker
from tangos.log import logger
import sys
import numpy as np
import gc
import glob
import pynbody


def resolve_multiple_mergers(bh_map):
    for k,v in bh_map.iteritems():
        if v[0] in bh_map:
            old_target = v[0]
            old_weight = v[1]
            bh_map[k] = bh_map[old_target][0],v[1]*bh_map[old_target][1]
            print "Reassignment:",k,old_target,bh_map[k]
            resolve_multiple_mergers(bh_map)
            return

def generate_halolinks(sim, session):

    fname = glob.glob(db.config.base+"/"+sim.basename+"/*.mergers")
    if len(fname)==0:
        print "No merger file for "+sim.basename
        return
    elif len(fname)>1:
        print "Can't work out which is the merger file for "+sim.basename
        print "Found: ",fname
        return
    for ts1, ts2 in parallel_tasks.distributed(zip(sim.timesteps[:-1],sim.timesteps[1:])):
        links = []
        mergers_links = []
        bh_map = {}
        logger.info("getting BH tracking between steps %r and %r", ts1, ts2)
        with parallel_tasks.RLock("bh"):
            dict_obj = db.core.get_or_create_dictionary_item(session, "tracker")
            dict_obj_next = db.core.get_or_create_dictionary_item(session, "BH_merger_next")
            dict_obj_prev = db.core.get_or_create_dictionary_item(session, "BH_merger_prev")
            track_links_n, idf_n, idt_n = db.tracker.get_tracker_links(session, dict_obj_next)
            halos_1, nums1, id1 = db.tracker.get_tracker_halos(ts1)
            halos_2, nums2, id2 = db.tracker.get_tracker_halos(ts2)
            tracker_links, idf, idt = db.tracker.get_tracker_links(session,dict_obj)

        if len(nums1) == 0 or len(nums2) == 0:
            continue

        o1 = np.where(np.in1d(nums1,nums2))[0]
        o2 = np.where(np.in1d(nums2,nums1))[0]
        if len(o1) == 0 or len(o2) == 0:
            continue
        for ii, jj in zip(o1,o2):
            if nums1[ii] != nums2[jj]:
                raise RuntimeError("ERROR mismatch of BH iords")
            exists = np.where((idf==id1[ii])&(idt==id2[jj]))[0]
            if len(exists) == 0:
                links.append(db.HaloLink(halos_1[ii],halos_2[jj],dict_obj,1.0))

        for l in open(fname[0]):
            l_split = l.split()
            t = float(l_split[0])
            bh_dest_id = int(l_split[2])
            bh_src_id = int(l_split[3])
            ratio = float(l_split[4])

            if t>ts1.time_gyr and t<=ts2.time_gyr:
                bh_map[bh_src_id] = (bh_dest_id, ratio)

        resolve_multiple_mergers(bh_map)

        for src,(dest,ratio) in bh_map.iteritems():
            ob = np.where(nums1 == src)[0]
            oa = np.where(nums2 == dest)[0]
            if len(oa) == 0 or len(ob) == 0:
                continue
            bh_src_before = halos_1[ob[0]]
            bh_dest_after = halos_1[oa[0]]

            if len(np.where((idf_n==id1[ob[0]])&(idt_n==id2[oa[0]]))[0]) == 0:
                mergers_links.append(db.HaloLink(bh_src_before,bh_dest_after,dict_obj_next,1.0))
                mergers_links.append(db.HaloLink(bh_dest_after,bh_src_before,dict_obj_prev,ratio))

        with parallel_tasks.RLock("bh"):
            session.add_all(links)
            session.add_all(mergers_links)
            session.commit()

        logger.info("Done committing %d BH tracking links between steps %r and %r", len(links)+len(mergers_links), ts1, ts2)


def collect_bhs(bh_iord,sim,f,existing_track_num, existing_obj_num):
    track = []
    halo = []
    for bhi in bh_iord:
        bhi = int(bhi)
        et = np.where(existing_track_num == bhi)[0]
        if len(et)==0 :
            print "ADD ",bhi
            tx = tangos.core.tracking.TrackData(sim, bhi)
            tx.particles = [bhi]
            tx.use_iord = True
            print " ->",tx
            track.append(tx)

        eh = np.where(existing_obj_num == bhi)[0]
        if len(eh)==0:
            halo.append(tangos.core.halo.Halo(f, bhi, bhi, 0, 0, 0, 1))

    return track, halo


def bh_halo_assign(f_pb):
    f_pbh = f_pb.halos()
    bh_cen_halos=None
    bh_halos = None
    if type(f_pbh) == pynbody.halo.RockstarIntermediateCatalogue:
        bh_cen_halos = f_pbh.get_group_array(family = 'BH')
    if type(f_pbh) == pynbody.halo.AHFCatalogue:
        bh_cen_halos = f_pbh.get_group_array(top_level=False, family = 'BH')
        bh_halos = f_pbh.get_group_array(top_level=True, family='BH')
    if type(f_pbh) != pynbody.halo.AHFCatalogue and type(f_pbh) != pynbody.halo.RockstarIntermediateCatalogue:
        f_pb['gp'] = f_pbh.get_group_array()
        bh_cen_halos = f_pb.star['gp'][np.where(f_pb.star['tform']<0)[0]]
    del f_pbh
    gc.collect()
    return bh_cen_halos, bh_halos


def run():
    session = db.core.get_default_session()
    query = db.sim_query_from_args(sys.argv, session)

    files = db.core.get_default_session().query(tangos.core.timestep.TimeStep).filter(
        tangos.core.timestep.TimeStep.simulation_id.in_([q.id for q in query.all()])). \
        order_by(tangos.core.timestep.TimeStep.time_gyr).all()

    if "backwards" in sys.argv:
        files = files[::-1]

    files = parallel_tasks.distributed(files)
    parallel_tasks.mpi_sync_db(session)

    if "link-only" in sys.argv:
        files = []

    for f in files:
        print f
        sim = f.simulation
        
        try:
            f_pb = f.load()
        except:
            print "File not found - continuing"
            continue
        f_pb.physical_units()
        if len(f_pb.star)<1:
            print "No stars - continuing"
            continue
        bh_iord = f_pb.star['iord'][np.where(f_pb.star['tform']<0)[0]]
        bh_mass = f_pb.star['mass'][np.where(f_pb.star['tform']<0)[0]]
        bh_iord = bh_iord[np.argsort(bh_mass)[::-1]]
        logger.info("Found %d black holes for %r", len(bh_iord), f)

        with parallel_tasks.RLock("bh"):
            track, track_nums = db.tracker.get_trackers(sim)
            bhobjs, bhobj_nums, bhobj_ids = db.tracker.get_tracker_halos(f)
            halos = f.halos.all()
            halo_nums = np.array([h.finder_id for h in halos])
            halo_ids = np.array([h.id for h in halos])

            bh_dict_id = tangos.core.dictionary.get_or_create_dictionary_item(session, "BH")
            bh_dict_cen_id = tangos.core.dictionary.get_or_create_dictionary_item(session, "BH_central")
            host_dict_id = tangos.core.dictionary.get_or_create_dictionary_item(session, "host_halo")

            links_c, idf_c, idt_c = db.tracker.get_tracker_links(session, bh_dict_cen_id)
            links, idf, idt = db.tracker.get_tracker_links(session, bh_dict_id)

            session.commit()

        logger.info("gathering and committing BHs into step %r", f)
        with session.no_autoflush:
            tracker_to_add, halo_to_add = collect_bhs(bh_iord,sim,track_nums,bhobj_nums)
        with parallel_tasks.RLock("bh"):
            session.add_all(tracker_to_add)
            session.add_all(halo_to_add)
            session.commit()
        logger.info("Done committing BH objects into %r", f)

        bh_cen_halos, bh_halos = bh_halo_assign(f_pb)
        logger.info("Found associated halos for BHs in %r", f)

        del(f_pb)
        gc.collect()

        if bh_halos is not None:
            bh_links = []
            bh_halos = bh_halos[np.argsort(bh_mass)[::-1]]
            print "Associated halos: ",bh_halos
            with session.no_autoflush:
                for bhi, haloi in zip(bh_iord, bh_halos):
                    haloi = int(haloi)
                    bhi = int(bhi)
                    oh = np.where(halo_nums==haloi)[0]
                    obh = np.where(bhobj_nums==bhi)[0]
                    if len(oh)==0:
                        logger.warn("NOTE: skipping BH in halo %d as no corresponding DB object found", haloi)
                        continue
                    if len(obh)==0:
                        logger.warn("WARNING: can't find the db object for BH %d", bhi)
                        continue
                    bhobj_i = bhobjs[obh[0]]
                    h_i = halos[(oh[0])]

                    exists = np.where((idf==halo_ids[oh[0]])&(idt==bhobj_ids[obh[0]]))
                    if len(exists)==0:
                        bh_links.append(tangos.core.halo_data.HaloLink(h_i, bhobj_i, bh_dict_id))

            with parallel_tasks.RLock("bh"):
                session.add_all(bh_links)
                session.commit()

            logger.info("Done committing BH halo links into %r", f)

        if bh_cen_halos is not None:
            bh_cen_links = []
            bh_cen_halos = bh_cen_halos[np.argsort(bh_mass)[::-1]]
            print "Associated halos: ",bh_cen_halos
            with session.no_autoflush:
                for bhi, haloi in zip(bh_iord, bh_cen_halos):
                    haloi = int(haloi)
                    bhi = int(bhi)
                    oh = np.where(halo_nums==haloi)[0]
                    obh = np.where(bhobj_nums==bhi)[0]
                    if len(oh)==0:
                        logger.warn("NOTE: skipping BH in halo %d as no corresponding DB object found", haloi)
                        continue
                    if len(obh)==0:
                        logger.warn("WARNING: can't find the db object for BH %d", bhi)
                        continue
                    bhobj_i = bhobjs[obh[0]]
                    h_i = halos[(oh[0])]

                    exists = np.where((idf_c==halo_ids[oh[0]])&(idt_c==bhobj_ids[obh[0]]))
                    if len(exists)==0:
                        bh_cen_links.append(tangos.core.halo_data.HaloLink(h_i, bhobj_i, bh_dict_cen_id))
                        bh_cen_links.append(tangos.core.halo_data.HaloLink(bhobj_i, h_i, host_dict_id))

            with parallel_tasks.RLock("bh"):
                session.add_all(bh_cen_links)
                session.commit()

            logger.info("Done committing central BH objects into %r", f)

    for sim in query.all():
        with session.no_autoflush:
            generate_halolinks(sim, session)


if __name__=="__main__":
    parallel_tasks.launch(run)




