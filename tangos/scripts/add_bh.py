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
import sys
import numpy as np
import gc
import glob
import pynbody
from six.moves import zip


def collect_bh_halos(bh_iord,f, existing_obj_num):
    halo = []
    for bhi in bh_iord:
        bhi = int(bhi)

        eh = np.where(existing_obj_num == bhi)[0]
        if len(eh)==0:
            halo.append(tangos.core.halo.Halo(f, bhi, bhi, 0, 0, 0, 1))

    return halo

def collect_bh_trackers(bh_iord,sim,existing_trackers):
    track = []
    for bhi in bh_iord:
        bhi = int(bhi)
        et = np.where(existing_trackers == bhi)[0]
        if len(et)==0:
            tx = tangos.core.tracking.TrackData(sim, bhi)
            tx.particles = [bhi]
            tx.use_iord = True
            track.append(tx)
    return track


def bh_halo_assign(f_pb):
    f_pbh = f_pb.halos()
    bh_cen_halos=None
    bh_halos = None
    if type(f_pbh) == pynbody.halo.RockstarIntermediateCatalogue:
        bh_cen_halos = f_pbh.get_group_array(family = 'BH')
    if type(f_pbh) == pynbody.halo.AHFCatalogue:
        bh_cen_halos = f_pbh.get_group_array(top_level=False, family = 'bh')
        bh_halos = f_pbh.get_group_array(top_level=True, family='bh')
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
    parallel_tasks.database.synchronize_creator_object(session)

    if "link-only" in sys.argv:
        files = []

    for f in files:
        print(f)
        sim = f.simulation
        
        try:
            f_pb = f.load()
        except:
            print("File not found - continuing")
            continue
        f_pb.physical_units()
        if len(f_pb.star)<1:
            print("No stars - continuing")
            continue

        logger.info("Gathering BH halo information for step %r", f)
        bhobjs, bhobj_nums, bhobj_ids = db.tracker.get_tracker_halos(f)
        halos = f.halos.filter_by(object_typecode=0).all()
        halo_nums = np.array([h.finder_id for h in halos])
        halo_ids = np.array([h.id for h in halos])
        logger.info("Gathering BH - halo link information for step %r", f)
        with parallel_tasks.RLock("bh"):
            bh_dict_id = tangos.core.dictionary.get_or_create_dictionary_item(session, "BH")
            bh_dict_cen_id = tangos.core.dictionary.get_or_create_dictionary_item(session, "BH_central")
            host_dict_id = tangos.core.dictionary.get_or_create_dictionary_item(session, "host_halo")
            session.commit()
        links_c, idf_c, idt_c = db.tracker.get_tracker_links(session, bh_dict_cen_id)
        links, idf, idt = db.tracker.get_tracker_links(session, bh_dict_id)

        logger.info("Gathering BH info from simulation for step %r", f)
        bh_iord = f_pb.star['iord'][np.where(f_pb.star['tform']<0)[0]]
        bh_mass = f_pb.star['mass'][np.where(f_pb.star['tform']<0)[0]]
        bh_iord = bh_iord[np.argsort(bh_mass)[::-1]]
        logger.info("Found %d black holes for %r", len(bh_iord), f)

        logger.info("gathering and committing BHs into step %r", f)
        with session.no_autoflush:
            halo_to_add = collect_bh_halos(bh_iord,f,bhobj_nums)
        with parallel_tasks.RLock("bh"):
            session2 = db.core.Session()
            sim2 = db.get_simulation(sim.id,session2)
            track, track_nums = db.tracker.get_trackers(sim2)
            tracker_to_add = collect_bh_trackers(bh_iord,sim,track_nums)
            session.add_all(tracker_to_add)
            session.add_all(halo_to_add)
            session.commit()
        logger.info("Done committing BH %d trackers and %d halos into %r", len(tracker_to_add), len(halo_to_add), f)

        logger.info("re-gathering bh halo information for %r", f)
        with parallel_tasks.RLock("bh"):
            bhobjs, bhobj_nums, bhobj_ids = db.tracker.get_tracker_halos(f)
        logger.info("Done re-gathering halo information for %r", f)

        logger.info("Getting halo information for BHs from simulation for step %r", f)
        bh_cen_halos, bh_halos = bh_halo_assign(f_pb)
        logger.info("Found associated halos for BHs in %r", f)

        del(f_pb)
        gc.collect()

        logger.info("Gathering BH-Halo links (all) for step %r", f)
        if bh_halos is not None:
            bh_links = []
            bh_halos = bh_halos[np.argsort(bh_mass)[::-1]]
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

                    exists = np.where((idf==halo_ids[oh[0]])&(idt==bhobj_ids[obh[0]]))[0]
                    if len(exists)==0:
                        bh_links.append(tangos.core.halo_data.HaloLink(h_i, bhobj_i, bh_dict_id))

            logger.info("Committing %d HaloLinks for All BHs in each halo for step %r", len(bh_links),f)
            with parallel_tasks.RLock("bh"):
                session.add_all(bh_links)
                session.commit()
            logger.info("Done committing %d links for step %r", len(bh_links), f)

        logger.info("Gathering BH-Halo links (central) for step %r")
        if bh_cen_halos is not None:
            bh_cen_links = []
            bh_cen_halos = bh_cen_halos[np.argsort(bh_mass)[::-1]]
            print("Associated halos: ",bh_cen_halos)
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

                    exists = np.where((idf_c==halo_ids[oh[0]])&(idt_c==bhobj_ids[obh[0]]))[0]
                    if len(exists)==0:
                        bh_cen_links.append(tangos.core.halo_data.HaloLink(h_i, bhobj_i, bh_dict_cen_id))
                        bh_cen_links.append(tangos.core.halo_data.HaloLink(bhobj_i, h_i, host_dict_id))

            logger.info("Committing %d HaloLinks for Central BHs in each halo for step %r", len(bh_cen_links),f)
            with parallel_tasks.RLock("bh"):
                session.add_all(bh_cen_links)
                session.commit()
            logger.info("Done committing %d links for step %r", len(bh_cen_links), f)


def main():
    parallel_tasks.launch(run)




