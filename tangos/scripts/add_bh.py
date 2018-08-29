#!/usr/bin/env python2.7

from __future__ import absolute_import
from __future__ import print_function
import tangos as db
from tangos.util.check_deleted import check_deleted
import tangos.core.dictionary
import tangos.core.halo
import tangos.core.halo_data
import tangos.core.timestep
import tangos.core.tracking
import tangos.parallel_tasks as parallel_tasks
import tangos.parallel_tasks.database
import tangos.tracking
from tangos.input_handlers.changa_bh import BHShortenedLog
from sqlalchemy.orm import Session
from tangos.log import logger
import sys
import numpy as np
import gc
import argparse
import glob
import pynbody
import six
from six.moves import zip

def get_parser_object():
    parser = argparse.ArgumentParser()
    tangos.core.supplement_argparser(parser)
    parser.add_argument('--sims', '--for', action='store', nargs=1,
                        metavar='simulation_name',
                        help='Specify a simulation to run on')
    parser.add_argument('--link-only', action='store_true',
                        help='Do not look for any new black holes; only link already-known black holes to new timesteps or halos')
    parser.add_argument('--backwards', action='store_true',
                        help="Scan through timesteps from low to high redshift (default is high to low)")
    return parser

def generate_missing_bh_objects(bh_iord, f, existing_obj_num):
    halo = []
    for bhi in bh_iord:
        if bhi not in existing_obj_num:
            halo.append(tangos.core.halo.BH(f, int(bhi)))

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


def scan_for_BHs(files, session):
    for timestep in parallel_tasks.distributed(files):
        logger.info("Processing %s", timestep)

        try:
            timestep_particle_data = timestep.load()
        except:
            logger.warning("File not found - continuing")
            continue

        if len(timestep_particle_data.star) < 1:
            logger.warning("No stars - continuing")
            continue

        timestep_particle_data.physical_units()

        logger.info("Gathering existing BH halo information from database for step %r", timestep)

        bhobjs = timestep.bhs.all()
        existing_bh_nums = [x.halo_number for x in bhobjs]

        logger.info("...found %d existing BHs", len(existing_bh_nums))

        logger.info("Gathering BH info from simulation for step %r", timestep)
        bh_iord_this_timestep = timestep_particle_data.star['iord'][
            np.where(timestep_particle_data.star['tform'] < 0)[0]]
        bh_mass_this_timestep = timestep_particle_data.star['mass'][
            np.where(timestep_particle_data.star['tform'] < 0)[0]]

        logger.info("Found %d black holes for %r", len(bh_iord_this_timestep), timestep)

        logger.info("Updating BH trackdata and BH objects using on-disk information from %r", timestep)

        add_missing_trackdata_and_BH_objects(timestep, bh_iord_this_timestep, existing_bh_nums, session)
        session.expire_all()

        logger.info("Calculating halo associations for BHs in timestep %r", timestep)
        bh_cen_halos, bh_halos = bh_halo_assign(timestep_particle_data)

        # re-order our information so that links refer to BHs in descending order of mass
        bh_order_by_mass = np.argsort(bh_mass_this_timestep)[::-1]
        bh_iord_this_timestep = bh_iord_this_timestep[bh_order_by_mass]
        bh_halos = bh_halos[bh_order_by_mass]
        bh_cen_halos = bh_cen_halos[bh_order_by_mass]

        logger.info("Freeing the timestep particle data")
        with check_deleted(timestep_particle_data):
            del (timestep_particle_data)

        if bh_halos is not None:
            assign_bh_to_halos(bh_halos, bh_iord_this_timestep, timestep, "BH")
        if bh_cen_halos is not None:
            assign_bh_to_halos(bh_cen_halos, bh_iord_this_timestep, timestep, "BH_central", "host_halo")


def assign_bh_to_halos(bh_halo_assignment, bh_iord, timestep, linkname, hostname=None):
    session = Session.object_session(timestep)
    linkname_dict_id = tangos.core.dictionary.get_or_create_dictionary_item(session, linkname)
    if hostname is not None:
        host_dict_id = tangos.core.dictionary.get_or_create_dictionary_item(session, hostname)
    else:
        host_dict_id = None

    logger.info("Gathering %s links for step %r", linkname, timestep)

    links, link_id_from, link_id_to = db.tracking.get_tracker_links(session, linkname_dict_id)
    halos = timestep.halos.filter_by(object_typecode=0).all()
    halo_nums = [h.finder_id for h in halos]
    halo_ids = np.array([h.id for h in halos])

    logger.info("Gathering bh halo information for %r", timestep)
    with parallel_tasks.lock.SharedLock("bh"):
        bh_database_object, existing_bh_nums, bhobj_ids = get_bh_objs_numbers_and_dbids(timestep)

    bh_links = []

    with session.no_autoflush:
        for bhi, haloi in zip(bh_iord, bh_halo_assignment):
            haloi = int(haloi)
            bhi = int(bhi)
            if haloi not in halo_nums:
                logger.warn("Skipping BH in halo %d as no corresponding halo found in the database", haloi)
                continue
            if bhi not in existing_bh_nums:
                logger.warn("Can't find the database object for BH %d", bhi)
                print(bhi)
                print(existing_bh_nums)
                continue

            bh_index_in_list = existing_bh_nums.index(bhi)
            halo_index_in_list = halo_nums.index(haloi)
            bh_obj = bh_database_object[bh_index_in_list]
            halo_obj = halos[halo_index_in_list]

            num_existing_links = ((link_id_from == halo_ids[halo_index_in_list]) & (link_id_to == bhobj_ids[bh_index_in_list])).sum()
            if num_existing_links==0:
                bh_links.append(tangos.core.halo_data.HaloLink(halo_obj, bh_obj, linkname_dict_id))
                if host_dict_id is not None:
                    bh_links.append(tangos.core.halo_data.HaloLink(bh_obj, halo_obj, host_dict_id))

    logger.info("Committing %d %s links for step %r...", len(bh_links), linkname, timestep)
    with parallel_tasks.ExclusiveLock("bh"):
        session.add_all(bh_links)
        session.commit()
    logger.info("...done")


def get_bh_objs_numbers_and_dbids(timestep):
    bh_database_object = timestep.bhs.all()
    existing_bh_nums = [x.halo_number for x in bh_database_object]
    bhobj_ids = np.array([x.id for x in bh_database_object])
    return bh_database_object, existing_bh_nums, bhobj_ids


def add_missing_trackdata_and_BH_objects(timestep, this_step_bh_iords, existing_bhobj_iords, session):
    with parallel_tasks.ExclusiveLock("bh"):
        track, track_nums = db.tracking.get_trackers(timestep.simulation)
        with session.no_autoflush:
            tracker_to_add = collect_bh_trackers(this_step_bh_iords, timestep.simulation, track_nums)
            halo_to_add = generate_missing_bh_objects(this_step_bh_iords, timestep, existing_bhobj_iords)

        session.add_all(tracker_to_add)
        session.add_all(halo_to_add)
        session.commit()
    logger.info("Committed %d new trackdata and %d new BH objects for %r", len(tracker_to_add), len(halo_to_add), timestep)



def resolve_multiple_mergers(bh_map):
    for k,v in six.iteritems(bh_map):
        if v[0] in bh_map:
            old_target = v[0]
            old_weight = v[1]
            bh_map[k] = bh_map[old_target][0],v[1]*bh_map[old_target][1]
            logger.info("Multi-merger detected; reassigning %d->%d (old) %d (new)",k,old_target,bh_map[k][0])
            resolve_multiple_mergers(bh_map)
            return

def generate_halolinks(session, fname, pairs):
    for ts1, ts2 in parallel_tasks.distributed(pairs):
        bh_log = BHShortenedLog(ts2.filename)
        links = []
        mergers_links = []
        bh_map = {}
        logger.info("Gathering BH tracking information for steps %r and %r", ts1, ts2)
        with parallel_tasks.ExclusiveLock("bh"):
            dict_obj = db.core.get_or_create_dictionary_item(session, "tracker")
            dict_obj_next = db.core.get_or_create_dictionary_item(session, "BH_merger_next")
            dict_obj_prev = db.core.get_or_create_dictionary_item(session, "BH_merger_prev")

        track_links_n, idf_n, idt_n = db.tracking.get_tracker_links(session, dict_obj_next)
        bh_objects_1, nums1, id1 = get_bh_objs_numbers_and_dbids(ts1)
        bh_objects_2, nums2, id2 = get_bh_objs_numbers_and_dbids(ts2)
        tracker_links, idf, idt = db.tracking.get_tracker_links(session,dict_obj)

        idf_n = np.array(idf_n)
        idt_n = np.array(idt_n)

        if len(nums1) == 0 or len(nums2) == 0:
            logger.info("No BHs found in either step %r or %r... moving on", ts1, ts2)
            continue

        logger.info("Generating BH tracker links between steps %r and %r", ts1, ts2)
        o1 = np.where(np.in1d(nums1,nums2))[0]
        o2 = np.where(np.in1d(nums2,nums1))[0]
        if len(o1) == 0 or len(o2) == 0:
            continue
        with session.no_autoflush:
            for ii, jj in zip(o1,o2):
                if nums1[ii] != nums2[jj]:
                    raise RuntimeError("BH iords are mismatched")
                exists = np.where((idf==id1[ii])&(idt==id2[jj]))[0]
                if len(exists) == 0:
                    links.append(tangos.core.halo_data.HaloLink(bh_objects_1[ii],bh_objects_2[jj],dict_obj,1.0))
                    links.append(tangos.core.halo_data.HaloLink(bh_objects_2[jj],bh_objects_1[ii],dict_obj,1.0))
        logger.info("Generated %d tracker links between steps %r and %r", len(links), ts1, ts2)

        logger.info("Generating BH Merger information for steps %r and %r", ts1, ts2)
        for l in open(fname[0]):
            l_split = l.split()
            t = float(l_split[0])
            bh_dest_id = int(l_split[2])
            bh_src_id = int(l_split[3])
            ratio = float(l_split[4])

            # ratios in merger file are ambiguous (since major progenitor may be "source" rather than "destination")
            # re-establish using the log file:
            try:
                ratio = bh_log.determine_merger_ratio(bh_src_id, bh_dest_id)
            except ValueError:
                logger.debug("Could not calculate merger ratio for %d->%d from the BH log; assuming the .mergers-asserted value is accurate",
                            bh_src_id, bh_dest_id)

            if t>ts1.time_gyr and t<=ts2.time_gyr:
                bh_map[bh_src_id] = (bh_dest_id, ratio)

        resolve_multiple_mergers(bh_map)
        logger.info("Gathering BH merger links for steps %r and %r", ts1, ts2)
        with session.no_autoflush:
            for src,(dest,ratio) in six.iteritems(bh_map):
                if src not in nums1 or dest not in nums2:
                    logger.warn("Can't link BH %r -> %r; missing BH objects in database",src,dest)
                    continue
                bh_src_before = bh_objects_1[nums1.index(src)]
                bh_dest_after = bh_objects_2[nums2.index(dest)]

                if ((idf_n==bh_src_before.id)&(idt_n==bh_dest_after.id)).sum()==0:
                    mergers_links.append(tangos.core.halo_data.HaloLink(bh_src_before,bh_dest_after,dict_obj_next,1.0))
                    mergers_links.append(tangos.core.halo_data.HaloLink(bh_dest_after,bh_src_before,dict_obj_prev,ratio))

        logger.info("Generated %d BH merger links for steps %r and %r", len(mergers_links), ts1, ts2)

        with parallel_tasks.ExclusiveLock("bh"):
            logger.info("Committing total %d BH links for steps %r and %r", len(mergers_links)+len(links), ts1, ts2)
            session.add_all(links)
            session.add_all(mergers_links)
            session.commit()
            logger.info("Finished committing BH links for steps %r and %r", ts1, ts2)

def timelink_bh(sims, session):
    query = db.sim_query_from_name_list(sims, session)
    for sim in query.all():
        pairs = list(zip(sim.timesteps[:-1],sim.timesteps[1:]))
        fname = glob.glob(db.config.base+"/"+sim.basename+"/*.mergers")
        if len(fname)==0:
            logger.error("No merger file for "+sim.basename)
            return
        elif len(fname)>1:
            logger.error("Can't work out which is the merger file for "+sim.basename)
            logger.error("Found: %s",fname)
            return
        with session.no_autoflush:
            generate_halolinks(session, fname, pairs)



def run():
    session = db.core.get_default_session()
    args = get_parser_object().parse_args(sys.argv[1:])

    query = db.sim_query_from_name_list(args.sims, session)

    files = db.core.get_default_session().query(tangos.core.timestep.TimeStep).filter(
        tangos.core.timestep.TimeStep.simulation_id.in_([q.id for q in query.all()])). \
        order_by(tangos.core.timestep.TimeStep.time_gyr).all()

    if args.backwards:
        files = files[::-1]


    parallel_tasks.database.synchronize_creator_object(session)

    if not args.link_only:
        scan_for_BHs(files, session)

    timelink_bh(args.sims, session)


def main():
    parallel_tasks.launch(run)




