#!/usr/bin/env python2.7

import halo_db as db
import halo_db.parallel_tasks as parallel_tasks
import halo_db.tracker
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

def generate_halolinks(sim):
    db.tracker.generate_tracker_halo_links(sim)
    fname = glob.glob(db.config.base+"/"+sim.basename+"/*.mergers")
    if len(fname)==0:
        print "No merger file for "+sim.basename
        return
    elif len(fname)>1:
        print "Can't work out which is the merger file for "+sim.basename
        print "Found: ",fname
        return


    timestep_numbers = np.array([int(ts.extension[-6:]) for ts in sim.timesteps])
    dict_obj = db.core.get_or_create_dictionary_item(session, "BH_merger")

    for ts1, ts2 in zip(sim.timesteps[:-1],sim.timesteps[1:]):
        ts1_step = int(ts1.extension[-6:])
        ts2_step = int(ts2.extension[-6:])

        bh_map = {}
        print ts1, ts2
        for l in open(fname[0]):
            l_split = l.split()
            ts = float(l_split[1])
            bh_dest_id = int(l_split[2])
            bh_src_id = int(l_split[3])
            ratio = float(l_split[4])

            if ts>ts1_step and ts<=ts2_step:
                bh_map[bh_src_id] = (bh_dest_id, ratio)

        resolve_multiple_mergers(bh_map)

        for src,(dest,ratio) in bh_map.iteritems():
            bh_src_before = ts1.halos.filter_by(halo_type=1,halo_number=src).first()
            bh_dest_after = ts2.halos.filter_by(halo_type=1,halo_number=dest).first()

            if bh_src_before is not None and bh_dest_after is not None:
                db.tracker.generate_tracker_halo_link_if_not_present(bh_src_before,bh_dest_after,dict_obj,1.0)
                db.tracker.generate_tracker_halo_link_if_not_present(bh_dest_after,bh_src_before,dict_obj,ratio)


def run():
    db.use_blocking_session()
    session = db.core.internal_session
    query = db.sim_query_from_args(sys.argv, session)

    files = db.core.internal_session.query(db.TimeStep).filter(
        db.TimeStep.simulation_id.in_([q.id for q in query.all()])). \
        order_by(db.TimeStep.time_gyr).all()


    files = parallel_tasks.distributed(files)
    parallel_tasks.mpi_sync_db(session)

    for f in files:
        print f
        sim = f.simulation

        f_pb = f.load()
        f_pb.physical_units()
        if len(f_pb.star)<1:
            print "No stars - continuing"
            continue
        bh_iord = f_pb.star['iord'][np.where(f_pb.star['tform']<0)[0]]
        bh_mass = f_pb.star['mass'][np.where(f_pb.star['tform']<0)[0]]
        bh_iord = bh_iord[np.argsort(bh_mass)[::-1]]
        print "Found black holes:", bh_iord

        bh_objs = []

        for bhi in bh_iord:
            bhi = int(bhi)
            if sim.trackers.filter_by(halo_number=bhi).count()==0 :
                print "ADD ",bhi
                tx = db.TrackData(sim, bhi)
                tx = session.merge(tx)
                tx.particles = [bhi]
                tx.use_iord = True
                print " ->",tx
            else:
                tx = sim.trackers.filter_by(halo_number=bhi).first()

            if f.halos.filter_by(halo_number=tx.halo_number, halo_type = 1).count()==0:
                session.merge(db.Halo(f, tx.halo_number, 0, 0, 0, 1))


        session.commit()

        f_pbh = f_pb.halos()
        if type(f_pbh) == pynbody.halo.RockstarIntermediateCatalogue:
            bh_halos = f_pbh.get_fam_group_array(family = 'BH')
        else:
            f_pb['gp'] = f_pbh.get_group_array()
            bh_halos = f_pb.star['gp'][np.where(f_pb.star['tform']<0)[0]]
        bh_halos = bh_halos[np.argsort(bh_mass)[::-1]]

        print "Associated halos: ",bh_halos
        bh_dict_id = db.core.get_or_create_dictionary_item(session, "BH")

        for bhi, haloi in zip(bh_iord, bh_halos):
            haloi = int(haloi)
            bhi = int(bhi)
            halo = f.halos.filter_by(halo_type=0, halo_number=haloi).first()
            bh_obj = f.halos.filter_by(halo_type=1, halo_number=bhi).first()
            if halo is None:
                print "NOTE: skipping BH in halo",haloi,"as no corresponding DB object found"
                continue
            if bh_obj is None:
                print "WARNING: can't find the db object for BH ",bh_iord,"?"
                continue
            existing = halo.links.filter_by(relation_id=bh_dict_id.id,halo_to_id=bh_obj.id).count()

            if existing==0:

                session.merge(db.core.HaloLink(halo,bh_obj,bh_dict_id))
            else:
                print "NOTE: skipping BH in halo",haloi,"as link already exists"

        session.commit()


        del f_pb
        gc.collect()

    print "Generate merger trees...."
    for sim in query.all():
        generate_halolinks(sim)

    session.commit()



if __name__=="__main__":
    parallel_tasks.launch(run)




