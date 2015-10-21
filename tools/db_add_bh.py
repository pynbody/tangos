#!/usr/bin/env python2.7

import halo_db as db
import halo_db.parallel_tasks as parallel_tasks
import sys
import numpy as np
import gc


session = db.core.internal_session


if __name__=="__main__":
    query = db.sim_query_from_args(sys.argv, session)

    files = db.core.internal_session.query(db.TimeStep).filter(
        db.TimeStep.simulation_id.in_([q.id for q in query.all()])). \
        order_by(db.TimeStep.time_gyr).all()

    import sim_output_finder

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

        f_pb['gp'] = f_pb.halos().get_group_array()

        bh_halos = f_pb.star['gp'][np.where(f_pb.star['tform']<0)[0]]
        bh_halos = bh_halos[np.argsort(bh_mass)[::-1]]
        print "Associated halos: ",bh_halos

        bh_dict_id = db.core.get_or_create_dictionary_item(session, "BH")
        
        for bhi, haloi in zip(bh_iord, bh_halos):
            haloi = int(haloi)
            bhi = int(bhi)
            halo = f.halos.filter_by(halo_type=0, halo_number=haloi).first()
            if halo is None:
                print "NOTE: skipping BH in halo",haloi,"as no corresponding DB object found"
                continue
            obj = f.halos.filter_by(halo_type=1, halo_number=bhi).first()

            session.merge(db.core.HaloLink(halo,obj,bh_dict_id))


        session.commit()


        del f_pb
        gc.collect()
