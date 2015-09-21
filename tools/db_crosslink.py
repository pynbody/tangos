#!/usr/bin/env python2.7
from halo_db import parallel_tasks

import halo_db as db
import sqlalchemy
import time
import pynbody

import terminalcontroller
from terminalcontroller import term


def get_halo_entry(ts, halo_number):
    h = ts.halos.filter_by(halo_number=halo_number).first()
    return h

def need_crosslink_ts(ts1,ts2):
    same_d_id = db.get_or_create_dictionary_item(session, "ptcls_in_common").id
    sources = [h.id for h in ts1.halos.all()]
    targets = [h.id for h in ts2.halos.all()]
    if len(targets)==0 or len(sources)==0:
        print "--> no halos"
        return False

    exists = session.query(db.HaloLink).filter(db.and_(db.HaloLink.halo_from_id.in_(sources), db.HaloLink.relation_id == same_d_id, db.HaloLink.halo_to_id.in_(targets))).count() > 0
    if exists:
        print "--> existing objects"
    return not exists

def create_db_objects_from_catalog(cat, ts1, ts2):
    same_d_id = db.get_or_create_dictionary_item(session, "ptcls_in_common")

    for i, possibilities in enumerate(cat):
        h1 = get_halo_entry(ts1,i)
        for cat_i, weight in possibilities:
            h2 = get_halo_entry(ts2,cat_i)

            if h1 is not None and h2 is not None:
                print i,cat_i,h1,h2,weight
                cx = db.HaloLink(h1, h2, same_d_id, weight)
                session.merge(cx)
    session.commit()


def crosslink_ts(ts1, ts2):
    snap1 = ts1.load()
    snap2 = ts2.load()

    try:
        cat = snap1.bridge(snap2).fuzzy_match_catalog(0, 15, threshold=0.05)
        back_cat = snap2.bridge(snap1).fuzzy_match_catalog(0,15, threshold=0.05)
    except:
        print "ERROR"
        return

    create_db_objects_from_catalog(cat, ts1, ts2)
    create_db_objects_from_catalog(back_cat, ts2, ts1)


def crosslink_sim(sim1, sim2, force=False):
    global session

    assert sim1 != sim2, "Can't link simulation to itself"
    terminalcontroller.heading("ALL: %s -> %s" % (sim1, sim2))
    ts1s = sim1.timesteps
    ts2s = sim2.timesteps
    tasks = parallel_tasks.distributed(ts1s)
    parallel_tasks.mpi_sync_db(session)

    for ts1 in tasks:
        ts2 = min(ts2s, key=lambda ts2: abs(ts2.time_gyr - ts1.time_gyr))
        terminalcontroller.heading(
            "%.2e Gyr -> %.2e Gyr" % (ts1.time_gyr, ts2.time_gyr))
        if need_crosslink_ts(ts1,ts2) or force:
            crosslink_ts(ts1, ts2)

if __name__ == "__main__":
    import sys
    import glob
    import halo_db as db

    db.use_blocking_session()
    session = db.core.internal_session

    ts1 = db.get_item(sys.argv[1])
    ts2 = db.get_item(sys.argv[2])

    if isinstance(ts1, db.Simulation) and isinstance(ts2, db.Simulation):
        crosslink_sim(ts1, ts2)
    elif isinstance(ts1, db.TimeStep) and isinstance(ts2, db.TimeStep):
        crosslink_ts(ts1, ts2)
    else:
        print "Sorry, couldn't work out what to do with your arguments"
