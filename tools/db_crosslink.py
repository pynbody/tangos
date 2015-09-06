#!/usr/bin/env python
from halo_db import parallel_tasks

import halo_db as db
import sqlalchemy
import time
import pynbody

import terminalcontroller
from terminalcontroller import term


def need_crosslink_ts(ts1,ts2):
    same_d_id = db.get_or_create_dictionary_item(session, "same_as").id
    sources = [h.id for h in ts1.halos.all()]
    targets = [h.id for h in ts2.halos.all()]
    if len(targets)==0 or len(sources)==0:
        print "--> no halos"
        return False

    exists = session.query(db.HaloLink).filter(db.and_(db.HaloLink.halo_from_id.in_(sources), db.HaloLink.relation_id == same_d_id, db.HaloLink.halo_to_id.in_(targets))).count() > 0
    if exists:
        print "--> existing objects"
    return not exists

def crosslink_ts(ts1, ts2):
    snap1 = ts1.load()
    snap2 = ts2.load()

    try:
        cat = snap1.bridge(snap2).match_catalog(0, 1000)
        back_cat = snap2.bridge(snap1).match_catalog(0,1000)
    except:
        print "ERROR"
        return

    same_d_id = db.get_or_create_dictionary_item(session, "same_as")
    contained_d_id = db.get_or_create_dictionary_item(session, "contained_in")

    for i, cat_i in enumerate(cat):
        if cat_i<0:
            continue
        if back_cat[cat[i]]!=i:
            print "Skip halo",i,"because the backwards-matching doesn't give the same result as the forwards matching"
            continue
        h1 = ts1.halos.filter_by(halo_number=i).first()
        h2 = ts2.halos.filter_by(halo_number=cat_i).first()
        if h1 != None and h2 != None:
            cx = db.HaloLink(h1, h2, same_d_id)
            session.merge(cx)
            print "Create:", cx
        else:
            print "No halo DB entry for link ", i, "->", cat_i
    session.commit()


def crosslink_sim(sim1, sim2):
    global session

    assert sim1 != sim2, "Can't link simulation to itself"
    terminalcontroller.heading("ALL: %s -> %s" % (sim1, sim2))
    ts1s = sim1.timesteps
    ts2s = sim2.timesteps
    parallel_tasks.mpi_sync_db(session)

    for ts1 in parallel_tasks.distributed(ts1s):
        ts2 = min(ts2s, key=lambda ts2: abs(ts2.time_gyr - ts1.time_gyr))
        terminalcontroller.heading(
            "%.2e Gyr -> %.2e Gyr" % (ts1.time_gyr, ts2.time_gyr))
        if need_crosslink_ts(ts1,ts2):
            crosslink_ts(ts1, ts2)

if __name__ == "__main__":
    import sys
    import glob
    import halo_db as db
    session = db.internal_session

    ts1 = db.get_item(sys.argv[1])
    ts2 = db.get_item(sys.argv[2])

    if isinstance(ts1, db.Simulation) and isinstance(ts2, db.Simulation):
        crosslink_sim(ts1, ts2)
    elif isinstance(ts1, db.TimeStep) and isinstance(ts2, db.TimeStep):
        crosslink_ts(ts1, ts2)
    else:
        print "Sorry, couldn't work out what to do with your arguments"
