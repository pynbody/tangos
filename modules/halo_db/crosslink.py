from . import core
import traceback
import sys
import sqlalchemy

def get_halo_entry(ts, halo_number):
    h = ts.halos.filter_by(halo_number=halo_number).first()
    return h


def need_crosslink_ts(ts1,ts2,session=None):
    session = session or core.internal_session
    same_d_id = core.get_or_create_dictionary_item(session, "ptcls_in_common").id
    sources = [h.id for h in ts1.halos.all()]
    targets = [h.id for h in ts2.halos.all()]
    if len(targets)==0 or len(sources)==0:
        print "--> no halos"
        return False

    halo_source = sqlalchemy.orm.aliased(core.Halo,name="halo_source")
    halo_target = sqlalchemy.orm.aliased(core.Halo,name="halo_target")
    exists = session.query(core.HaloLink).join(halo_source,core.HaloLink.halo_from).\
                                        join(halo_target,core.HaloLink.halo_to).\
                                        filter(halo_source.timestep_id==ts1.id, halo_target.timestep_id==ts2.id,
                                               core.HaloLink.relation_id == same_d_id).count() > 0

    if exists:
        print "--> existing objects"
    return not exists


def create_db_objects_from_catalog(cat, ts1, ts2, session=None):
    session = session or core.internal_session
    same_d_id = core.get_or_create_dictionary_item(session, "ptcls_in_common")

    for i, possibilities in enumerate(cat):
        h1 = get_halo_entry(ts1,i)
        for cat_i, weight in possibilities:
            h2 = get_halo_entry(ts2,cat_i)

            if h1 is not None and h2 is not None:
                print i,cat_i,h1,h2,weight
                cx = core.HaloLink(h1, h2, same_d_id, weight)
                session.merge(cx)
    session.commit()


def crosslink_ts(ts1, ts2, halo_min=0, halo_max=100, session=None):
    snap1 = ts1.load()
    snap2 = ts2.load()

    try:
        cat = snap1.bridge(snap2).fuzzy_match_catalog(halo_min, halo_max, threshold=0.05)
        back_cat = snap2.bridge(snap1).fuzzy_match_catalog(halo_min,halo_max, threshold=0.05)
    except:
        print "ERROR"
        traceback.print_exc(file=sys.stderr)
        return

    create_db_objects_from_catalog(cat, ts1, ts2,session)
    create_db_objects_from_catalog(back_cat, ts2, ts1,session)
