from . import core
import traceback
import sys
import sqlalchemy
from . import parallel_tasks as pt

def get_halo_entry(ts, halo_number):
    h = ts.halos.filter_by(halo_number=halo_number).first()
    return h


def need_crosslink_ts(ts1,ts2,session=None):
    session = session or core.get_default_session()
    same_d_id = core.dictionary.get_or_create_dictionary_item(session, "ptcls_in_common").id
    sources = [h.id for h in ts1.halos.all()]
    targets = [h.id for h in ts2.halos.all()]
    if len(targets)==0 or len(sources)==0:
        print "--> no halos"
        return False

    halo_source = sqlalchemy.orm.aliased(core.halo.Halo, name="halo_source")
    halo_target = sqlalchemy.orm.aliased(core.halo.Halo, name="halo_target")
    exists = session.query(core.halo_data.HaloLink).join(halo_source, core.halo_data.HaloLink.halo_from).\
                                        join(halo_target, core.halo_data.HaloLink.halo_to).\
                                        filter(halo_source.timestep_id == ts1.id, halo_target.timestep_id == ts2.id,
                                               core.halo_data.HaloLink.relation_id == same_d_id).count() > 0

    if exists:
        print "--> existing objects"
    return not exists


def create_db_objects_from_catalog(cat, ts1, ts2, session=None):
    session = session or core.get_default_session()
    same_d_id = core.dictionary.get_or_create_dictionary_item(session, "ptcls_in_common")
    items = []
    for i, possibilities in enumerate(cat):
        h1 = get_halo_entry(ts1,i)
        for cat_i, weight in possibilities:
            h2 = get_halo_entry(ts2,cat_i)

            if h1 is not None and h2 is not None:
                print i,cat_i,h1,h2,weight
                items.append(core.halo_data.HaloLink(h1, h2, same_d_id, weight))

    with pt.RLock("create_db_objects_from_catalog"):
        session.add_all(items)
        session.commit()


def crosslink_ts(ts1, ts2, halo_min=0, halo_max=100, dmonly=False, session=None):
    snap1 = ts1.load()
    snap2 = ts2.load()

    try:
        if dmonly is True:
            cat = snap1.bridge(snap2).fuzzy_match_catalog(halo_min, halo_max, threshold=0.005, only_family='dark')
            back_cat = snap2.bridge(snap1).fuzzy_match_catalog(halo_min,halo_max, threshold=0.005, only_family='dark')
        else:
            cat = snap1.bridge(snap2).fuzzy_match_catalog(halo_min, halo_max, threshold=0.005)
            back_cat = snap2.bridge(snap1).fuzzy_match_catalog(halo_min,halo_max, threshold=0.005)
    except:
        print "ERROR"
        traceback.print_exc(file=sys.stderr)
        return

    create_db_objects_from_catalog(cat, ts1, ts2,session)
    create_db_objects_from_catalog(back_cat, ts2, ts1,session)
