from __future__ import absolute_import
from __future__ import print_function
from sqlalchemy.orm import Session
import numpy as np
from .core import get_or_create_dictionary_item, Halo, HaloLink, TrackData
from . import query
import six
import tangos.parallel_tasks as parallel_tasks
from six.moves import zip

def generate_tracker_halo_link_if_not_present(halo_1, halo_2, dict_obj=None, weight=1.0):
    assert isinstance(halo_1, Halo)
    assert isinstance(halo_2, Halo)
    session = Session.object_session(halo_1)
    if session.query(HaloLink).filter_by(halo_from_id=halo_1.id, halo_to_id=halo_2.id).count()>0:
        return

    if dict_obj is None:
        dict_obj = get_or_create_dictionary_item(session, "tracker")
    #print halo_1.id,"->",halo_2.id
    session.merge(HaloLink(halo_1,halo_2,dict_obj,weight))

def get_trackers(sim):
    trackers = sim.trackers.all()
    nums = [tx.halo_number for tx in trackers]
    return trackers, np.array(nums)

def get_tracker_halos(ts):
    halos = ts.trackers.order_by(Halo.halo_number).all()
    hid = [h.id for h in halos]
    num = [h.halo_number for h in halos]
    return halos, np.array(num), np.array(hid)

def get_tracker_links(session, relation):
    links = session.query(HaloLink).filter_by(relation=relation).all()
    idf = [tl.halo_from_id for tl in links]
    idt = [tl.halo_to_id for tl in links]
    return links, np.array(idf), np.array(idt)

def generate_tracker_halo_links(sim, session):
    dict_obj = get_or_create_dictionary_item(session, "tracker")
    links = []
    for ts1, ts2 in parallel_tasks.distributed(list(zip(sim.timesteps[1:],sim.timesteps[:-1]))):
        print("generating links for", ts1, ts2)
        halos_1, nums1, id = get_tracker_halos(ts1)
        halos_2, nums2, id = get_tracker_halos(ts2)
        tracker_links, idf, idt = get_tracker_links(session,dict_obj)

        if len(nums1) == 0 or len(nums2) == 0:
            continue
        o1 = np.where(np.in1d(nums1,nums2))[0]
        o2 = np.where(np.in1d(nums2,nums1))[0]
        if len(o1) == 0 or len(o2) == 0:
            continue
        for ii, jj in zip(o1,o2):
            if halos_1[ii].halo_number != halos_2[jj].halo_number:
                raise RuntimeError("ERROR mismatch of BH iords")
            exists = np.where((idf==halos_1[ii].id)&(idt==halos_2[jj].id))[0]
            if len(exists) == 0:
                links.append(HaloLink(halos_1[ii],halos_2[jj],dict_obj,1.0))
                links.append(HaloLink(halos_2[jj],halos_1[ii],dict_obj,1.0))
    session.add_all(links)
    session.commit()

def new(for_simulation, using_particles):
    if isinstance(for_simulation, six.string_types):
        for_simulation = query.get_simulation(for_simulation)
    tracker = TrackData(for_simulation)
    use_iord = 'iord' in using_particles.loadable_keys()
    tracker.select(using_particles, use_iord)
    session = Session.object_session(for_simulation)
    session.add(tracker)
    tracker.create_objects()
    tracker.create_links()
    session.commit()
    return tracker.halo_number