from sqlalchemy.orm import Session
from .core import Halo, HaloLink, get_or_create_dictionary_item
import halo_db.parallel_tasks as parallel_tasks
import numpy as np

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


def generate_tracker_halo_links(sim, session=None):
    for ts1, ts2 in zip(sim.timesteps[1:],sim.timesteps[:-1]):
        print "generating links for", ts1, ts2
        halos_1 = ts1.halos.filter_by(halo_type=1).order_by(Halo.halo_number).all()
        halos_2 = ts2.halos.filter_by(halo_type=1).order_by(Halo.halo_number).all()

        nums1 = [h.halo_number for h in halos_1]
        nums2 = [h.halo_number for h in halos_2]
        if len(nums1) == 0 or len(nums2) == 0:
            continue
        o1 = np.where(np.in1d(nums1,nums2))[0]
        o2 = np.where(np.in1d(nums2,nums1))[0]
        if len(o1) == 0 or len(o2) == 0:
            continue
        for ii, jj in zip(o1,o2):
            if halos_1[ii].halo_number != halos_2[jj].halo_number:
                raise RuntimeError("ERROR mismatch of BH iords")
            generate_tracker_halo_link_if_not_present(halos_1[ii],halos_2[jj])
            generate_tracker_halo_link_if_not_present(halos_2[jj], halos_1[ii])
        if session:
            session.commit()