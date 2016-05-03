from sqlalchemy.orm import Session
from .core import Halo, HaloLink, get_or_create_dictionary_item

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


def generate_tracker_halo_links(sim):
    for ts1, ts2 in zip(sim.timesteps[1:],sim.timesteps[:-1]):
        print "generating links for", ts1, ts2
        halos_1 = ts1.halos.filter_by(halo_type=1).all()
        halos_2 = ts2.halos.filter_by(halo_type=1).all()
        for halo_1 in halos_1:
            halo_2 = filter(lambda x: x.halo_number==halo_1.halo_number, halos_2)
            if len(halo_2)!=0:
                generate_tracker_halo_link_if_not_present(halo_1, halo_2[0])
                generate_tracker_halo_link_if_not_present(halo_2[0], halo_1)