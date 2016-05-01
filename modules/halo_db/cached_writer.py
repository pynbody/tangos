import halo_db as db
import halo_db.core.dictionary
import halo_db.core.halo
import halo_db.core.halo_data


def create_property(halo, name, prop, session):

    name = halo_db.core.dictionary.get_or_create_dictionary_item(session, name)

    if isinstance(prop, halo_db.core.halo.Halo):
        px = halo_db.core.halo_data.HaloLink(halo, prop, name)
    else:
        px = halo_db.core.halo_data.HaloProperty(halo, name, prop)

    px.creator = db.core.creator.get_creator()
    return px


def _insert_list_unlocked(property_list):
    session = db.core.internal_session

    property_object_list = [create_property(
        p[0], p[1], p[2], session) for p in property_list if p[2] is not None]

    session.add_all(property_object_list)

    session.commit()

def insert_list(property_list):
    from halo_db import parallel_tasks as pt

    if pt.backend!=None:
        with pt.RLock("insert_list"):
            _insert_list_unlocked(property_list)
    else:
        _insert_list_unlocked(property_list)


