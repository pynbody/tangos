import halo_db as db

def create_property(halo, name, prop, session):

    name = db.get_or_create_dictionary_item(session, name)

    if isinstance(prop, db.Halo):
        px = db.HaloLink(halo, prop, name)
    else:
        px = db.HaloProperty(halo, name, prop)

    px.creator = db.core.current_creator
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


