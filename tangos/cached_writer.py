from . import core


def create_property(halo, name, prop, session):

    name = core.dictionary.get_or_create_dictionary_item(session, name)

    if isinstance(prop, core.halo.Halo):
        px = core.halo_data.HaloLink(halo, prop, name)
    else:
        px = core.halo_data.HaloProperty(halo, name, prop)

    px.creator = core.creator.get_creator(session)
    return px


def _insert_list_unlocked(property_list):
    session = core.get_default_session()
    number = 0
    for p in property_list:
        if p[2] is not None:
            session.add(create_property(p[0], p[1], p[2], session))
            number += 1

    session.commit()
    return number

def insert_list(property_list):
    from tangos import parallel_tasks as pt

    if pt.backend!=None:
        with pt.ExclusiveLock("insert_list"):
            return _insert_list_unlocked(property_list)
    else:
        return _insert_list_unlocked(property_list)
