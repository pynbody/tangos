from __future__ import absolute_import
from . import core

def create_property(halo, name, prop, session):

    name = core.dictionary.get_or_create_dictionary_item(session, name)

    if isinstance(prop, core.halo.Halo):
        px = core.halo_data.HaloLink(halo, prop, name)
    else:
        px = core.halo_data.HaloProperty(halo, name, prop)

    px.creator_id = core.creator.get_creator().id
    return px


def _insert_list_unlocked(property_list):
    session = core.get_default_session()

    property_object_list = [create_property(
        p[0], p[1], p[2], session) for p in property_list if p[2] is not None]

    session.add_all(property_object_list)

    session.commit()

def insert_list(property_list):
    from tangos import parallel_tasks as pt

    if pt.backend!=None:
        with pt.ExclusiveLock("insert_list"):
            _insert_list_unlocked(property_list)
    else:
        _insert_list_unlocked(property_list)


