from tangos import parallel_tasks as pt

from . import core, query
from .log import logger
from .util import proxy_object, timestep_object_cache


def create_property(halo, name, prop, session):
    name = core.dictionary.get_or_create_dictionary_item(session, name)

    if name.id is None:
        logger.info("Created a new dictionary item for %s", name.text)
        session.merge(name)
        session.commit()

    name_id = name.id
    assert name_id is not None, f"Problem with creating a dictionary item for {name.text}"

    if isinstance(prop, core.halo.Halo):
        px = core.halo_data.HaloLink(halo.id, prop.id, name_id)
    else:
        px = core.halo_data.HaloProperty(halo.id, name_id, prop)

    px.creator_id = core.creator.get_creator(session).id
    return px

def _resolve_proxy(possibly_proxy, timestep_cache: timestep_object_cache.TimestepObjectCache):
    if isinstance(possibly_proxy, proxy_object.ProxyObjectBase):
        # TODO: possible optimization here using relative_to_timestep_cache
        result = possibly_proxy.relative_to_timestep_cache(timestep_cache).resolve(core.get_default_session())
    else:
        result = possibly_proxy
    return result

def _insert_list_unlocked(property_list, timestep_id):
    session = core.get_default_session()
    number = 0
    timestep_cache = timestep_object_cache.TimestepObjectCache(query.get_timestep(timestep_id, session=session))
    objects = []
    for p in property_list:
        p = [_resolve_proxy(pi, timestep_cache) for pi in p]
        if p[2] is not None:
            objects.append(create_property(*p, session))
            number += 1

    session.bulk_save_objects(objects)
    session.commit()
    return number

def insert_list(property_list, timestep_id, commit_on_server):
    if pt.backend!=None:
        with pt.ExclusiveLock("insert_list"):
            if commit_on_server:
                # nb though it looks weird to have a lock when the commit will be
                # processed on the server process, this is actually just easier than attaining
                # the lock on the server
                return PropertyListCommitMessage((property_list, timestep_id)).send_and_get_response(0)
            else:
                return _insert_list_unlocked(property_list, timestep_id)
    else:
        if commit_on_server:
            raise ValueError("insert_list called with commit_on_server=True, but no parallel backend is initialised")
        return _insert_list_unlocked(property_list, timestep_id)

class PropertyListCommitMessage(pt.message.MessageWithResponse):
    def __init__(self, contents=None):
        super().__init__(contents)

    def process(self):
        result = _insert_list_unlocked(*self.contents)
        self.respond(result)
