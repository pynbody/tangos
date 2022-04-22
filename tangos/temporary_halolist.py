import contextlib
import random
import string

import sqlalchemy
from sqlalchemy import Column, Integer, Table

from . import core

_temp_sessions = {}

def _create_temp_halolist(session):
    global _temp_sessions
    rstr = ''.join(random.choice(string.ascii_lowercase) for _ in range(10))
    halolist_table = Table(
            'halolist_'+rstr,
            core.Base.metadata,
            Column('id',Integer, primary_key=True),
            Column('halo_id',Integer), # don't declare ForeignKey, as MySQL can't handle it
            prefixes = ["TEMPORARY"]
        )

    halolist_table.create(bind=session.connection())

    _temp_sessions[id(halolist_table)] = session
    return halolist_table

def _delete_temp_halolist(table):
    global _temp_sessions
    connection = _get_connection_for(table)
    table.drop(bind=connection)
    core.Base.metadata.remove(table)
    del _temp_sessions[id(table)]

def _insert_into_temp_halolist(table, ids):
    connection = _get_connection_for(table)
    if isinstance(ids, sqlalchemy.orm.query.Query):
        connection.execute(table.insert().from_select(['halo_id'], ids))
    else:
        connection.execute(
            table.insert(),
            [{'halo_id': id} for id in ids]
        )

def _get_session_for(table):
    global _temp_sessions
    return _temp_sessions[id(table)]

def _get_connection_for(table):
    global _temp_sessions
    return _temp_sessions[id(table)].connection()

def halo_query(table):
    """Query that returns all halos referred to from the temporary table.

    Note that due to SQLALchemy's de-dup behaviour, the return is not guaranteed to be in
    1-1 correspondence with the rows in the temporary table. For this, you need to use
    enumerated_halo_query"""
    session = _get_session_for(table)
    return session.query(core.halo.SimulationObjectBase).select_from(table).join(core.halo.SimulationObjectBase, table.c.halo_id == core.halo.SimulationObjectBase.id).order_by(table.c.id)

def enumerated_halo_query(table):
    """Query that returns tuples of id, halo for each row in the temporary table"""
    session = _get_session_for(table)
    return session.query(table.c.id, core.halo.SimulationObjectBase).select_from(table).outerjoin(core.halo.SimulationObjectBase, table.c.halo_id == core.halo.SimulationObjectBase.id).order_by(table.c.id)

def all_halos_with_duplicates(table):
    """Return all halos in the temporary table, including duplicates"""
    _get_session_for(table)
    return [x[1] for x in enumerated_halo_query(table).all()]

def halolink_query(table):
    session = _get_session_for(table)
    return session.query(core.halo_data.HaloLink).select_from(table).join(core.halo_data.HaloLink, core.halo_data.HaloLink.halo_from_id == table.c.halo_id).order_by(table.c.id)

@contextlib.contextmanager
def temporary_halolist_table(session, ids=None, callback=None):

    table = _create_temp_halolist(session)
    if ids is not None:
        _insert_into_temp_halolist(table, ids)
    yield table
    _delete_temp_halolist(table)
    if callback is not None:
        callback()
