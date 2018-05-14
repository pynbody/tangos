"""
Support for creating, querying, and deleting temporary tables containing lists of objects/halos.

This module is used internally by the live calculation and relation finding subpackages.
"""

from __future__ import absolute_import
from . import core
from sqlalchemy import Column, Table, String, Integer, Float, ForeignKey
import sqlalchemy
import random
import string
import contextlib
from six.moves import range

_temp_sessions = {}

def _create_temp_halolist(session):
    global _temp_sessions

    rstr = ''.join(random.choice(string.ascii_lowercase) for _ in range(10))
    halolist_table = Table(
            'halolist_'+rstr,
            core.Base.metadata,
            Column('id',Integer, primary_key=True),
            Column('halo_id',Integer,ForeignKey('halos.id')),
            prefixes = ['TEMPORARY']
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
            *[{'halo_id': id} for id in ids]
        )

def _get_session_for(table):
    global _temp_sessions
    return _temp_sessions[id(table)]

def _get_connection_for(table):
    global _temp_sessions
    return _temp_sessions[id(table)].connection()

def halo_query(table):
    """Return a SQLAlchemy query that selects the objects pointed to by the given temporary table.
    
    Note that because of SQLAlchemy ORM de-duplication, the query will not return multiple rows when
    a halo is referenced multiple times from the table. To access a list of all halos including
    duplicates, see all_halos_with_duplicates.
    """
    session = _get_session_for(table)
    return session.query(core.halo.Halo).select_from(table).join(core.halo.Halo)

def all_halos_with_duplicates(table):
    """Return the Halo objects pointed to by the given temporary table, including duplicates"""
    session = _get_session_for(table)
    return [x[1] for x in session.query(table.c.id, core.halo.Halo).select_from(table).outerjoin(
        core.halo.Halo).all()]

def halolink_query(table):
    """Return a SQLAlchemy query that selects all links pointing away from the objects referred to by the temp table."""
    session = _get_session_for(table)
    return session.query(core.halo_data.HaloLink).select_from(table).join(core.halo_data.HaloLink, core.halo_data.HaloLink.halo_from_id == table.c.halo_id)

@contextlib.contextmanager
def temporary_halolist_table(session, ids=None, callback=None):
    """Context manager to create a temporary list of halos in the database, and ensure it is deleted.
    
    Usage:
    
    with temporary_halolist_table(session, [1,2]) as table:
        print(halo_query(table).all()) # prints repr for halos 1,2
        
    Optionally a callback may be specified which is called once the halolist has been deleted.
    """
    table = _create_temp_halolist(session)
    if ids is not None:
        _insert_into_temp_halolist(table, ids)
    yield table
    _delete_temp_halolist(table)
    if callback is not None:
        callback()
