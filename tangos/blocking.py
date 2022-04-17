import sqlalchemy
import sqlalchemy.engine
import sqlalchemy.event

from . import core
from . import parallel_tasks as pt

global rlock

__author__ = 'app'

def event_begin(conn):
    rlock.acquire()

def event_commit_or_rollback(conn):
    rlock.release()


def make_engine_blocking(engine=None):
    global rlock
    if engine is None:
        engine = core.get_default_engine()
    assert isinstance(engine, sqlalchemy.engine.Engine)
    rlock = pt.ExclusiveLock("db_write_lock")

    sqlalchemy.event.listen(engine, 'begin', event_begin )
    sqlalchemy.event.listen(engine, 'commit', event_commit_or_rollback)
    sqlalchemy.event.listen(engine, 'rollback', event_commit_or_rollback)
