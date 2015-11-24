import sqlalchemy, sqlalchemy.engine, sqlalchemy.event
from . import parallel_tasks as pt
global rlock

__author__ = 'app'

"""
class BlockingSession(Session):
    def _execute_with_retries(self, fn, *params, **kwparams):
        retries = 20
        while retries > 0:
            try:
                return fn(*params, **kwparams)
            except sqlalchemy.exc.OperationalError:
                super(BlockingSession,self).rollback()
                retries -= 1
                if retries > 0:
                    print "DB is locked (%d attempts remain)..." % retries
                    time.sleep(1)
                else:
                    raise

    def flush(self, *params, **kwparams):
        return self._execute_with_retries(super(BlockingSession, self).flush, *params, **kwparams)

    def execute(self, *params, **kwparams):
        return self._execute_with_retries(super(BlockingSession, self).execute, *params, **kwparams)
"""

def event_begin(conn):
    rlock.acquire()

def event_commit_or_rollback(conn):
    rlock.release()


def make_engine_blocking(engine):
    global rlock
    assert isinstance(engine, sqlalchemy.engine.Engine)
    rlock = pt.RLock("db_write_lock")

    sqlalchemy.event.listen(engine, 'begin', event_begin )
    sqlalchemy.event.listen(engine, 'commit', event_commit_or_rollback)
    sqlalchemy.event.listen(engine, 'rollback', event_commit_or_rollback)