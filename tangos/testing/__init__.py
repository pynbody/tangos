import contextlib
import gc
import inspect
import os
import traceback

import sqlalchemy
import sqlalchemy.event
from sqlalchemy import create_engine, text

from .. import core, get_halo
from ..config import testing_db_backend, testing_db_password, testing_db_user


def _as_halos(hlist, session=None):
    if session is None:
        session = core.get_default_session()
    rvals = []
    for h in hlist:
        if h is None:
            rvals.append(None)
        elif isinstance(h, core.halo.SimulationObjectBase):
            rvals.append(h)
        elif isinstance(h, list) or isinstance(h, tuple):
            rvals.append(_as_halos(h, session))
        else:
            rvals.append(get_halo(h, session))
    return rvals

def _halos_to_strings(hlist):
    if len(hlist)==0:
        return "(empty list)"
    else:
        strlist = []
        for h in hlist:
            if isinstance(h, list) or isinstance(h, tuple):
                strlist.append(_halos_to_strings(h))
            elif isinstance(h,str):
                strlist.append(h)
            else:
                strlist.append(h.path)
        return str(strlist)

def halolists_equal(hl1, hl2, session=None):
    """Return True if hl1 and hl2 are equivalent lists of halos"""

    hl1 = _as_halos(hl1)
    hl2 = _as_halos(hl2)

    return len(hl1)==len(hl2) and all([h1==h2 for h1, h2 in zip(hl1,hl2)])

def assert_halolists_equal(hl1, hl2, session=None):
    equal = halolists_equal(hl1, hl2, session=None)
    assert equal, "Not equal: %s %s"%(_halos_to_strings(hl1),_halos_to_strings(hl2))

@contextlib.contextmanager
def autorevert():
    old_session = core.get_default_session()
    connection = core.get_default_engine().connect()
    transaction = connection.begin()
    isolated_session = core.Session(bind=connection)
    core.set_default_session(isolated_session)
    yield
    transaction.rollback()
    isolated_session.close()
    connection.close()
    core.set_default_session(old_session)

@contextlib.contextmanager
def assert_connections_all_closed():
    num_connections = [0,0]
    connection_details = {}
    def on_checkout(dbapi_conn, connection_rec, connection_proxy):
        num_connections[0]+=1
        num_connections[1]+=1
        connection_details[id(connection_rec)] = traceback.extract_stack()

    def on_checkin(dbapi_conn, connection_rec):
        if id(connection_rec) in connection_details:
            num_connections[0]-=1
            del connection_details[id(connection_rec)]

    gc.collect()

    sqlalchemy.event.listen(core.get_default_engine().pool, 'checkout', on_checkout)
    sqlalchemy.event.listen(core.get_default_engine().pool, 'checkin', on_checkin)

    yield


    gc.collect()

    sqlalchemy.event.remove(core.get_default_engine().pool, 'checkout', on_checkout)
    sqlalchemy.event.remove(core.get_default_engine().pool, 'checkin', on_checkin)

    for k,v in connection_details.items():
        print("object id",k,"not checked in; was created here:")
        for line in traceback.format_list(v):
            print("  ",line)

    assert num_connections[0]==0, "%d (of %d) connections were not closed"%(num_connections[0], num_connections[1])

class SqlExecutionTracker:
    """Logs queries performed against the given sqlalchemy connection.

    Based on https://stackoverflow.com/questions/19073099/how-to-count-sqlalchemy-queries-in-unit-tests

    Usage:
        with SqlExecutionCounter(conn) as ctr:
            conn.execute("SELECT 1")
            conn.execute("SELECT 1")
        assert ctr.count == 2
        assert "select" in ctr
        assert "update" not in ctr
    """
    def __init__(self, conn=None):
        if conn is None:
            conn = core.get_default_engine()

        self.conn = conn
        self._queries = []
        self._stacks = []

    def __enter__(self):
        sqlalchemy.event.listen(self.conn, 'after_execute', self.callback)
        return self

    def __exit__(self, *_):
        sqlalchemy.event.remove(self.conn, 'after_execute', self.callback)

    @property
    def count(self):
        return len(self._queries)

    def get_statement(self, i):
        return self._queries[i]

    def count_statements_containing(self, search_string):
        return sum(self.statements_contain(search_string))

    def traceback_statements_containing(self, search_string):
        return [tb for include, tb in zip(self.statements_contain(search_string),
                                          self._stacks)
                if include]

    def statements_contain(self, search_string):
        return [search_string.lower() in q.lower() for q in self._queries]

    def __contains__(self, search_string):
        return (any(self.statements_contain(search_string)))

    def callback(self, conn, query, *_):
        self._queries.append(str(query))
        self._stacks.append("".join(traceback.format_list(traceback.extract_stack()[:-2])))

def init_blank_db_for_testing(**init_kwargs):
    """Initialize a blank database for testing purposes.

    :arg testing_db_name: name of the database to create.  Defaults to the name of the calling file.
    :arg erase_if_exists: if True (default), delete the database if it already exists.

    Other arguments are passed to tangos.core.init_db

    :returns: True if a blank database has been created. False if the database already existed and erase_if_exists was False.
    """

    db_is_blank = False

    core.close_db()

    try:
        os.mkdir("test_dbs")
    except OSError:
        pass

    caller_fname = os.path.basename(inspect.getframeinfo(inspect.currentframe().f_back)[0])[:-3]

    testing_db_name = init_kwargs.pop("testing_db_name", caller_fname)
    erase_if_exists = init_kwargs.pop("erase_if_exists", True)

    if testing_db_backend == "sqlite":
        db_name = f"test_dbs/%s.db"%testing_db_name
        if erase_if_exists:
            try:
                os.remove(db_name)
            except OSError:
                pass
            db_is_blank = True
        else:
            db_is_blank = not os.path.exists(db_name)

        core.init_db(f"sqlite:///{db_name}", **init_kwargs)
    else:
        db_url = f"{testing_db_backend}://{testing_db_user}:{testing_db_password}@localhost"
        engine = create_engine(db_url)

        if erase_if_exists:
            with engine.connect() as conn:
                conn.execute(text("COMMIT"))
                conn.execute(text(f"DROP DATABASE IF EXISTS {testing_db_name}"))
                conn.execute(text("COMMIT"))
                conn.execute(text(f"CREATE DATABASE {testing_db_name}"))
            db_is_blank = True
        else:
            try:
                create_engine(f"{db_url}/{testing_db_name}").connect()
            except sqlalchemy.exc.OperationalError:
                with engine.connect() as conn:
                    conn.execute(text("COMMIT"))
                    conn.execute(text(f"CREATE DATABASE {testing_db_name}"))
                    db_is_blank = True
        core.init_db(f"{db_url}/{testing_db_name}", **init_kwargs)

    return db_is_blank
