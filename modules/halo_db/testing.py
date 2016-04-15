from . import core
import sqlalchemy, sqlalchemy.event
import contextlib
import gc
import traceback

def add_symmetric_link(h1, h2, weight=1.0):
    rel = core.get_or_create_dictionary_item(core.internal_session, "ptcls_in_common")
    core.internal_session.add_all([core.HaloLink(h1,h2,rel,weight), core.HaloLink(h2,h1,rel,weight)])


def _as_halos(hlist, session=None):
    if session is None:
        session = core.internal_session
    rvals = []
    for h in hlist:
        if isinstance(h, core.Halo):
            rvals.append(h)
        else:
            rvals.append(core.get_halo(h, session))
    return rvals

def _halos_to_strings(hlist):
    return [hx.path for hx in _as_halos(hlist)]

def halolists_equal(hl1, hl2, session=None):
    """Return True if hl1 and hl2 are equivalent lists of halos"""

    hl1 = _as_halos(hl1)
    hl2 = _as_halos(hl2)

    return len(hl1)==len(hl2) and all([h1==h2 for h1, h2 in zip(hl1,hl2)])

def assert_halolists_equal(hl1, hl2, session=None):
    equal = halolists_equal(hl1, hl2, session=None)
    assert equal, "Not equal: %s %s"%(_halos_to_strings(hl1),_halos_to_strings(hl2))


@contextlib.contextmanager
def assert_connections_all_closed():
    num_connections = [0,0]
    connection_details = {}
    def on_checkout(dbapi_conn, connection_rec, connection_proxy):
        num_connections[0]+=1
        num_connections[1]+=1
        connection_details[id(connection_rec)] = traceback.extract_stack()

    def on_checkin(dbapi_conn, connection_rec):
        num_connections[0]-=1
        del connection_details[id(connection_rec)]

    gc.collect()

    sqlalchemy.event.listen(core.engine.pool, 'checkout', on_checkout)
    sqlalchemy.event.listen(core.engine.pool, 'checkin', on_checkin)

    yield

    gc.collect()

    sqlalchemy.event.remove(core.engine.pool, 'checkout', on_checkout)
    sqlalchemy.event.remove(core.engine.pool, 'checkin', on_checkin)

    for k,v in connection_details.iteritems():
        print "object id",k,"not checked in; was created here:"
        for line in traceback.format_list(v):
            print "  ",line

    assert num_connections[0]==0, "%d (of %d) connections were not closed"%(num_connections[0], num_connections[1])