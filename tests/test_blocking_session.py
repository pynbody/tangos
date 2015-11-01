from nose.tools import assert_raises
import halo_db as db
import time
from multiprocessing import Process, Condition
import os
import sys
import sqlalchemy.exc

def setup():
    try:
        os.remove("test.db")
    except OSError:
        pass
    db.init_db("sqlite:///test.db", timeout=0.1)

    session = db.core.internal_session

    sim = db.Simulation("sim")
    session.add(sim)

    ts1 = db.TimeStep(sim,"ts1",False)
    session.add(ts1)

    halo_1 = db.core.Halo(ts1,1,0,0,0,0)
    session.add_all([halo_1])

    session.commit()

def teardown():
    try:
        os.remove("test.db")
    except OSError:
        pass


def _multiprocess_block(ready_condition):
    db.init_db("sqlite:///test.db")
    session = db.core.internal_session

    ts = db.get_timestep("sim/ts1")
    new_halo = db.core.Halo(ts,5,0,0,0,0)

    session.merge(new_halo)
    session.flush()

    ready_condition.acquire()
    ready_condition.notify()
    ready_condition.release()
    time.sleep(0.5)

    session.commit()


def test_non_blocking_exception():
    ready = Condition()
    p = Process(target=_multiprocess_block, args=(ready,))

    p.start()
    ts = db.get_timestep("sim/ts1")

    ready.acquire()
    ready.wait()
    ready.release()

    new_halo = db.core.Halo(ts,6,0,0,0,0)

    with assert_raises(sqlalchemy.exc.OperationalError):
        db.core.internal_session.merge(new_halo)

    db.core.internal_session.rollback()

    p.join()



def test_blocking_avoids_exception():
    db.core.use_blocking_session()
    ready = Condition()
    p = Process(target=_multiprocess_block, args=(ready,))

    p.start()
    ts = db.get_timestep("sim/ts1")

    ready.acquire()
    ready.wait()
    ready.release()

    new_halo = db.core.Halo(ts,6,0,0,0,0)

    db.core.internal_session.merge(new_halo)

    p.join()


