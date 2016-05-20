from nose.tools import assert_raises
import halo_db as db
import halo_db.blocking
import halo_db.core.halo
import halo_db.core.simulation
import halo_db.core.timestep
import halo_db.parallel_tasks as pt
import halo_db.parallel_tasks.backend_multiprocessing
import time
from multiprocessing import Process, Condition
import os
import sys
import sqlalchemy.exc

import halo_db


def setup():
    pt.use("multiprocessing")
    try:
        os.remove("test.db")
    except OSError:
        pass
    db.init_db("sqlite:///test.db", timeout=0.1, verbose=False)

    session = db.core.get_default_session()

    sim = halo_db.core.simulation.Simulation("sim")
    session.add(sim)

    ts1 = halo_db.core.timestep.TimeStep(sim, "ts1", False)
    session.add(ts1)

    halo_1 = halo_db.core.halo.Halo(ts1, 1, 0, 0, 0, 0)
    session.add_all([halo_1])

    session.commit()

def teardown():
    try:
        os.remove("test.db")
    except OSError:
        pass


def _multiprocess_block():

    session = db.core.get_default_session()

    ts = halo_db.get_timestep("sim/ts1")
    new_halo = halo_db.core.halo.Halo(ts, 5, 0, 0, 0, 0)

    session.merge(new_halo)
    session.flush()


    time.sleep(1.0)

    session.commit()


def _multiprocess_test():

    time.sleep(0.5)

    ts = halo_db.get_timestep("sim/ts1")



    new_halo = halo_db.core.halo.Halo(ts, 6, 0, 0, 0, 0)

    db.core.get_default_session().merge(new_halo)

    db.core.get_default_session().commit()



def _perform_test(use_blocking=True):
    db.init_db("sqlite:///test.db", timeout=0.1, verbose=False)
    if use_blocking:
        db.blocking.make_engine_blocking()
    print "hello",pt.backend.rank()
    if pt.backend.rank()==1:
        _multiprocess_block()
    elif pt.backend.rank()==2:
        _multiprocess_test()


def test_non_blocking_exception():

    with assert_raises(sqlalchemy.exc.OperationalError):
        pt.launch(_perform_test,3, (False,))

    db.core.get_default_session().rollback()




def test_blocking_avoids_exception():

    assert halo_db.get_halo("sim/ts1/6") is None

    pt.launch(_perform_test,3, (True,))

    assert halo_db.get_halo("sim/ts1/6") is not None
