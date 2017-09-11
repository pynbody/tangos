from __future__ import absolute_import
from __future__ import print_function
from nose.tools import assert_raises
import tangos as db
import tangos.blocking
import tangos.core.halo
import tangos.core.simulation
import tangos.core.timestep
import tangos.parallel_tasks as pt
from tangos import log, testing
import time
import os
import sqlalchemy.exc

import tangos


def setup():
    pt.use("multiprocessing")
    testing.init_blank_db_for_testing(timeout=0.1, verbose=False)

    session = db.core.get_default_session()

    sim = tangos.core.simulation.Simulation("sim")
    session.add(sim)

    ts1 = tangos.core.timestep.TimeStep(sim, "ts1")
    session.add(ts1)

    halo_1 = tangos.core.halo.Halo(ts1, 1, 0, 0, 0, 0)
    session.add_all([halo_1])

    session.commit()

def teardown():
    try:
        os.remove("test.db")
    except OSError:
        pass


def _multiprocess_block():

    session = db.core.get_default_session()

    ts = tangos.get_timestep("sim/ts1")
    new_halo = tangos.core.halo.Halo(ts, 5, 0, 0, 0, 0)

    session.merge(new_halo)
    session.flush()


    time.sleep(1.0)

    session.commit()


def _multiprocess_test():

    time.sleep(0.5)

    ts = tangos.get_timestep("sim/ts1")



    new_halo = tangos.core.halo.Halo(ts, 6, 0, 0, 0, 0)

    db.core.get_default_session().merge(new_halo)

    db.core.get_default_session().commit()



def _perform_test(use_blocking=True):
    db.init_db("sqlite:///test_dbs/test_blocking_session.db", timeout=0.1, verbose=False)
    if use_blocking:
        db.blocking.make_engine_blocking()
    print("hello",pt.backend.rank())
    if pt.backend.rank()==1:
        _multiprocess_block()
    elif pt.backend.rank()==2:
        _multiprocess_test()


def test_non_blocking_exception():

    with assert_raises(sqlalchemy.exc.OperationalError):
        with log.LogCapturer():
            pt.launch(_perform_test,3, (False,))

    db.core.get_default_session().rollback()




def test_blocking_avoids_exception():

    assert tangos.get_halo("sim/ts1/6") is None
    db.core.get_default_session().commit()
    with log.LogCapturer():
        pt.launch(_perform_test,3, (True,))

    assert tangos.get_halo("sim/ts1/6") is not None
