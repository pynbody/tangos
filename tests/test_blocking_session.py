import contextlib
import os
import time

import sqlalchemy.exc
from pytest import raises as assert_raises
from pytest import skip

import tangos
import tangos as db
import tangos.blocking
import tangos.core.halo
import tangos.core.simulation
import tangos.core.timestep
import tangos.parallel_tasks as pt
from tangos import log, testing
from tangos.config import testing_db_backend


def setup_module():
    if testing_db_backend != "sqlite":
        skip("This test is only relevant for sqlite databases")

    pt.use("multiprocessing")
    testing.init_blank_db_for_testing(timeout=0.1, verbose=False)

    session = db.core.get_default_session()

    sim = tangos.core.simulation.Simulation("sim")
    session.add(sim)

    ts1 = tangos.core.timestep.TimeStep(sim, "ts1")
    session.add(ts1)

    halo_1 = tangos.core.halo.Halo(ts1, 1, 1, 1, 0, 0, 0, 0)
    session.add_all([halo_1])

    session.commit()

def teardown_module():
    tangos.core.close_db()
    try:
        os.remove("test.db")
    except OSError:
        pass


def _multiprocess_block():

    session = db.core.get_default_session()

    ts = tangos.get_timestep("sim/ts1")
    tangos.core.creator.get_creator(session) # this stops a warning being issued if the creator object is created during creation of Halo
    new_halo = tangos.core.halo.Halo(ts, 5, 5, 5, 0, 0, 0, 0)

    session.add(new_halo)
    session.flush()


    time.sleep(1.0)

    session.commit()


def _multiprocess_test():
    session = db.core.get_default_session()

    time.sleep(0.5)

    ts = tangos.get_timestep("sim/ts1")

    tangos.core.creator.get_creator(session) # this stops a warning being issued if the creator object is created during creation of Halo
    new_halo = tangos.core.halo.Halo(ts, 6, 6, 6, 0, 0, 0, 0)

    session.add(new_halo)

    session.commit()



def _perform_test(use_blocking=True):
    if pt.backend.rank()==1:
        db.init_db("sqlite:///test_dbs/test_blocking_session.db", timeout=0.1, verbose=False)
        pt.barrier()
    else:
        pt.barrier()
        db.init_db("sqlite:///test_dbs/test_blocking_session.db", timeout=0.1, verbose=False)
    if use_blocking:
        db.blocking.make_engine_blocking()
    if pt.backend.rank()==1:
        _multiprocess_block()
    elif pt.backend.rank()==2:
        _multiprocess_test()

@contextlib.contextmanager
def _suppress_exception_report():
    import tangos.parallel_tasks.backends.multiprocessing as backend
    backend._print_exceptions = False  # to prevent confusing error appearing in stdout
    yield
    backend._print_exceptions = True

def test_non_blocking_exception():
    if testing_db_backend != "sqlite":
        skip("This test is only relevant for sqlite databases")

    with _suppress_exception_report():
        with assert_raises(sqlalchemy.exc.OperationalError):
            with log.LogCapturer():
                pt.launch(_perform_test,3, (False,))

    db.core.get_default_session().rollback()



def test_blocking_avoids_exception():
    if testing_db_backend != "sqlite":
        skip("This test is only relevant for sqlite databases")

    assert tangos.get_halo("sim/ts1/6") is None
    db.core.get_default_session().commit()
    with log.LogCapturer():
        pt.launch(_perform_test,3, (True,))

    assert tangos.get_halo("sim/ts1/6") is not None
