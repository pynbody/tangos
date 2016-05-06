import halo_db as db
import halo_db.core.halo
import halo_db.core.simulation
import halo_db.core.timestep
import halo_db.crosslink
import halo_db


def setup():

    db.init_db("sqlite://")

    session = db.core.get_default_session()

    sim = halo_db.core.simulation.Simulation("sim")

    session.add(sim)

    ts1 = halo_db.core.timestep.TimeStep(sim, "ts1", False)
    ts2 = halo_db.core.timestep.TimeStep(sim, "ts2", False)
    ts3 = halo_db.core.timestep.TimeStep(sim, "ts3", False)

    for ts in ts1,ts2,ts3:
        session.add(ts)
        h1 = halo_db.core.halo.Halo(ts, 1, 1000, 0, 0, 0)
        h2 = halo_db.core.halo.Halo(ts, 2, 1000, 0, 0, 0)
        h3 = halo_db.core.halo.Halo(ts, 3, 1000, 0, 0, 0)
        session.add_all((h1,h2,h3))

def test_setitem():
    halo_db.get_halo("sim/ts1/1")['bla'] = 23
    db.core.get_default_session().commit()
    assert halo_db.get_halo("sim/ts1/1")['bla'] == 23

def test_set_another_item():
    halo_db.get_halo("sim/ts1/2")['bla'] = 42
    db.core.get_default_session().commit()
    assert halo_db.get_halo("sim/ts1/2")['bla'] == 42

def test_update_item():
    assert halo_db.get_halo("sim/ts1/1")['bla'] == 23
    halo_db.get_halo("sim/ts1/1")['bla'] = 96
    db.core.get_default_session().commit()
    assert halo_db.get_halo("sim/ts1/1")['bla'] == 96