import halo_db as db
import halo_db.core.halo
import halo_db.core.simulation
import halo_db.core.timestep
import halo_db.crosslink

def setup():

    db.init_db("sqlite://")

    session = db.core.internal_session

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

def test_needs_crosslink():
    ts1 = db.get_timestep("sim/ts1")
    ts2 = db.get_timestep("sim/ts2")
    ts3 = db.get_timestep("sim/ts3")

    ts1.halos[0]["ptcls_in_common"] = ts2.halos[0]

    assert not db.crosslink.need_crosslink_ts(ts1,ts2)
    assert db.crosslink.need_crosslink_ts(ts2,ts1)
    assert db.crosslink.need_crosslink_ts(ts2,ts3)
    assert db.crosslink.need_crosslink_ts(ts1,ts3)
