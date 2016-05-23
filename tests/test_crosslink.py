import halo_db as db
import halo_db.core.halo
import halo_db.core.simulation
import halo_db.core.timestep
import halo_db.crosslink
import halo_db, halo_db.testing


def setup():

    db.init_db("sqlite://")

    generator = db.testing.TestSimulationGenerator()

    for i in range(3):
        generator.add_timestep()
        generator.add_halos_to_timestep(3)


def test_needs_crosslink():
    ts1 = halo_db.get_timestep("sim/ts1")
    ts2 = halo_db.get_timestep("sim/ts2")
    ts3 = halo_db.get_timestep("sim/ts3")

    ts1.halos[0]["ptcls_in_common"] = ts2.halos[0]

    assert not db.crosslink.need_crosslink_ts(ts1,ts2)
    assert db.crosslink.need_crosslink_ts(ts2,ts1)
    assert db.crosslink.need_crosslink_ts(ts2,ts3)
    assert db.crosslink.need_crosslink_ts(ts1,ts3)
