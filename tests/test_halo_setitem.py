import halo_db as db
import halo_db.core.halo
import halo_db.core.simulation
import halo_db.core.timestep
import halo_db.testing
import halo_db


def setup():

    db.init_db("sqlite://")

    generator = db.testing.TestDatabaseGenerator()
    for i in range(3):
        generator.add_timestep()
        generator.add_halos_to_timestep(3)



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