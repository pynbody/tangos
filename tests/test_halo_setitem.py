import numpy as np

import tangos
import tangos as db
import tangos.testing
import tangos.testing.simulation_generator


def setup_module():

    tangos.testing.init_blank_db_for_testing()

    generator = tangos.testing.simulation_generator.SimulationGeneratorForTests()
    for i in range(3):
        generator.add_timestep()
        generator.add_objects_to_timestep(3)

    tangos.get_halo("sim/ts1/1")['item_for_update'] = 24

def teardown_module():
    tangos.core.close_db()

def test_setitem():
    tangos.get_halo("sim/ts1/1")['bla'] = 23
    db.core.get_default_session().commit()
    assert tangos.get_halo("sim/ts1/1")['bla'] == 23

def test_set_another_item():
    tangos.get_halo("sim/ts1/2")['bla'] = 42
    db.core.get_default_session().commit()
    assert tangos.get_halo("sim/ts1/2")['bla'] == 42

def test_update_item():
    assert tangos.get_halo("sim/ts1/1")['item_for_update'] == 24
    tangos.get_halo("sim/ts1/1")['item_for_update'] = 96
    db.core.get_default_session().commit()
    assert tangos.get_halo("sim/ts1/1")['item_for_update'] == 96

def test_set_large_item():
    "Test inserting arrays with size up to a few MiB"
    for length in [2**i for i in range(8, 20)]:
        value = np.random.rand(length)

        tangos.get_halo("sim/ts1/2")['this_is_large'] = value
        db.core.get_default_session().commit()

        np.testing.assert_array_equal(
            tangos.get_halo("sim/ts1/2")['this_is_large'],
            value
        )
