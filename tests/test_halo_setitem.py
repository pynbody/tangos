from __future__ import absolute_import
import tangos as db
import tangos.testing
import tangos
from six.moves import range

import tangos.testing.simulation_generator


def setup():

    tangos.testing.init_blank_db_for_testing()

    generator = tangos.testing.simulation_generator.TestSimulationGenerator()
    for i in range(3):
        generator.add_timestep()
        generator.add_objects_to_timestep(3)



def test_setitem():
    tangos.get_halo("sim/ts1/1")['bla'] = 23
    db.core.get_default_session().commit()
    assert tangos.get_halo("sim/ts1/1")['bla'] == 23

def test_set_another_item():
    tangos.get_halo("sim/ts1/2")['bla'] = 42
    db.core.get_default_session().commit()
    assert tangos.get_halo("sim/ts1/2")['bla'] == 42

def test_update_item():
    assert tangos.get_halo("sim/ts1/1")['bla'] == 23
    tangos.get_halo("sim/ts1/1")['bla'] = 96
    db.core.get_default_session().commit()
    assert tangos.get_halo("sim/ts1/1")['bla'] == 96