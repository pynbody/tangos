import numpy as np

import tangos as db
import tangos.testing.simulation_generator
from tangos import testing
from tangos.core.halo import Halo


def setup_module():
    testing.init_blank_db_for_testing()
    creator = testing.simulation_generator.SimulationGeneratorForTests()
    creator.add_timestep()

def teardown_module():
    tangos.core.close_db()

def test_big_integer_halo():
    crazy_number = np.iinfo(np.dtype('int64')).max - 10 # close to maximum but not the actual max, avoiding special case

    h  = Halo(db.get_timestep("sim/ts1"),1,crazy_number,1,0,0,0)
    session = db.core.get_default_session()

    session.add(h)
    session.commit()

    h = db.get_halo("sim/ts1/1")

    assert h.finder_id == crazy_number
