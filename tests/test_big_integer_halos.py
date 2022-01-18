from tangos.core import Halo
from tangos import testing
import tangos as db
import tangos.testing.simulation_generator
import numpy as np

def setup():
    testing.init_blank_db_for_testing()
    creator = testing.simulation_generator.TestSimulationGenerator()
    creator.add_timestep()

def teardown():
    tangos.core.close_db()

def test_big_integer_halo():
    crazy_number = np.iinfo(np.dtype('uint64')).max - 10 # close to maximum but not the actual max, avoiding special case

    h  = Halo(db.get_timestep("sim/ts1"),1,crazy_number,1,0,0,0)
    session = db.core.get_default_session()

    session.add(h)
    session.commit()

    h = db.get_halo("sim/ts1/1")

    assert h.finder_id == crazy_number

