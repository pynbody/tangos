from sqlalchemy.orm import lazyload

import tangos
from tangos import testing


def setup_module():
    testing.init_blank_db_for_testing()

def test_timestep_formatter():
    sim = tangos.core.Simulation('test_sim')
    ts = tangos.core.TimeStep(sim, "test_timestep")
    assert ts.path=="test_sim/test_timestep"

    tangos.core.get_default_session().add_all([sim, ts])
    tangos.core.get_default_session().commit()

    ts = (tangos.core.get_default_session().
          query(tangos.core.TimeStep).
          options(lazyload(tangos.core.TimeStep.simulation)).first())

    tangos.core.close_db()

    assert ts.path=="<detached>"
