import tangos
from tangos import testing

def setup_module():
    testing.init_blank_db_for_testing()
def test_timestep_formatter():
    sim = tangos.core.Simulation('test_sim')
    ts = tangos.core.TimeStep(sim, "test_timestep")
    assert ts.path=="test_sim/test_timestep"