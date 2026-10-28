import types

import numpy as np
import numpy.testing as npt

import tangos
import tangos.testing as testing
import tangos.testing.simulation_generator
from tangos.properties import TimeChunkedProperty
from tangos.properties.pynbody.BH import BHAccHistogram

# The black hole's log starts at this time; it is "born" between the two timesteps' storage windows
_log_start_Gyr = 0.5
_bh_iord = 12345


def setup_module():
    testing.init_blank_db_for_testing()
    generator = tangos.testing.simulation_generator.SimulationGeneratorForTests()
    generator.add_timestep() # at 0.9 Gyr
    generator.add_timestep() # at 1.8 Gyr

def teardown_module():
    tangos.core.close_db()


class _StubBlackHole:
    """Just enough of a pynbody star family for BHAccHistogram.calculate: a single black hole"""
    _arrays = {'tform': np.array([-1.0]), 'iord': np.array([_bh_iord])}

    def __len__(self):
        return 1

    def __getitem__(self, name):
        return self._arrays[name]

def _make_particles():
    return types.SimpleNamespace(s=_StubBlackHole())

def _make_calculator():
    calculator = BHAccHistogram(tangos.get_simulation("sim"))
    t_orbit = np.linspace(_log_start_Gyr, 2.0, 1000)
    # an accretion rate equal to the time makes it easy to check each bin was sampled at the right time
    calculator.log = types.SimpleNamespace(vars={'bhid': np.repeat(_bh_iord, len(t_orbit)),
                                                 'time': t_orbit,
                                                 'mdotmean': t_orbit.copy()})
    return calculator

def _calculate_at(timestep_path):
    timestep = tangos.get_timestep(timestep_path)
    calculator = _make_calculator()
    # BHAccHistogram.preloop would also load the orbit log, which _make_calculator stubs
    TimeChunkedProperty.preloop(calculator, None, timestep)
    result, = calculator.calculate(_make_particles(), types.SimpleNamespace(timestep=timestep))
    return result, timestep.time_gyr

def test_bins_are_sampled_at_their_start_times():
    chunk, t_end = _calculate_at("sim/ts2")
    dt = BHAccHistogram.pixel_delta_t_Gyr
    end = int(t_end/dt)
    expected_times = np.arange(end - len(chunk), end) * dt
    npt.assert_allclose(chunk, expected_times, rtol=1e-6)

def test_leading_nans_are_not_stored():
    # ts1 is the first timestep, so its whole history is in range; the log only starts at _log_start_Gyr
    chunk, t_end = _calculate_at("sim/ts1")
    dt = BHAccHistogram.pixel_delta_t_Gyr
    assert not np.isnan(chunk).any()
    assert len(chunk) == int(t_end/dt) - int(np.ceil(_log_start_Gyr/dt))
