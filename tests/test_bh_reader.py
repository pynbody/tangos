import numpy as np
import pynbody

import tangos as db
from tangos.input_handlers.changa_bh import BlackHolesLog, ShortenedOrbitLog

_sim_path = 'test_simulations/test_tipsy/tiny.000640'

def test_bhlog():
	sim = pynbody.load(_sim_path)
	mass_unit_conv = sim.infer_original_units('Msol').in_units('Msol')
	mdot_unit_conv = sim.infer_original_units('Msol yr**-1').in_units('Msol yr**-1')
	vunit_conv = sim.infer_original_units('km s**-1').in_units('km s**-1', a=0.666)
	posunit_conv = sim.infer_original_units('kpc').in_units('kpc', a=0.666)

	assert(BlackHolesLog.can_load(_sim_path))
	bhlog = BlackHolesLog(_sim_path)
	assert(np.abs(bhlog.get_at_stepnum_for_id(2.0, 12345)['mass']/ mass_unit_conv - 400) < 1e-6)
	assert(np.abs(bhlog.get_at_stepnum_for_id(1.0, 12345)['mass'] / mass_unit_conv - 200) < 1e-6)
	assert(np.abs(bhlog.get_at_stepnum_for_id(1.0, 12345)['mdot'] / mdot_unit_conv - 100) < 1e-6)
	assert(np.abs(bhlog.get_at_stepnum_for_id(2.0, 12346)['mdot'] / mdot_unit_conv - 25) < 1e-6)

	assert(np.abs(bhlog.get_at_stepnum_for_id(2.0, 12345)['mdotmean'] / mdot_unit_conv - 200) < 1e-6)
	assert(np.abs(bhlog.get_at_stepnum_for_id(1.0, 12346)['mdotmean'] / mdot_unit_conv - 50) < 1e-6)

	assert(np.abs(bhlog.get_at_stepnum_for_id(1.0, 12345)['x'] / posunit_conv - 1.0) < 1e-6)
	assert(np.abs(bhlog.get_at_stepnum_for_id(1.0, 12345)['vx'] / vunit_conv - 1.0) < 1e-6)

	assert(bhlog.get_last_entry_for_id(12345)['step'] == 2.0)
	assert(bhlog.get_last_entry_for_id(12346)['step'] == 2.0)
