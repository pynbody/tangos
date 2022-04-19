import copy
import os

import numpy as np
import numpy.testing as npt

import tangos
import tangos as db
import tangos.input_handlers.pynbody as pynbody_outputs
import tangos.tools.add_simulation as add
from tangos import config, log, testing, tracking


def setup_module():
    global output_manager, iord_expected_s960, iord_expected_s832
    testing.init_blank_db_for_testing()
    db.config.base = os.path.join(os.path.dirname(__file__), "test_simulations")
    output_manager = pynbody_outputs.ChangaInputHandler("test_tipsy")
    with log.LogCapturer():
        add.SimulationAdderUpdater(output_manager).scan_simulation_and_add_all_descendants()

    h1 = db.get_halo("test_tipsy/%640/1").load()
    h1_sub = h1[::5]

    tracking.new("test_tipsy", h1_sub)
    iord_expected_s960 = copy.deepcopy(h1_sub['iord'])

    # because the tiny test files are downsampled, only a small number of particles are actually
    # in common between the two steps
    iord_expected_s832 = np.intersect1d(iord_expected_s960, db.get_timestep("test_tipsy/%832").load()['iord'])

def teardown_module():
    tangos.core.close_db()


def test_get_tracker():
    global iord_expected_s832, iord_expected_s960
    t640 = db.get_halo("test_tipsy/%640/tracker_1")
    npt.assert_equal(t640.load()['iord'], iord_expected_s960)
    t832 = db.get_halo("test_tipsy/%832/tracker_1")
    npt.assert_equal(t832.load()['iord'], iord_expected_s832)


def test_tracker_linkage():
    t640 = db.get_halo("test_tipsy/%640/tracker_1")
    t832 = db.get_halo("test_tipsy/%832/tracker_1")

    assert t640.next==t832
    assert t832.previous==t640
