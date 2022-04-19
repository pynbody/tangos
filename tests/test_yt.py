import os

import numpy.testing as npt

import tangos
import tangos as db
import tangos.input_handlers.yt as yt_outputs
import tangos.tools.add_simulation as add
from tangos import config, log, testing


def setup_module():
    global output_manager
    testing.init_blank_db_for_testing()
    db.config.base = os.path.join(os.path.dirname(__file__), "test_simulations")
    output_manager = yt_outputs.YtChangaAHFInputHandler("test_tipsy_yt")
    add.SimulationAdderUpdater(output_manager).scan_simulation_and_add_all_descendants()

def teardown_module():
    tangos.core.close_db()

def test_handler():
    assert isinstance(db.get_simulation("test_tipsy_yt").get_output_handler(), yt_outputs.YtChangaAHFInputHandler)

def test_timestep():
    ts = db.get_timestep("test_tipsy_yt/tiny.000640")
    npt.assert_allclose(ts.time_gyr, 2.173594670375)
    npt.assert_allclose(ts.redshift, 2.96382819878)

def test_halos():
    ts = db.get_timestep("test_tipsy_yt/tiny.000640")
    assert ts.halos.count()==9

def test_load():
    yt_obj = db.get_halo("test_tipsy_yt/tiny.000640/halo_2").load()
    import yt.data_objects.data_containers
    assert isinstance(yt_obj, yt.data_objects.data_containers.YTDataContainer)
