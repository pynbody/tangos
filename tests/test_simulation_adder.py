from __future__ import absolute_import
import tangos as db
from tangos import log
import os
from tangos.tools import add_simulation
from tangos.input_handlers import output_testing
from tangos import testing
from pytest import fixture

def setup_func(add=True):
    testing.init_blank_db_for_testing()
    db.config.base = os.path.join(os.path.dirname(__file__), "test_simulations")
    if add:
        manager = add_simulation.SimulationAdderUpdater(output_testing.TestInputHandler("dummy_sim_1"))
        with log.LogCapturer():
            manager.scan_simulation_and_add_all_descendants()

def teardown_func():
    db.core.close_db()

@fixture
def fresh_database():
    setup_func()
    yield
    teardown_func()

@fixture
def fresh_database_no_contents():
    setup_func(add=False)
    yield
    teardown_func()

def test_simulation_exists(fresh_database):
    manager = add_simulation.SimulationAdderUpdater(output_testing.TestInputHandler("dummy_sim_2"))
    assert not manager.simulation_exists()

    manager = add_simulation.SimulationAdderUpdater(output_testing.TestInputHandler("dummy_sim_1"))
    assert manager.simulation_exists()

def test_step_halo_count(fresh_database):
    assert db.get_timestep("dummy_sim_1/step.1").halos.count()==10
    assert db.get_timestep("dummy_sim_1/step.2").halos.count()==5

def test_step_info(fresh_database):
    assert db.get_timestep("dummy_sim_1/step.1").time_gyr==1
    assert db.get_timestep("dummy_sim_1/step.1").redshift==0.5

def test_simulation_properties(fresh_database):
    assert db.get_simulation("dummy_sim_1").properties.count()==3
    assert db.get_simulation("dummy_sim_1")['dummy_sim_property']=='42'

def test_readd_simulation(fresh_database):
    manager = add_simulation.SimulationAdderUpdater(output_testing.TestInputHandler("dummy_sim_1"))
    with log.LogCapturer():
        manager.scan_simulation_and_add_all_descendants()

    assert db.core.get_default_session().query(db.core.Simulation).count()==1
    assert len(db.get_simulation("dummy_sim_1").timesteps)==2
    assert db.get_simulation("dummy_sim_1").properties.count()==3

def test_appropriate_loader(fresh_database):
    assert str(db.get_timestep("dummy_sim_1/step.1").load())=="Test string - this would contain the data for step.1"

def _perform_simulation_update():
    try:
        old_base = db.config.base
        db.config.base = os.path.join(os.path.dirname(__file__), "test_simulations_mock_update")
        manager = add_simulation.SimulationAdderUpdater(output_testing.TestInputHandler("dummy_sim_1"))
        with log.LogCapturer():
            manager.scan_simulation_and_add_all_descendants()
    finally:
        db.config.base = old_base

def test_update_simulation(fresh_database):
    with testing.autorevert():
        assert db.get_simulation("dummy_sim_1")['dummy_sim_property_2'] == 'banana'
        _perform_simulation_update()
        assert db.get_simulation("dummy_sim_1").properties.count() == 4
        assert db.get_simulation("dummy_sim_1")['dummy_sim_property_2']=='orange'
        assert db.get_simulation("dummy_sim_1")['dummy_sim_property_new'] == 'fruits'
        assert len(db.get_simulation("dummy_sim_1").timesteps) == 3
        assert db.get_timestep("dummy_sim_1/step.3").halos.count()==7
    assert db.get_simulation("dummy_sim_1")['dummy_sim_property_2'] == 'banana'

def test_renumbering(fresh_database_no_contents):
    manager = add_simulation.SimulationAdderUpdater(output_testing.TestInputHandlerReverseHaloNDM("dummy_sim_2"))
    assert not manager.simulation_exists()
    with log.LogCapturer():
        manager.scan_simulation_and_add_all_descendants()

    assert db.get_halo("dummy_sim_2/step.1/halo_2").halo_number==2
    assert db.get_halo("dummy_sim_2/step.1/halo_2").finder_id == 7
    assert db.get_halo("dummy_sim_2/step.1/halo_2").NDM==2006

def test_renumbering_disabled(fresh_database_no_contents):
    manager = add_simulation.SimulationAdderUpdater(output_testing.TestInputHandlerReverseHaloNDM("dummy_sim_2"),
                                                    renumber=False)
    assert not manager.simulation_exists()

    with log.LogCapturer():
        manager.scan_simulation_and_add_all_descendants()

    assert db.get_halo("dummy_sim_2/step.1/halo_2").halo_number==2
    assert db.get_halo("dummy_sim_2/step.1/halo_2").finder_id == 2
    assert db.get_halo("dummy_sim_2/step.1/halo_2").NDM==2001

def test_limited_numbers(fresh_database_no_contents):
    manager = add_simulation.SimulationAdderUpdater(output_testing.TestInputHandlerReverseHaloNDM("dummy_sim_2"))
    manager.max_num_objects = 3
    manager.scan_simulation_and_add_all_descendants()
    assert db.get_timestep("dummy_sim_2/step.1").halos.count()==3

def test_NDM_cut(fresh_database_no_contents):
    manager = add_simulation.SimulationAdderUpdater(output_testing.TestInputHandlerReverseHaloNDM("dummy_sim_2"))
    manager.min_halo_particles = 2005
    manager.scan_simulation_and_add_all_descendants()
    ndm, = db.get_timestep("dummy_sim_2/step.1").calculate_all("NDM()")
    assert ndm.min()==2005
