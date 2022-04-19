import os
import os.path

import test_db_writer  # for dummy_property
from pytest import fixture

import tangos as db
from tangos import log, parallel_tasks, testing
from tangos.input_handlers import output_testing
from tangos.tools import add_simulation, property_deleter, property_writer


def setup_func():
    parallel_tasks.use('null')
    testing.init_blank_db_for_testing()
    db.config.base = os.path.join(os.path.dirname(__file__), "test_simulations")
    manager = add_simulation.SimulationAdderUpdater(output_testing.TestInputHandler("dummy_sim_1"))
    manager2 = add_simulation.SimulationAdderUpdater(output_testing.TestInputHandler("dummy_sim_2"))
    with log.LogCapturer():
        manager.scan_simulation_and_add_all_descendants()
        manager2.scan_simulation_and_add_all_descendants()
        writer = property_writer.PropertyWriter()
        writer.parse_command_line(['dummy_property'])
        writer.run_calculation_loop()

def teardown_func():
    db.core.close_db()

@fixture
def fresh_database():
    setup_func()
    yield
    teardown_func()

def test_delete_property_one_halo(fresh_database):
   assert 'dummy_property' in db.get_halo("dummy_sim_1/step.1/halo_1")

   tool = property_deleter.PropertyDeleter()
   tool.parse_command_line("dummy_property --for dummy_sim_1/step.1/halo_1 -f".split())
   tool.run_calculation_loop()

   # check deleted:
   assert 'dummy_property' not in db.get_halo("dummy_sim_1/step.1/halo_1")

   # check hasn't affected other halos, timesteps or simulations:
   assert 'dummy_property' in db.get_halo("dummy_sim_1/step.2/halo_1")
   assert 'dummy_property' in db.get_halo("dummy_sim_1/step.1/halo_2")
   assert 'dummy_property'  in db.get_halo("dummy_sim_2/step.1/halo_1")

def test_delete_property_one_timestep(fresh_database):
   assert 'dummy_property' in db.get_halo("dummy_sim_1/step.1/halo_1")

   tool = property_deleter.PropertyDeleter()
   tool.parse_command_line("dummy_property --for dummy_sim_1/step.1 -f".split())
   tool.run_calculation_loop()

   # check deleted:
   assert 'dummy_property' not in db.get_halo("dummy_sim_1/step.1/halo_1")
   assert 'dummy_property' not in db.get_halo("dummy_sim_1/step.1/halo_2")

   # hasn't affected other steps or simulations:
   assert 'dummy_property' in db.get_halo("dummy_sim_1/step.2/halo_1")
   assert 'dummy_property'  in db.get_halo("dummy_sim_2/step.1/halo_1")


def test_delete_property_one_simulation(fresh_database):
    assert 'dummy_property' in db.get_halo("dummy_sim_1/step.1/halo_1")

    tool = property_deleter.PropertyDeleter()
    tool.parse_command_line("dummy_property --for dummy_sim_1 -f".split())
    tool.run_calculation_loop()

    # check deleted:
    assert 'dummy_property' not in db.get_halo("dummy_sim_1/step.1/halo_1")
    assert 'dummy_property' not in db.get_halo("dummy_sim_1/step.1/halo_2")
    assert 'dummy_property' not in db.get_halo("dummy_sim_1/step.2/halo_1")

    # hasn't affected other simulations:
    assert 'dummy_property' in db.get_halo("dummy_sim_2/step.1/halo_1")

def test_delete_property_entire_db(fresh_database):
    assert 'dummy_property' in db.get_halo("dummy_sim_1/step.1/halo_1")

    tool = property_deleter.PropertyDeleter()
    tool.parse_command_line("dummy_property -f".split())
    tool.run_calculation_loop()

    s = db.core.get_default_session()
    assert s.query(db.core.HaloProperty).count() == 0

def test_delete_one_of_two_properties_whole_db(fresh_database):
    writer = property_writer.PropertyWriter()
    writer.parse_command_line(['another_dummy_property'])
    writer.run_calculation_loop()

    assert 'dummy_property' in db.get_halo("dummy_sim_1/step.1/halo_1")
    assert 'another_dummy_property' in db.get_halo("dummy_sim_1/step.1/halo_1")

    tool = property_deleter.PropertyDeleter()
    tool.parse_command_line("another_dummy_property -f".split())
    tool.run_calculation_loop()

    assert 'dummy_property' in db.get_halo("dummy_sim_1/step.1/halo_1")
    assert 'another_dummy_property' not in db.get_halo("dummy_sim_1/step.1/halo_1")
