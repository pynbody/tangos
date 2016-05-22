import halo_db as db
import halo_db.config
import os
from halo_db.tools import add_simulation
from halo_db.tools import property_writer
from halo_db.simulation_output_handlers import testing
from halo_db import parallel_tasks
import properties

def setup():
    parallel_tasks.use('null')

class DummyProperty(properties.HaloProperties):
    @classmethod
    def name(self):
        return "dummy_property",

    def requires_property(self):
        return []

    def calculate(self, data, entry):
        return data.time*data.halo,

class DummyPropertyCausingException(properties.HaloProperties):
    @classmethod
    def name(self):
        return "dummy_property_with_exception",

    def calculate(self, data, entry):
        raise RuntimeError, "Test of exception handling"

def init_blank_simulation():
    db.init_db("sqlite://")
    db.config.base = os.path.join(os.path.dirname(__name__), "test_simulations")
    manager = add_simulation.SimulationAdderUpdater(testing.TestOutputSetHandler("dummy_sim_1"))
    manager.scan_simulation_and_add_all_descendants()


def test_basic_writing():
    init_blank_simulation()
    writer = property_writer.PropertyWriter()
    writer.parse_command_line(["dummy_property"])
    writer.run_calculation_loop()

    assert db.get_halo("dummy_sim_1/step.1/1")['dummy_property'] == 1.0
    assert db.get_halo("dummy_sim_1/step.1/2")['dummy_property'] == 2.0
    assert db.get_halo("dummy_sim_1/step.2/1")['dummy_property'] == 2.0

def test_error_ignoring():
    init_blank_simulation()
    writer = property_writer.PropertyWriter()
    writer.parse_command_line(["dummy_property", "dummy_property_with_exception"])
    writer.run_calculation_loop()

    assert db.get_halo("dummy_sim_1/step.1/1")['dummy_property'] == 1.0
    assert db.get_halo("dummy_sim_1/step.1/2")['dummy_property'] == 2.0
    assert db.get_halo("dummy_sim_1/step.2/1")['dummy_property'] == 2.0