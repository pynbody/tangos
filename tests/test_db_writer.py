from __future__ import absolute_import
import tangos as db
import tangos.config
import os
from tangos.tools import add_simulation
from tangos.tools import property_writer
from tangos.input_handlers import output_testing
from tangos import parallel_tasks, log, testing
from tangos import properties
from tangos.util import proxy_object

def setup():
    parallel_tasks.use('null')

class DummyProperty(properties.PropertyCalculation):
    names = "dummy_property",
    requires_particle_data = True

    def requires_property(self):
        return []

    def calculate(self, data, entry):
        return data.time*data.halo,


class DummyPropertyCausingException(properties.PropertyCalculation):
    names = "dummy_property_with_exception",
    requires_particle_data = True

    def calculate(self, data, entry):
        raise RuntimeError("Test of exception handling")

def init_blank_simulation():
    testing.init_blank_db_for_testing(timeout=0.0)
    db.config.base = os.path.join(os.path.dirname(__file__), "test_simulations")
    manager = add_simulation.SimulationAdderUpdater(output_testing.TestInputHandler("dummy_sim_1"))
    with log.LogCapturer():
        manager.scan_simulation_and_add_all_descendants()

def run_writer_with_args(*args):
    stored_log = log.LogCapturer()
    writer = property_writer.PropertyWriter()
    writer.parse_command_line(args)
    with stored_log:
        writer.run_calculation_loop()
    return stored_log.get_output()

def test_basic_writing():
    init_blank_simulation()
    run_writer_with_args("dummy_property")
    _assert_properties_as_expected()


def test_parallel_writing():
    init_blank_simulation()
    parallel_tasks.use('multiprocessing')
    try:
        parallel_tasks.launch(run_writer_with_args,3,["dummy_property"])
    finally:
        parallel_tasks.use('null')
    _assert_properties_as_expected()


def _assert_properties_as_expected():
    assert db.get_halo("dummy_sim_1/step.1/1")['dummy_property'] == 1.0
    assert db.get_halo("dummy_sim_1/step.1/2")['dummy_property'] == 2.0
    assert db.get_halo("dummy_sim_1/step.2/1")['dummy_property'] == 2.0

def test_error_ignoring():
    init_blank_simulation()
    log = run_writer_with_args("dummy_property", "dummy_property_with_exception")
    assert "Uncaught exception during property calculation" in log

    assert db.get_halo("dummy_sim_1/step.1/1")['dummy_property'] == 1.0
    assert db.get_halo("dummy_sim_1/step.1/2")['dummy_property'] == 2.0
    assert db.get_halo("dummy_sim_1/step.2/1")['dummy_property'] == 2.0

    assert 'dummy_property' in list(db.get_halo("dummy_sim_1/step.1/1").keys())
    assert 'dummy_property_with_exception' not in list(db.get_halo("dummy_sim_1/step.1/1").keys())


class DummyRegionProperty(properties.PropertyCalculation):
    names = "dummy_region_property",

    def requires_property(self):
        return "dummy_property",

    def region_specification(self, db_data):
        assert 'dummy_property' in db_data
        return slice(1,5)

    def calculate(self, data, entry):
        assert data.message=="Test string"[1:5]
        return 100.0,

def test_region_property():
    init_blank_simulation()
    run_writer_with_args("dummy_property","dummy_region_property")
    _assert_properties_as_expected()
    assert db.get_halo("dummy_sim_1/step.2/1")['dummy_region_property']==100.0

def test_no_duplication():
    init_blank_simulation()
    run_writer_with_args("dummy_property")
    assert db.get_default_session().query(db.core.HaloProperty).count()==15
    run_writer_with_args("dummy_property") # should not create duplicates
    assert db.get_default_session().query(db.core.HaloProperty).count() == 15
    run_writer_with_args("dummy_property", "--force")  # should create duplicates
    assert db.get_default_session().query(db.core.HaloProperty).count() == 30



class DummyLink(DummyProperty):
    names = "dummy_link"

    def calculate(self, data, entry):
        return proxy_object.IncompleteProxyObjectFromFinderId(1,'halo')

class DummyPropertyRequiringLink(DummyProperty):
    names = "dummy_property_requiring_link",

    def requires_property(self):
        return ["dummy_link"]

def test_link_property():
    init_blank_simulation()
    run_writer_with_args("dummy_link")
    assert db.get_default_session().query(db.core.HaloLink).count() == 15
    db.testing.assert_halolists_equal([db.get_halo(2)['dummy_link']], [db.get_halo(1)])

def test_link_dependency():
    init_blank_simulation()
    run_writer_with_args("dummy_property_requiring_link")
    assert db.get_default_session().query(db.core.HaloProperty).count() == 0


    run_writer_with_args("dummy_link")
    run_writer_with_args("dummy_property_requiring_link")
    assert db.get_default_session().query(db.core.HaloProperty).count() == 15