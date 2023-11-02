import os

from numpy import testing as npt
from pytest import fixture

import tangos as db
import tangos.config
from tangos import log, parallel_tasks, properties, testing
from tangos.input_handlers import output_testing
from tangos.tools import add_simulation, property_writer
from tangos.util import proxy_object


def setup_func(sim="dummy_sim_1"):
    parallel_tasks.use('null')

    testing.init_blank_db_for_testing()
    db.config.base = os.path.join(os.path.dirname(__file__), "test_simulations")
    manager = add_simulation.SimulationAdderUpdater(output_testing.TestInputHandler(sim))
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
def fresh_database_2():
    setup_func("dummy_sim_2")
    yield
    teardown_func()

class DummyProperty(properties.PropertyCalculation):
    names = "dummy_property",
    requires_particle_data = True

    def requires_property(self):
        return []

    def calculate(self, data, entry):
        return data.time*data.halo,

class DummyProperty2(properties.PropertyCalculation):
    """Used by test_property_deleter"""
    names = "another_dummy_property",
    requires_particle_data = True

    def requires_property(self):
        return []

    def calculate(self, data, entry):
        return data.time*data.halo+1,

class DummyPropertyCausingException(properties.PropertyCalculation):
    names = "dummy_property_with_exception",
    requires_particle_data = True

    def calculate(self, data, entry):
        raise RuntimeError("Test of exception handling")

class DummyPropertyWithReconstruction(properties.PropertyCalculation):
    names = "dummy_property_with_reconstruction",
    requries_particle_data = False
    callback = None

    def calculate(self, data, entry):
        return 1.0,

    def reassemble(self, property_name):
        if self.callback:
            self.callback() # hook to allow us to know reassemble has been called
        return 2.0

class DummyPropertyAccessingSimulationProperty(properties.PropertyCalculation):
    names = "dummy_property_accessing_simulation_property",
    requires_particle_data = False

    def preloop(self, sim_data, db_timestep):
        self._num_queries = 0

    def calculate(self, data, entry):
        with tangos.testing.SqlExecutionTracker() as ctr:
            result = self.get_simulation_property("dummy_sim_property", None)
        assert result == '42'
        self._num_queries += ctr.count_statements_containing("simulationproperties")
        assert self._num_queries<=1 # don't want to see simulationproperties queried more than once

        check_null_result = object()
        result2 = self.get_simulation_property("nonexistent_sim_property",check_null_result)
        assert result2 is check_null_result

        # store the value 1 to indicate that everything above passed (assertion errors will be
        # caught by the db_writer so wouldn't directly result in a failure)
        return 1,


def run_writer_with_args(*args, parallel=False):
    writer = property_writer.PropertyWriter()
    writer.parse_command_line(args)

    def _runner():
        stored_log = log.LogCapturer()
        with stored_log:
            writer.run_calculation_loop()
        return stored_log.get_output()

    if parallel:
        parallel_tasks.launch(_runner, [])
    else:
        return _runner()



def test_basic_writing(fresh_database):
    run_writer_with_args("dummy_property")
    _assert_properties_as_expected()


def test_parallel_writing(fresh_database):
    parallel_tasks.use('multiprocessing-2')
    run_writer_with_args("dummy_property", parallel=True)

    _assert_properties_as_expected()


def _assert_properties_as_expected():
    assert db.get_halo("dummy_sim_1/step.1/1")['dummy_property'] == 1.0
    assert db.get_halo("dummy_sim_1/step.1/2")['dummy_property'] == 2.0
    assert db.get_halo("dummy_sim_1/step.2/1")['dummy_property'] == 2.0

def test_error_ignoring(fresh_database):
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

def test_region_property(fresh_database):
    run_writer_with_args("dummy_property","dummy_region_property")
    _assert_properties_as_expected()
    assert db.get_halo("dummy_sim_1/step.2/1")['dummy_region_property']==100.0

def test_no_duplication(fresh_database):
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

def test_link_property(fresh_database):
    run_writer_with_args("dummy_link")
    assert db.get_default_session().query(db.core.HaloLink).count() == 15
    db.testing.assert_halolists_equal([db.get_halo(2)['dummy_link']], [db.get_halo(1)])

def test_link_dependency(fresh_database):
    run_writer_with_args("dummy_property_requiring_link")
    assert db.get_default_session().query(db.core.HaloProperty).count() == 0


    run_writer_with_args("dummy_link")
    run_writer_with_args("dummy_property_requiring_link")
    assert db.get_default_session().query(db.core.HaloProperty).count() == 15

def test_writer_sees_raw_properties(fresh_database):
    # regression test for issue #121
    run_writer_with_args("dummy_property_with_reconstruction")
    assert db.get_halo(2)['dummy_property_with_reconstruction']==2.0
    assert db.get_halo(2).calculate('raw(dummy_property_with_reconstruction)')==1.0

    def raise_exception(obj):
        raise RuntimeError("reconstruct has been called")

    DummyPropertyWithReconstruction.callback = raise_exception
    run_writer_with_args("dummy_property_with_reconstruction") # should not try to reconstruct the existing data stream

def test_writer_handles_sim_properties(fresh_database):
    """Test for issue where simulation properties could be queried from within a calculation.

    This could lead to unexpected database locks. Tangos 1.3 provides a safe route to doing this.
    The test ensures that the results are cached to prevent hammering the database

    However it does not directly test that the parallel_tasks locking mechanism is called,
    which is hard. Ideally this test would therefore be completed at some point..."""

    parallel_tasks.use('multiprocessing-3')
    try:
        parallel_tasks.launch(run_writer_with_args,  ["dummy_property_accessing_simulation_property"])
    finally:
        parallel_tasks.use('null')

    for i in range(1,3):
        ts = db.get_timestep("dummy_sim_1/step.%d"%i)
        x, = ts.calculate_all("dummy_property_accessing_simulation_property")
        npt.assert_equal(x,[1]*ts.halos.count())

def test_timesteps_matching(fresh_database_2):
    run_writer_with_args("dummy_property", "--timesteps-matching", "step.1", "--timesteps-matching", "step.2")
    assert 'dummy_property' in  db.get_halo("dummy_sim_2/step.1/1").keys()
    assert 'dummy_property' in db.get_halo("dummy_sim_2/step.1/2").keys()
    assert 'dummy_property' in db.get_halo("dummy_sim_2/step.2/1").keys()
    assert 'dummy_property' not in db.get_halo("dummy_sim_2/step.3/1").keys()
