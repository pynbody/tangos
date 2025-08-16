import os
import time

import pytest
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


def run_writer_with_args(*args, parallel=False, allow_resume=False):
    writer = property_writer.PropertyWriter()
    if allow_resume:
        writer.parse_command_line(args)
    else:
        writer.parse_command_line((*args, "--no-resume"))

    def _runner():
        stored_log = log.LogCapturer()
        with stored_log:
            writer.run_calculation_loop()
        return stored_log.get_output()

    if parallel:
        return parallel_tasks.launch(writer.run_calculation_loop, [], {'capture_log': True})
    else:
        return _runner()



def test_basic_writing(fresh_database):
    print(run_writer_with_args("dummy_property"))
    _assert_properties_as_expected()

@pytest.mark.parametrize('load_mode', [None, 'server'])
def test_parallel_writing(fresh_database, load_mode):
    parallel_tasks.use('multiprocessing-3')
    if load_mode is None:
        run_writer_with_args("dummy_property", parallel=True)
    else:
        print(run_writer_with_args("dummy_property", "--load-mode="+load_mode, parallel=True))

    _assert_properties_as_expected()

def test_property_gathering_across_processes(fresh_database):
    parallel_tasks.use('multiprocessing-5')
    results = run_writer_with_args('dummy_property',  parallel=True)
    assert "Succeeded: 15 property calculations" in results

    results = run_writer_with_args("dummy_property", "--load-mode=server-shared-mem", parallel=True)
    assert "Already exists: 15" in results


def test_resuming(fresh_database):
    parallel_tasks.use("multiprocessing-3")
    log = []
    for allow_resume in [False, False, True]:
        log.append(run_writer_with_args("dummy_property", parallel=True, allow_resume=allow_resume))

    for i in [0,1]:
        assert "Resuming from previous run" not in log[i]
        assert len(log[i].split("\n"))>2 # should have done lots of stuff,
        # even if second time it ultimately wrote nothing into the db

    assert len(log[2].split("\n"))==2 # it should resume at the end, and so do nothing other than log a message


def _assert_properties_as_expected():
    assert db.get_halo("dummy_sim_1/step.1/1")['dummy_property'] == 1.0
    assert db.get_halo("dummy_sim_1/step.1/2")['dummy_property'] == 2.0
    assert db.get_halo("dummy_sim_1/step.2/1")['dummy_property'] == 2.0

@pytest.mark.parametrize('parallel', [True, False])
def test_exception_reporting(fresh_database, parallel):
    if parallel:
        parallel_tasks.use('multiprocessing-3')
    log = run_writer_with_args("dummy_property", "dummy_property_with_exception", parallel=parallel)
    print(log)
    assert "Uncaught exception RuntimeError('Test of exception handling') during property calculation" in log
    assert ":     result = property_calculator.calculate(snapshot_data, existing_properties)" in log
    # above tests that a bit of the traceback is present, but also that it has been put on a formatted line

    # count occurrences of the traceback, should be only one:
    assert log.count("Traceback (most recent call last)")==1

    assert "Errored: 15 property calculations" in log

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
        return ["dummy_link.halo_number()"]

    def calculate(self, data, entry):
        if entry['dummy_link.halo_number()'] is not None:
            return entry['dummy_link.halo_number()'],
        else:
            raise ValueError("No link found for halo %s" % entry.id)


def test_link_property(fresh_database):
    print(run_writer_with_args("dummy_link"))
    assert db.get_default_session().query(db.core.HaloLink).count() == 15
    db.testing.assert_halolists_equal([db.get_halo(2)['dummy_link']], [db.get_halo(1)])

def test_link_dependency(fresh_database):
    run_writer_with_args("dummy_property_requiring_link")
    assert db.get_default_session().query(db.core.HaloProperty).count() == 0


    run_writer_with_args("dummy_link")
    print(run_writer_with_args("dummy_property_requiring_link"))
    assert db.get_default_session().query(db.core.HaloProperty).count() == 15

    assert db.get_halo("dummy_sim_1/step.2/1")['dummy_property_requiring_link'] == 1

def test_writer_sees_raw_properties(fresh_database):
    # regression test for issue #121
    # Note that this is desirable to prevent long reconstructions from being run (see issue discussion),
    # but now actually happens as a beneficial side-effect of the way the writer disconnects objects from
    # the session before running calculations.
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


class DummyPropertyTakingTime(DummyProperty):
    names = "dummy_property_taking_time",

    def calculate(self, data, entry):
        time.sleep(0.1)
        return 0.0,


@fixture
def success_tracker():
    from tangos.tools.property_writer import CalculationSuccessTracker
    st = CalculationSuccessTracker()
    for i in range(1):
        st.register_success()
    for i in range(2):
        st.register_error()
    for i in range(3):
        st.register_already_exists()
    for i in range(4):
        st.register_loading_error()
    for i in range(5):
        st.register_missing_prerequisite()
    yield st

def test_calc_success_tracker(success_tracker):
    with log.LogCapturer() as lc:
        success_tracker.report_to_log(log.logger)
    output = lc.get_output()
    assert "Succeeded: 1 property calculations" in output
    assert "Errored: 2 property calculations" in output
    assert "Already exists: 3 property" in output
    assert "Errored during load: 4 property calculations" in output
    assert "Missing pre-requisite: 5 property" in output

def test_log_when_needed(success_tracker):
    with log.LogCapturer() as lc:
        success_tracker.report_to_log_if_needed(log.logger)
    assert len(lc.get_output())>0

    with log.LogCapturer() as lc:
        success_tracker.report_to_log_if_needed(log.logger)
    assert len(lc.get_output())==0

    success_tracker.register_success()

    with log.LogCapturer() as lc:
        success_tracker.report_to_log_if_needed(log.logger)
    assert len(lc.get_output())>0


def test_calc_success_tracker_addition(success_tracker):
    success_tracker.add(success_tracker)
    with log.LogCapturer() as lc:
        success_tracker.report_to_log(log.logger)
    output = lc.get_output()
    assert "Succeeded: 2 property calculations" in output
    assert "Errored: 4 property calculations" in output
    assert "Already exists: 6 property" in output
    assert "Errored during load: 8 property calculations" in output
    assert "Missing pre-requisite: 10 property" in output

def test_writer_reports_aggregates(fresh_database):
    parallel_tasks.use('multiprocessing-4')
    try:
        res = run_writer_with_args("dummy_property_taking_time", parallel=True)
    finally:
        parallel_tasks.use('null')


    assert "Succeeded: 15 property calculations" in res
    assert "myPropertyTakingTime         1.5s" in res

    assert "CUMULATIVE RESPONSE WAIT" in res
    assert "MessageRequestJobResponse" in res # checking the server response time stats are being printed


class MoreThanOneDummyProperty(properties.PropertyCalculation):
    names = "dummy_property_t1", "dummy_property_t2"
    requires_particle_data = True

    def requires_property(self):
        return []

    def calculate(self, data, entry):
        return data.time*data.halo, data.time*data.halo+1

def test_writer_doesnt_duplicate_property_classes(fresh_database):
    res = run_writer_with_args("dummy_property_t1", "dummy_property_t2")
    assert "Succeeded: 15" in res
    assert "Already exists: 0" in res
    assert db.get_halo("dummy_sim_1/step.1/1")['dummy_property_t1'] == 1.0
    assert db.get_halo("dummy_sim_1/step.1/1")['dummy_property_t2'] == 2.0

def test_write_include_only(fresh_database):
    run_writer_with_args("dummy_property")
    run_writer_with_args("another_dummy_property","--include","dummy_property<5")

    dp, _ = db.get_timestep("%/step.1").calculate_all("dummy_property","another_dummy_property")
    # the above only returns halos where another_dummy_property has been written, which should
    # correspond to just those with dummy_property<5
    assert((dp<5).all())
    assert len(dp)==4

    dp, _ = db.get_timestep("%/step.2").calculate_all("dummy_property", "another_dummy_property")
    assert ((dp < 5).all())
    assert len(dp)==2

def test_writer_num_regions_optimization(fresh_database):
    log = run_writer_with_args("dummy_property", "dummy_region_property")

    # there are 10 halos, and dummy_region_property requests a region. dummy_property
    # does not request a region. So the expected number of region queries is 10.
    assert "load_region expected_number_of_queries=10" in log

@pytest.fixture
def db_with_trackers():
    import numpy as np

    from tangos.input_handlers import pynbody
    parallel_tasks.use('null')

    testing.init_blank_db_for_testing()
    db.config.base = os.path.join(os.path.dirname(__file__), "test_simulations")
    manager = add_simulation.SimulationAdderUpdater(pynbody.ChangaInputHandler("test_tipsy"))
    with log.LogCapturer():
        manager.scan_simulation_and_add_all_descendants()

    sim = db.get_simulation("test_tipsy")
    track_id_data = tangos.core.tracking.TrackData(sim)
    track_id_data.halo_number = 2 # not 1, as we want to check the id and halo_number don't get confused
    track_id_data.particle_array = np.array([20000, 40000, 60000])
    track_id_data.use_iord = True

    db.get_default_session().add(track_id_data)
    db.get_default_session().commit()

    track_id_data.create_objects()

class CheckTrackerProperty(properties.pynbody.PynbodyPropertyCalculation):
    """A property that checks that the tracker objects have been created"""
    names = "check_tracker_property"

    def requires_property(self):
        return []

    def calculate(self, data, entry):
        if (data['iord'] == [20000, 40000, 60000]).all():
            return 1
        else:
            return -1


@pytest.mark.parametrize('load_mode', [None, 'partial', 'server', 'server-shared-mem'])
def test_writer_with_trackers(db_with_trackers, load_mode):
    """Test that the writer can handle trackers. Server-partial mode is not supported."""

    load_mode_args = []
    if load_mode is not None:
        load_mode_args = ["--load-mode", load_mode]

    is_parallel = load_mode is not None and load_mode != 'partial'
    if is_parallel:
        parallel_tasks.use('multiprocessing-3')
    print(run_writer_with_args("check_tracker_property", "--type", "tracker", *load_mode_args,
                               parallel = is_parallel,))

    assert db.get_halo("test_tipsy/%640/tracker_2")['check_tracker_property'] == 1
    assert db.get_halo("test_tipsy/%832/tracker_2")['check_tracker_property'] == 1

class DummyPropertyWithNoProxies(properties.PropertyCalculation):
    """A property that accesses a timestep, but does not use proxies."""
    names = "dummy_property_with_no_proxies",

    def calculate(self, data, entry):
        return 1.0,

    @classmethod
    def no_proxies(self):
        return True


def test_proxies_warning(fresh_database):
    """Test that proxies are not used in the writer, and that a warning is issued if they are."""
    output = run_writer_with_args("dummy_property_with_no_proxies")

    print(output)
    # should have issued warning and succeeded
    assert "asks for no proxies, but this is no longer supported" in output
    assert db.get_halo("dummy_sim_1/step.1/1")['dummy_property_with_no_proxies'] == 1.0


class DummyPropertyAccessingLink(properties.PropertyCalculation):
    """A property that accesses a link, but does not use proxies."""
    names = "dummy_property_accessing_link",

    def calculate(self, data, entry):
        dl = entry['link(dummy_link)']
        return dl.id, # should FAIL

    def requires_property(self):
        return ["link(dummy_link)"]

def test_writer_cannot_see_links(fresh_database):
    """Test that the writer cannot see links, and that a warning is issued if it tries."""
    output = run_writer_with_args("dummy_link")
    output = run_writer_with_args("dummy_property_accessing_link")

    assert "Cannot pass database objects" in output
    assert "Missing pre-requisite: 15" in output
    assert "Succeeded: 0" in output
