import numpy as np
import numpy.testing as npt

import tangos
import tangos as db
import tangos.core.simulation
import tangos.testing as testing
import tangos.testing.simulation_generator
from tangos import log, properties
from tangos.tools import property_writer


def setup_module():
    testing.init_blank_db_for_testing()

    generator = tangos.testing.simulation_generator.SimulationGeneratorForTests()

    ts1 = generator.add_timestep()
    generator.add_objects_to_timestep(2)
    ts2 = generator.add_timestep()
    generator.add_objects_to_timestep(1)
    generator.link_last_halos_using_mapping({1:1, 2:1})

    _setup_dummy_histogram_data(ts1, ts2)

def teardown_module():
    tangos.core.close_db()


def _setup_dummy_histogram_data(ts1, ts2):
    global test_histogram
    test_histogram = np.arange(0.0,1000.0,1.0)

    stored_log = log.LogCapturer()
    writer = property_writer.PropertyWriter()
    writer.parse_command_line(["dummy_histogram"])
    with stored_log:
        writer.run_calculation_loop()


class DummyHistogramProperty(properties.TimeChunkedProperty):
    minimum_store_Gyr = 1.0
    names = "dummy_histogram"

    def preloop(self, _, timestep):
        self.time = timestep.time_gyr

    def calculate(self, halo, existing_properties):
        global test_histogram
        hist = test_histogram[self.store_slice(self.time)]/existing_properties.halo_number
        return hist

def test_histogram_written_as_expected():
    dumhistprop = DummyHistogramProperty(db.get_simulation("sim"))
    ts1 = db.get_timestep("sim/ts1")
    ts2 = db.get_timestep("sim/ts2")
    ts1_h1 = db.get_halo("sim/ts1/1")
    ts1_h2 = db.get_halo("sim/ts1/2")
    ts2_h1 = db.get_halo("sim/ts2/1")
    npt.assert_almost_equal(ts1_h1.calculate('raw(dummy_histogram)'),
                            test_histogram[dumhistprop.store_slice(ts1.time_gyr)])
    npt.assert_almost_equal(ts1_h2.calculate('raw(dummy_histogram)'),
                            test_histogram[dumhistprop.store_slice(ts1.time_gyr)] * 0.5)
    npt.assert_almost_equal(ts2_h1.calculate('raw(dummy_histogram)'),
                            test_histogram[dumhistprop.store_slice(ts2.time_gyr)])

def test_default_reconstruction():
    ts2_h1 = db.get_halo("sim/ts2/1")
    reconstructed = db.get_halo("sim/ts2/1")['dummy_histogram']
    assert np.all(reconstructed == test_histogram[:len(reconstructed)])
    assert len(reconstructed)==int(ts2_h1.timestep.time_gyr/DummyHistogramProperty.pixel_delta_t_Gyr)

def test_summed_reconstruction():
    ts2_h1 = db.get_halo("sim/ts2/1")
    reconstructed = ts2_h1.get_objects("dummy_histogram")[0].get_data_with_reassembly_options('sum')

    manual_reconstruction = ts2_h1['dummy_histogram']
    added_bit = tangos.get_halo("sim/ts1/2")['dummy_histogram']
    manual_reconstruction[:len(added_bit)]+=added_bit

    npt.assert_almost_equal(reconstructed, manual_reconstruction)

def test_reconstruction_optimized():
    # check that no temporary tables are created during reassembly of a histogram property
    ts2_h1 = db.get_halo("sim/ts2/1")
    hist_obj = ts2_h1.get_objects("dummy_histogram")[0]

    with testing.SqlExecutionTracker(db.core.get_default_engine()) as track:
        hist_obj.get_data_with_reassembly_options('sum')

    # print out any tracebacks for select haloproperties, for debug help
    for s in track.traceback_statements_containing("select haloproperties"):
        print ("traceback for select haloproperties:")
        print(s)

    # current algorithm constructs 2 temp tables for merger tree probe, plus one for final gathering of properties
    assert track.count_statements_containing("create temporary table") <= 3

    # joined load should prevent separate selects being emitted
    assert "select haloproperties" not in track


def test_live_calculation_summed_reconstruction():
    ts2_h1 = db.get_halo("sim/ts2/1")
    reconstructed = ts2_h1.get_objects("dummy_histogram")[0].get_data_with_reassembly_options('sum')
    reconstructed_lc = ts2_h1.calculate("reassemble(dummy_histogram, 'sum')")
    npt.assert_almost_equal(reconstructed, reconstructed_lc)

def test_placed_reconstruction():
    ts2_h1 = db.get_halo("sim/ts2/1")
    reconstructed = ts2_h1.get_objects("dummy_histogram")[0].get_data_with_reassembly_options('place')
    manual_reconstruction = np.zeros(len(ts2_h1['dummy_histogram']))
    raw_data = ts2_h1.get_data('dummy_histogram',raw=True)
    manual_reconstruction[-len(raw_data):] = raw_data
    return npt.assert_almost_equal(reconstructed, manual_reconstruction)

def test_summed_reconstruction_across_simulations():

    ts2_h1 = db.get_halo("sim/ts2/1")
    session = db.core.get_default_session()
    sim2 = tangos.core.simulation.Simulation("sim2")
    ts2 = db.query.get_timestep("sim/ts2")
    session.add(sim2)

    try:
        # assign timestep 2 to another simulation
        ts2.simulation = sim2
        session.commit()


        # in default construction mode, the earlier branch will not be found
        manual_reconstruction = ts2_h1.get_objects("dummy_histogram")[0].get_data_with_reassembly_options('place')
        reconstructed = ts2_h1.get_objects("dummy_histogram")[0].get_data_with_reassembly_options('major')
        npt.assert_almost_equal(reconstructed, manual_reconstruction)


        # when reassmebling across simulations, the timestep should be found
        reconstructed = ts2_h1.get_objects("dummy_histogram")[0].get_data_with_reassembly_options('major_across_simulations')

        manual_reconstruction = ts2_h1['dummy_histogram']
        added_bit = tangos.get_halo("sim/ts1/1")['dummy_histogram']
        manual_reconstruction[:len(added_bit)]=added_bit

        npt.assert_almost_equal(reconstructed, manual_reconstruction)



    finally:
        ts2.simulation = db.query.get_simulation("sim")
        session.commit()


def test_custom_delta_t():
    try:
        db.get_simulation("sim")["histogram_delta_t_Gyr"] = 0.01
        reconstructed_lc = db.get_halo("sim/ts2/1").calculate("reassemble(dummy_histogram, 'place')")
        assert len(reconstructed_lc) == int(db.get_timestep("sim/ts2").time_gyr/0.01)
    finally:
        # restore to default
        db.get_simulation("sim")["histogram_delta_t_Gyr"] = DummyHistogramProperty.pixel_delta_t_Gyr
