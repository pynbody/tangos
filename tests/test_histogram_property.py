from __future__ import absolute_import
import tangos as db
import tangos.core.simulation
import tangos
import tangos.testing as testing
from tangos import properties
import numpy as np

import numpy.testing as npt


def setup():
    testing.init_blank_db_for_testing()

    generator = testing.TestSimulationGenerator()

    ts1 = generator.add_timestep()
    generator.add_objects_to_timestep(2)
    ts2 = generator.add_timestep()
    generator.add_objects_to_timestep(1)
    generator.link_last_halos_using_mapping({1:1, 2:1})

    _setup_dummy_histogram_data(ts1, ts2)



def _setup_dummy_histogram_data(ts1, ts2):
    global test_histogram
    test_histogram = np.arange(0.0,1000.0,1.0)
    ts1_h1 = db.get_halo("sim/ts1/1")
    ts1_h2 = db.get_halo("sim/ts1/2")
    ts2_h1 = db.get_halo("sim/ts2/1")
    ts1_h1['dummy_histogram'] = test_histogram[DummyHistogramProperty.store_slice(ts1.time_gyr)]
    ts1_h2['dummy_histogram'] = test_histogram[DummyHistogramProperty.store_slice(ts1.time_gyr)] * 0.5
    ts2_h1['dummy_histogram'] = test_histogram[DummyHistogramProperty.store_slice(ts2.time_gyr)]
    db.core.get_default_session().commit()


class DummyHistogramProperty(properties.TimeChunkedProperty):
    names = "dummy_histogram"


def test_default_reconstruction():
    ts2_h1 = db.get_halo("sim/ts2/1")
    reconstructed = db.get_halo("sim/ts2/1")['dummy_histogram']
    assert np.all(reconstructed == test_histogram[:len(reconstructed)])
    assert len(reconstructed)==int(DummyHistogramProperty.nbins*ts2_h1.timestep.time_gyr/DummyHistogramProperty.tmax_Gyr)

def test_summed_reconstruction():
    ts2_h1 = db.get_halo("sim/ts2/1")
    reconstructed = ts2_h1.get_objects("dummy_histogram")[0].get_data_with_reassembly_options('sum')

    manual_reconstruction = ts2_h1['dummy_histogram']
    added_bit = tangos.get_halo("sim/ts1/2")['dummy_histogram']
    manual_reconstruction[:len(added_bit)]+=added_bit

    npt.assert_almost_equal(reconstructed, manual_reconstruction)

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
