import halo_db as db
import halo_db.core.halo
import halo_db.core.simulation
import halo_db.core.timestep
import halo_db
import halo_db.testing as testing
import properties
import numpy as np

import numpy.testing as npt


test_histogram = np.arange(0.0,1000.0,1.0)

def setup():
    global test_histogram, ts2_h1

    db.init_db("sqlite://")
    session = db.core.get_default_session()

    sim = halo_db.core.simulation.Simulation("sim")

    session.add(sim)

    ts1 = halo_db.core.timestep.TimeStep(sim, "ts1", False)


    session.add(ts1)

    ts1.time_gyr = 0.9
    ts1.redshift = 10


    ts2 = halo_db.core.timestep.TimeStep(sim, "ts2", False)

    session.add(ts2)

    ts2.time_gyr = 1.8
    ts2.redshift = 9



    ts1_h1 = halo_db.core.halo.Halo(ts1, 1, 1000, 0, 0, 0)
    ts1_h2 = halo_db.core.halo.Halo(ts1, 2, 900, 0, 0, 0)

    # both halos at ts1 merge into one halo at ts2

    ts2_h1 = halo_db.core.halo.Halo(ts2, 1, 10000, 0, 0, 0)

    session.add_all([ts1_h1, ts1_h2, ts2_h1])
    testing.add_symmetric_link(ts2_h1, ts1_h1)
    testing.add_symmetric_link(ts2_h1, ts1_h2)


    ts1_h1['dummy_histogram'] = test_histogram[DummyHistogramProperty.store_slice(ts1.time_gyr)]
    ts1_h2['dummy_histogram'] = test_histogram[DummyHistogramProperty.store_slice(ts1.time_gyr)]*0.5
    ts2_h1['dummy_histogram'] = test_histogram[DummyHistogramProperty.store_slice(ts2.time_gyr)]

    db.core.get_default_session().commit()


class DummyHistogramProperty(properties.TimeChunkedProperty):
    @classmethod
    def name(self):
        return "dummy_histogram"


def test_default_reconstruction():
    reconstructed = ts2_h1['dummy_histogram']
    assert np.all(reconstructed == test_histogram[:len(reconstructed)])
    assert len(reconstructed)==int(DummyHistogramProperty.nbins*ts2_h1.timestep.time_gyr/DummyHistogramProperty.tmax_Gyr)

def test_summed_reconstruction():
    reconstructed = ts2_h1.get_objects("dummy_histogram")[0].get_data_with_reassembly_options('sum')

    manual_reconstruction = ts2_h1['dummy_histogram']
    added_bit = halo_db.get_halo("sim/ts1/2")['dummy_histogram']
    manual_reconstruction[:len(added_bit)]+=added_bit

    npt.assert_almost_equal(reconstructed, manual_reconstruction)

def test_live_calculation_summed_reconstruction():
    reconstructed = ts2_h1.get_objects("dummy_histogram")[0].get_data_with_reassembly_options('sum')
    reconstructed_lc = ts2_h1.calculate("reassemble(dummy_histogram, 'sum')")
    npt.assert_almost_equal(reconstructed, reconstructed_lc)

def test_placed_reconstruction():
    reconstructed = ts2_h1.get_objects("dummy_histogram")[0].get_data_with_reassembly_options('place')
    manual_reconstruction = np.zeros(len(ts2_h1['dummy_histogram']))
    raw_data = ts2_h1.get_data('dummy_histogram',raw=True)
    manual_reconstruction[-len(raw_data):] = raw_data
    return npt.assert_almost_equal(reconstructed, manual_reconstruction)

def test_summed_reconstruction_across_simulations():


    session = db.core.get_default_session()
    sim2 = halo_db.core.simulation.Simulation("sim2")
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
        added_bit = halo_db.get_halo("sim/ts1/1")['dummy_histogram']
        manual_reconstruction[:len(added_bit)]=added_bit

        npt.assert_almost_equal(reconstructed, manual_reconstruction)



    finally:
        ts2.simulation = db.query.get_simulation("sim")
