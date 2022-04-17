import warnings

import numpy as np
import numpy.testing as npt
from pytest import raises as assert_raises

import tangos as db
import tangos.live_calculation as lc
import tangos.testing as testing
import tangos.testing.simulation_generator
from tangos.testing import assert_halolists_equal


def setup_module():
    testing.init_blank_db_for_testing(verbose=True)

    generator = tangos.testing.simulation_generator.SimulationGeneratorForTests()
    generator.add_timestep()
    ts1_h1, ts1_h2, ts1_h3, ts1_h4, ts1_h5 = generator.add_objects_to_timestep(5)




    ts1_h1['testlink'] = ts1_h2, ts1_h3, ts1_h4, ts1_h5
    ts1_h1['testlink_univalued'] = ts1_h2


    ts1_h5['nonexistent_link'] = 0 # This is just to generate the results

    ts1_h2['testval'] = 1.0
    ts1_h2['testvalpartial'] = 1.0
    ts1_h3['testval'] = 2.0
    ts1_h3['testvalpartial'] = 2.0
    ts1_h4['testval'] = 3.0
    ts1_h5['testval'] = 0.0

    ts1_h2['testval2'] = 10.0
    ts1_h3['testval2'] = 20.0
    ts1_h4['testval2'] = 30.0
    ts1_h5['testval2'] = 40.0

    generator.add_timestep()
    ts2_h1, ts2_h2, ts2_h3 = generator.add_objects_to_timestep(3)
    ts2_h1['testval'] = 2.0
    ts2_h2['testval'] = 100.0
    generator.link_last_halos_using_mapping({2: 1, 3: 2})

    generator_sim2 = tangos.testing.simulation_generator.SimulationGeneratorForTests("sim2")
    # this is added to offer red herring links that should be ignored
    # We add a timestep and links that create an illusory past, but actually it's in another simulation
    # so should be ignored. It triggers a problem with the original implementation of find_progenitor.

    generator_sim2.add_timestep()
    s2_h1, s2_h2 = generator_sim2.add_objects_to_timestep(2)
    generator_sim2.link_last_halos_across_using_mapping(generator, {1: 1, 2: 2})
    s2_h2['testval'] = 1e6

    # end of red herring

    generator.add_timestep()
    ts3_h1, ts3_h2 = generator.add_objects_to_timestep(2)
    generator.link_last_halos()
    ts3_h1['testval'] = -1.0
    ts3_h2['testval'] = 1000.0

    db.core.get_default_session().commit()

def teardown_module():
    tangos.core.close_db()

def test_ambiguous_link():
    with warnings.catch_warnings(record=True) as w:
        assert db.get_halo("sim/ts1/1").calculate('testlink.testval')==1.0
    assert len(w)>0

def test_ambiguous_link_made_explicit():
    assert db.get_halo("sim/ts1/1").calculate('link(testlink).halo_number()') == 2
    assert db.get_halo("sim/ts1/1").calculate('link(testlink,testval).halo_number()')==4
    assert db.get_halo("sim/ts1/1").calculate('link(testlink,testval,"min").halo_number()') == 5

def test_link_returned_halo_is_valid():
    assert db.get_halo("sim/ts1/1").calculate('link(testlink)').halo_number == 2
    assert db.get_halo("sim/ts1/1").calculate('link(testlink,testval)').halo_number == 4
    assert db.get_halo("sim/ts1/1").calculate('link(testlink,testval,"min")').halo_number == 5
    assert db.get_halo("sim/ts1/1").calculate('link(testlink,testval,"min",testval>1)').halo_number==3
    assert db.get_halo("sim/ts1/1").calculate('link(testlink,testval,"max",testval2>10,testval2<40)').halo_number==4
    assert db.get_halo("sim/ts1/1").calculate('link(testlink,testval,"min",testval2>10,testval2<40)').halo_number==3

def test_link_missing_data():
    assert db.get_halo("sim/ts1/1").calculate('link(testlink,testvalpartial,"max")').halo_number == 3
    assert db.get_halo("sim/ts1/1").calculate('link(testlink,testval,"max",testvalpartial>1.0)').halo_number == 3

def test_link_can_be_used_within_calculation():
    assert db.get_halo("sim/ts1/1").calculate('link(testlink).testval2') == 10.0
    assert db.get_halo("sim/ts1/1").calculate('link(testlink,testval).testval2') == 30.0
    assert db.get_halo("sim/ts1/1").calculate('link(testlink,testval,"min").testval2') == 40.0
    assert db.get_halo("sim/ts1/1").calculate('link(testlink,testval,"max",testval2>10,testval2<40).testval2') == 30

def test_link_returned_halo_is_usable():
    assert db.get_halo("sim/ts1/1").calculate('link(testlink)')["testval"] == 1.0
    assert db.get_halo("sim/ts1/1").calculate('link(testlink,testval)')["testval"] == 3.0
    assert db.get_halo("sim/ts1/1").calculate('link(testlink,testval,"min")')["testval"] == 0.0
    assert db.get_halo("sim/ts1/1").calculate('link(testlink,testval,"max",testval2>10,testval2<40)')["testval"] == 3.0


def test_multi_calculation_link_returned_halo_is_usable():
    all_links = db.get_timestep("sim/ts1").calculate_all('link(testlink)')
    assert all_links[0][0]['testval']==1.0

    all_links = db.get_halo("sim/ts1/1").calculate_for_progenitors('link(testlink)')
    assert all_links[0][0]['testval'] == 1.0

def test_unambiguous_link():
    with warnings.catch_warnings(record=True) as w:
        assert db.get_halo("sim/ts1/1").calculate('testlink_univalued.testval')==1.0
    assert len(w)==0

def test_missing_link():
    npt.assert_allclose(db.get_timestep("sim/ts1").calculate_all("testlink.testval"), [[1.0]])

    with assert_raises(lc.NoResultsError):
        db.get_halo("sim/ts1/1").calculate("nonexistent_link.testval")

def test_historical_value_finding():
    vals = db.get_halo("sim/ts3/1").calculate_for_progenitors("testval")
    halo = db.get_halo("sim/ts3/1")
    assert_halolists_equal([halo.calculate("find_progenitor(testval, 'max')")], ["sim/ts2/1"])
    assert_halolists_equal([halo.calculate("find_progenitor(testval, 'min')")], ["sim/ts3/1"])

    timestep = db.get_timestep("sim/ts3")
    assert_halolists_equal(timestep.calculate_all("find_progenitor(testval, 'max')")[0], ["sim/ts2/1", "sim/ts3/2"])
    assert_halolists_equal(timestep.calculate_all("find_progenitor(testval, 'min')")[0], ["sim/ts3/1", "sim/ts1/3"])
    assert_halolists_equal(db.get_timestep("sim/ts1").calculate_all("find_descendant(testval, 'min')")[0],
                           ["sim/ts3/1", "sim/ts1/3", "sim/ts1/4", "sim/ts1/5"])

def test_historical_value_finding_missing_data():
    sources, targets = db.get_timestep("sim/ts3").calculate_all("path()", "find_progenitor(testvalpartial, 'max')")
    assert_halolists_equal(sources, ["sim/ts3/1", "sim/ts3/2"])
    assert_halolists_equal(targets, ["sim/ts1/2", "sim/ts1/3"])

def test_latest_earliest():
    earliest = db.get_halo("sim/ts3/1").calculate("earliest()")
    assert earliest == db.get_halo("sim/ts3/1").earliest

    latest = db.get_halo("sim/ts1/2").calculate("latest()")
    assert latest == db.get_halo("sim/ts1/2").latest

    latest = db.get_halo("sim/ts3/1").calculate("latest()")
    assert latest == db.get_halo("sim/ts3/1") # this is already the latest. See #123

    earliest_no_ancestors = db.get_halo("sim/ts2/3").calculate("earliest()") # has no predecessor in ts1. See #124
    assert earliest_no_ancestors == db.get_halo("sim/ts2/3")

    multi_earliest,  = db.get_timestep("sim/ts2").calculate_all("earliest()")
    assert_halolists_equal(multi_earliest, ['sim/ts1/2', 'sim/ts1/3', 'sim/ts2/3'])
