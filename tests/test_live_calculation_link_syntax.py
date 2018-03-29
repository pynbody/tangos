from __future__ import absolute_import
import numpy as np
import numpy.testing as npt
from nose.tools import assert_raises

import tangos as db
import tangos.testing as testing
import tangos.live_calculation as lc
import warnings

import tangos.testing.simulation_generator


def setup():
    testing.init_blank_db_for_testing()

    generator = tangos.testing.simulation_generator.TestSimulationGenerator()
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

    db.core.get_default_session().commit()

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

def test_link_missing_data():
    assert db.get_halo("sim/ts1/1").calculate('link(testlink,testvalpartial,"max")').halo_number == 3

def test_link_can_be_used_within_calculation():
    assert db.get_halo("sim/ts1/1").calculate('link(testlink).testval2') == 10.0
    assert db.get_halo("sim/ts1/1").calculate('link(testlink,testval).testval2') == 30.0
    assert db.get_halo("sim/ts1/1").calculate('link(testlink,testval,"min").testval2') == 40.0


def test_link_returned_halo_is_usable():
    assert db.get_halo("sim/ts1/1").calculate('link(testlink)')["testval"] == 1.0
    assert db.get_halo("sim/ts1/1").calculate('link(testlink,testval)')["testval"] == 3.0
    assert db.get_halo("sim/ts1/1").calculate('link(testlink,testval,"min")')["testval"] == 0.0


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