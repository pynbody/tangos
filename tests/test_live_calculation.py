from nose.tools import assert_raises

import halo_db as db
import halo_db.live_calculation as lc
import halo_db.testing as testing
import numpy as np

import halo_db.live_calculation.parser
import halo_db
from halo_db import halo_data_extraction_patterns
import properties

def setup():
    db.init_db("sqlite://")

    session = db.core.internal_session

    sim = db.Simulation("sim")

    session.add(sim)

    ts1 = db.TimeStep(sim,"ts1",False)


    session.add(ts1)

    ts1.time_gyr = 1
    ts1.redshift = 10


    ts1_h1 = db.Halo(ts1,1,1000,0,0,0)
    ts1_h2 = db.Halo(ts1,2,900,0,0,0)

    ts1_h1['dummy_property_1'] = np.arange(0,100.0)

    angmom = np.empty((100,3))
    angmom[:,:] = np.arange(0,100.0).reshape((100,1))
    ts1_h1['dummy_property_2'] = angmom


    ts1_h1_bh1 = db.core.BH(ts1,1)
    ts1_h1_bh1["BH_mass"]=1000.0

    ts1_h1_bh2 = db.core.BH(ts1,2)
    ts1_h1_bh2["BH_mass"]=900.0
    ts1_h1["BH"] = ts1_h1_bh1, ts1_h1_bh2


    ts2 = db.TimeStep(sim, "ts2", False)
    session.add(ts2)
    ts2.time_gyr = 2
    ts2.redshift = 9

    ts2_h1 = db.Halo(ts2,1,10000,0,0,0)
    testing.add_symmetric_link(ts2_h1, ts1_h1)

    db.core.internal_session.commit()


class DummyProperty1(properties.HaloProperties):
    @classmethod
    def name(self):
        return "dummy_property_1"

    def plot_x0(cls):
        return 0.0

    def plot_xdelta(cls):
        return 0.1


class DummyProperty2(properties.HaloProperties):
    @classmethod
    def name(self):
        return "dummy_property_2"

    def plot_x0(cls):
        return 0.0

    def plot_xdelta(cls):
        return 0.2


def test_simple_retrieval():
    BH = db.get_halo("sim/ts1/1.1")
    assert BH['BH_mass'] == BH.calculate("BH_mass")

def test_at_function():
    halo = db.get_halo("sim/ts1/1")
    assert np.allclose(halo.calculate("at(3.0,dummy_property_1)"), 30.0)

def test_abs_function():
    halo = db.get_halo("sim/ts1/1")
    assert np.allclose(halo.calculate("abs(dummy_property_2)"), np.arange(0,100.0)*np.sqrt(3))

def test_nested_abs_at_function():
    halo = db.get_halo("sim/ts1/1")
    # n.b. for J_dm_enc
    assert np.allclose(halo.calculate("abs(at(3.0,dummy_property_2))"), 15.0*np.sqrt(3))

def test_abcissa_passing_function():
    """In this example, the x-coordinates need to be successfully passed "through" the abs function for the
    at function to return the correct result."""
    halo = db.get_halo("sim/ts1/1")
    assert np.allclose(halo.calculate("at(3.0,abs(dummy_property_2))"), 15.0*np.sqrt(3))

def test_property_redirection():
    halo = db.get_halo("sim/ts1/1")
    assert halo.calculate("BH.BH_mass")==db.get_halo("sim/ts1/1.1")["BH_mass"]

def test_function_after_property_redirection():
    halo = db.get_halo("sim/ts1/1")
    bh_dbid= halo.calculate("BH.dbid()")
    assert bh_dbid == db.get_halo("sim/ts1/1.1").id

def test_BH_redirection_function():
    halo = db.get_halo("sim/ts1/1")
    bh_dbid= halo.calculate("BH('BH_mass','max','BH').dbid()")
    assert bh_dbid == db.get_halo("sim/ts1/1.1").id

    bh_dbid= halo.calculate("BH('BH_mass','min','BH').dbid()")
    assert bh_dbid == db.get_halo("sim/ts1/1.2").id

def test_non_existent_property():
    halo = db.get_halo("sim/ts1/1")
    with assert_raises(KeyError):
        halo.calculate("non_existent_property")

def test_non_existent_redirection():
    halo = db.get_halo("sim/ts1/2")
    with assert_raises(ValueError):
        halo.calculate("BH.dbid()")

def test_parse_raw_psuedofunction():
    parsed = halo_db.live_calculation.parser.parse_property_name("raw(dummy_property_1)")
    assert parsed._inputs[0]._extraction_pattern is halo_data_extraction_patterns.HaloPropertyRawValueGetter

    assert all(db.get_halo("sim/ts1/1").calculate(parsed)==db.get_halo("sim/ts1/1")['dummy_property_1'])

def test_new_builtin():
    from halo_db.live_calculation import BuiltinFunction

    @BuiltinFunction.register
    def my_test_function(halos):
        return [[101]*len(halos)]

    assert db.get_halo("sim/ts1/2").calculate("my_test_function()")==101

def test_match():
    dbid = db.get_halo("sim/ts1/1").calculate("match('sim/ts2').dbid()")
    assert dbid==db.get_halo("sim/ts2/1").id

def test_match_inappropriate_argument():
    with assert_raises(ValueError):
        db.get_halo("sim/ts1/1").calculate("match(dbid()).dbid()")


def test_arithmetic():
    h = db.get_halo("sim/ts1/1")
    pname = lc.parser.parse_property_name("1.0+2.0")
    assert h.calculate("1.0+2.0")==3.0
    assert h.calculate("1.0-2.0") == -1.0
    assert h.calculate("1.0/2.0") == 0.5
    assert h.calculate("3.0*2.0") == 6.0
    assert h.calculate("1.0+2.0*3.0")==7.0
    assert h.calculate("2.0*3.0+1.0")==7.0
    assert h.calculate("2.0*(3.0+1.0)") == 8.0
    assert h.calculate("2.0**2")==4.0
    assert h.calculate("2.0*3.0**2")==18.0
    assert h.calculate("at(1.0,dummy_property_1)*at(5.0,dummy_property_1)") ==\
           h.calculate("at(1.0,dummy_property_1)") * h.calculate("at(5.0,dummy_property_1)")