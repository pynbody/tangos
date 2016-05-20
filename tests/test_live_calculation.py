import numpy as np
from nose.tools import assert_raises

import halo_db as db
import halo_db
import halo_db.core.halo
import halo_db.core.simulation
import halo_db.core.timestep
import halo_db.live_calculation as lc
import halo_db.live_calculation.parser
import halo_db
import halo_db.testing as testing
import properties
from halo_db.core import extraction_patterns


def setup():
    db.init_db("sqlite://")

    session = db.core.get_default_session()

    sim = halo_db.core.simulation.Simulation("sim")

    session.add(sim)

    ts1 = halo_db.core.timestep.TimeStep(sim, "ts1", False)


    session.add(ts1)

    ts1.time_gyr = 1
    ts1.redshift = 10


    ts1_h1 = halo_db.core.halo.Halo(ts1, 1, 1000, 0, 0, 0)
    ts1_h2 = halo_db.core.halo.Halo(ts1, 2, 900, 0, 0, 0)

    ts1_h1['dummy_property_1'] = np.arange(0,100.0)

    angmom = np.empty((100,3))
    angmom[:,:] = np.arange(0,100.0).reshape((100,1))
    ts1_h1['dummy_property_2'] = angmom


    ts1_h1_bh1 = halo_db.core.halo.BH(ts1, 1)
    ts1_h1_bh1["BH_mass"]=1000.0

    ts1_h1_bh2 = halo_db.core.halo.BH(ts1, 2)
    ts1_h1_bh2["BH_mass"]=900.0
    ts1_h1["BH"] = ts1_h1_bh1, ts1_h1_bh2


    ts2 = halo_db.core.timestep.TimeStep(sim, "ts2", False)
    session.add(ts2)
    ts2.time_gyr = 2
    ts2.redshift = 9

    ts2_h1 = halo_db.core.halo.Halo(ts2, 1, 10000, 0, 0, 0)
    testing.add_symmetric_link(ts2_h1, ts1_h1)

    db.core.get_default_session().commit()


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

class DummyPropertyArray(properties.LiveHaloProperties):
    @classmethod
    def name(cls):
        return "dummy_property_array"

    def live_calculate(self, db_halo_entry, *input_values):
        return np.array([1,2,3])

class DummyPropertyWithReassemblyOptions(properties.HaloProperties):
    @classmethod
    def name(self):
        return "dummy_property_with_reassembly"

    @classmethod
    def reassemble(cls, property, test_option=25):
        if test_option=='actual_data':
            return property.data_raw
        else:
            return test_option

class LivePropertyRequiringRedirectedProperty(properties.LiveHaloProperties):
    @classmethod
    def name(cls):
        return "first_BHs_BH_mass"

    def requires_property(self):
        return "BH.BH_mass",

    def live_calculate(self, db_halo_entry, *input_values):
        return db_halo_entry["BH"][0]["BH_mass"]


def test_simple_retrieval():
    BH = halo_db.get_halo("sim/ts1/1.1")
    assert BH['BH_mass'] == BH.calculate("BH_mass")

def test_at_function():
    halo = halo_db.get_halo("sim/ts1/1")
    assert np.allclose(halo.calculate("at(3.0,dummy_property_1)"), 30.0)

def test_abs_function():
    halo = halo_db.get_halo("sim/ts1/1")
    assert np.allclose(halo.calculate("abs(dummy_property_2)"), np.arange(0,100.0)*np.sqrt(3))

def test_nested_abs_at_function():
    halo = halo_db.get_halo("sim/ts1/1")
    # n.b. for J_dm_enc
    assert np.allclose(halo.calculate("abs(at(3.0,dummy_property_2))"), 15.0*np.sqrt(3))

def test_abcissa_passing_function():
    """In this example, the x-coordinates need to be successfully passed "through" the abs function for the
    at function to return the correct result."""
    halo = halo_db.get_halo("sim/ts1/1")
    assert np.allclose(halo.calculate("at(3.0,abs(dummy_property_2))"), 15.0*np.sqrt(3))

def test_property_redirection():
    halo = halo_db.get_halo("sim/ts1/1")
    assert halo.calculate("BH.BH_mass") == halo_db.get_halo("sim/ts1/1.1")["BH_mass"]

def test_function_after_property_redirection():
    halo = halo_db.get_halo("sim/ts1/1")
    bh_dbid= halo.calculate("BH.dbid()")
    assert bh_dbid == halo_db.get_halo("sim/ts1/1.1").id

def test_BH_redirection_function():
    halo = halo_db.get_halo("sim/ts1/1")
    bh_dbid= halo.calculate("BH('BH_mass','max','BH').dbid()")
    assert bh_dbid == halo_db.get_halo("sim/ts1/1.1").id

    bh_dbid= halo.calculate("BH('BH_mass','min','BH').dbid()")
    assert bh_dbid == halo_db.get_halo("sim/ts1/1.2").id

def test_non_existent_property():
    halo = halo_db.get_halo("sim/ts1/1")
    with assert_raises(KeyError):
        halo.calculate("non_existent_property")

def test_non_existent_redirection():
    halo = halo_db.get_halo("sim/ts1/2")
    with assert_raises(ValueError):
        halo.calculate("BH.dbid()")

def test_parse_raw_psuedofunction():
    parsed = halo_db.live_calculation.parser.parse_property_name("raw(dummy_property_1)")
    assert parsed._inputs[0]._extraction_pattern is extraction_patterns.halo_property_raw_value_getter

    assert all(halo_db.get_halo("sim/ts1/1").calculate(parsed) == halo_db.get_halo("sim/ts1/1")['dummy_property_1'])

def test_new_builtin():
    from halo_db.live_calculation import BuiltinFunction

    @BuiltinFunction.register
    def my_test_function(halos):
        return [[101]*len(halos)]

    assert halo_db.get_halo("sim/ts1/2").calculate("my_test_function()")[0] == 101

def test_match():
    dbid = halo_db.get_halo("sim/ts1/1").calculate("match('sim/ts2').dbid()")
    assert dbid == halo_db.get_halo("sim/ts2/1").id

def test_match_inappropriate_argument():
    with assert_raises(ValueError):
        halo_db.get_halo("sim/ts1/1").calculate("match(dbid()).dbid()")


def test_arithmetic():
    h = halo_db.get_halo("sim/ts1/1")
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

def test_calculate_array():
    h = halo_db.get_halo("sim/ts1/1")

    assert (h.calculate("dummy_property_array()")==[1,2,3]).all()

    assert (h.calculate("BH.dummy_property_array()") == [1, 2, 3]).all()

    assert (h.calculate("dummy_property_array()*2/(BH.dummy_property_array()*2)") == np.array([1,1,1])).all()

def test_reassembly():
    h = halo_db.get_halo("sim/ts1/1")
    h['dummy_property_with_reassembly']=101

    # default reassembly (see above) always returns 25
    assert h['dummy_property_with_reassembly'] == 25

    # raw stored value is 101
    assert h.calculate('raw(dummy_property_with_reassembly)') == 101

    # we can pass in an option to the reassembly function
    assert h.calculate('reassemble(dummy_property_with_reassembly, 50)') == 50

    # our dummy reassembly function can return the actual value if suitably requested
    assert h.calculate('reassemble(dummy_property_with_reassembly, "actual_data")') == 101


def test_liveproperty_requiring_redirection():
    h = halo_db.get_halo("sim/ts1/1")
    assert h.calculate("first_BHs_BH_mass()") == h['BH'][0]['BH_mass']
    cascade_version = h.property_cascade("first_BHs_BH_mass()")
    assert cascade_version[0] == h['BH'][0]['BH_mass']