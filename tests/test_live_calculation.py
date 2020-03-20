from __future__ import absolute_import
import numpy as np
from nose.tools import assert_raises

import tangos as db
import tangos
import tangos.core.halo
import tangos.core.simulation
import tangos.core.timestep
import tangos.live_calculation as lc
import tangos.live_calculation.parser
import tangos
import tangos.testing as testing
import tangos.testing.simulation_generator
from tangos import properties
from tangos.core import extraction_patterns


def setup():
    testing.init_blank_db_for_testing()

    generator = tangos.testing.simulation_generator.TestSimulationGenerator()


    ts1 = generator.add_timestep()
    ts1_h1, ts2_h2 = generator.add_objects_to_timestep(2)


    ts1_h1['dummy_property_1'] = np.arange(0,100.0)

    angmom = np.empty((100,3))
    angmom[:,:] = np.arange(0,100.0).reshape((100,1))
    ts1_h1['dummy_property_2'] = angmom

    ts1_h1['dummy_property_3'] = -2.5


    ts1_h1_bh1 = tangos.core.halo.BH(ts1, 1)
    ts1_h1_bh1["BH_mass"]=1000.0

    ts1_h1_bh2 = tangos.core.halo.BH(ts1, 2)
    ts1_h1_bh2["BH_mass"]=900.0
    ts1_h1["BH"] = ts1_h1_bh1, ts1_h1_bh2


    generator.add_timestep()
    generator.add_objects_to_timestep(1)
    generator.link_last_halos()

    generator.add_timestep() # intentionally empty final timestep





class DummyProperty1(properties.PropertyCalculation):
    names = "dummy_property_1"

    def plot_x0(self):
        return 0.0

    def plot_xdelta(self):
        return 0.1

class DummyProperty2(properties.PropertyCalculation):
    names = "dummy_property_2"

    def plot_x0(self):
        return 0.0

    def plot_xdelta(self):
        return 0.2

class DummyPropertyArray(properties.LivePropertyCalculation):
    names = "dummy_property_array"

    def live_calculate(self, db_halo_entry, *input_values):
        return np.array([1,2,3])

class DummyPropertyWithReassemblyOptions(properties.PropertyCalculation):
    names = "dummy_property_with_reassembly"

    @classmethod
    def reassemble(cls, property, test_option=25):
        if test_option=='actual_data':
            return property.data_raw
        else:
            return test_option

class LivePropertyRequiringRedirectedProperty(properties.LivePropertyCalculation):
    names = "first_BHs_BH_mass"

    def requires_property(self):
        return "BH.BH_mass",

    def live_calculate(self, db_halo_entry, *input_values):
        return db_halo_entry["BH"][0]["BH_mass"]

class LivePropertyWithCustomInterpolator(properties.LivePropertyCalculation):
    names = "property_with_custom_interpolator"

    def live_calculate(self, halo_entry, *input_values):
        return np.arange(10,20) # irrelevant

    def get_interpolated_value(self, at_x_position, property_array):
        return at_x_position


def test_simple_retrieval():
    BH = tangos.get_halo("sim/ts1/1.1")
    assert BH['BH_mass'] == BH.calculate("BH_mass")

def test_at_function():
    halo = tangos.get_halo("sim/ts1/1")
    assert np.allclose(halo.calculate("at(3.0,dummy_property_1)"), 30.0)

def test_custom_at_function():
    halo = tangos.get_halo("sim/ts1/1")
    assert np.allclose(halo.calculate("at(3.0,property_with_custom_interpolator())"), 3.0)

def test_abs_array_function():
    halo = tangos.get_halo("sim/ts1/1")
    assert np.allclose(halo.calculate("abs(dummy_property_2)"), halo.calculate("abs(dummy_property_2 * (-1))"))
    assert np.allclose(halo.calculate("abs(dummy_property_2)"), np.arange(0,100.0)*np.sqrt(3))

def test_abs_scalar_function():
    # Test that abs also works on a single scalar (issue 110)
    halo = tangos.get_halo("sim/ts1/1")
    assert np.allclose(halo.calculate("abs(dummy_property_3)"), - halo.calculate("dummy_property_3"))
    assert np.allclose(halo.calculate("abs(dummy_property_3)"), 2.5)

def test_nested_abs_at_function():
    halo = tangos.get_halo("sim/ts1/1")
    # n.b. for J_dm_enc
    assert np.allclose(halo.calculate("abs(at(3.0,dummy_property_2))"), 15.0*np.sqrt(3))

def test_abcissa_passing_function():
    """In this example, the x-coordinates need to be successfully passed "through" the abs function for the
    at function to return the correct result."""
    halo = tangos.get_halo("sim/ts1/1")
    assert np.allclose(halo.calculate("at(3.0,abs(dummy_property_2))"), 15.0*np.sqrt(3))

def test_property_redirection():
    halo = tangos.get_halo("sim/ts1/1")
    assert halo.calculate("BH.BH_mass") == tangos.get_halo("sim/ts1/1.1")["BH_mass"]

def test_function_after_property_redirection():
    halo = tangos.get_halo("sim/ts1/1")
    bh_dbid= halo.calculate("BH.dbid()")
    assert bh_dbid == tangos.get_halo("sim/ts1/1.1").id

def test_BH_redirection_function():
    halo = tangos.get_halo("sim/ts1/1")
    bh_dbid= halo.calculate("BH('BH_mass','max','BH').dbid()")
    assert bh_dbid == tangos.get_halo("sim/ts1/1.1").id

    bh_dbid= halo.calculate("BH('BH_mass','min','BH').dbid()")
    assert bh_dbid == tangos.get_halo("sim/ts1/1.2").id

def test_non_existent_property():
    halo = tangos.get_halo("sim/ts1/1")
    with assert_raises(KeyError):
        halo.calculate("non_existent_property")

def test_non_existent_redirection():
    halo = tangos.get_halo("sim/ts1/2")
    with assert_raises(ValueError):
        halo.calculate("BH.dbid()")

def test_parse_raw_psuedofunction():
    parsed = tangos.live_calculation.parser.parse_property_name("raw(dummy_property_1)")
    assert isinstance(parsed._inputs[0]._extraction_pattern,
                      extraction_patterns.HaloPropertyRawValueGetter)

    assert all(tangos.get_halo("sim/ts1/1").calculate(parsed) == tangos.get_halo("sim/ts1/1")['dummy_property_1'])

def test_new_builtin():
    from tangos.live_calculation import BuiltinFunction

    @BuiltinFunction.register
    def my_test_function(halos):
        return [[101]*len(halos)]

    assert tangos.get_halo("sim/ts1/2").calculate("my_test_function()")[0] == 101

def test_match():
    dbid = tangos.get_halo("sim/ts1/1").calculate("match('sim/ts2').dbid()")
    assert dbid == tangos.get_halo("sim/ts2/1").id

def test_match_inappropriate_argument():
    with assert_raises(ValueError):
        tangos.get_halo("sim/ts1/1").calculate("match(dbid()).dbid()")


def test_arithmetic():
    h = tangos.get_halo("sim/ts1/1")
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

def test_comparison():
    h = tangos.get_halo("sim/ts1/1")
    assert h.calculate("1.0<2.0")
    assert not h.calculate("1.0>2.0")
    assert h.calculate("1.0==1.0")
    assert h.calculate("1.0>=1.0")
    assert h.calculate("1.0<=1.0")
    assert h.calculate("1.0>=0.5")
    assert not h.calculate("1.0<=0.5")


def test_calculate_array():
    h = tangos.get_halo("sim/ts1/1")

    assert (h.calculate("dummy_property_array()")==[1,2,3]).all()

    assert (h.calculate("BH.dummy_property_array()") == [1, 2, 3]).all()

    assert (h.calculate("dummy_property_array()*2/(BH.dummy_property_array()*2)") == np.array([1,1,1])).all()

def test_calculate_array_element():
    h = tangos.get_halo("sim/ts1/1")
    assert h.calculate("dummy_property_array()[0]")==1

    h['test_array'] = [5,6,7]
    assert h.calculate("test_array[1]")==6


def test_reassembly():
    h = tangos.get_halo("sim/ts1/1")
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
    h = tangos.get_halo("sim/ts1/1")
    assert h.calculate("first_BHs_BH_mass()") == h['BH'][0]['BH_mass']
    cascade_version = h.calculate_for_descendants("first_BHs_BH_mass()")
    assert cascade_version[0] == h['BH'][0]['BH_mass']


def test_calculate_preserves_numpy_dtype():
    h = tangos.get_halo("sim/ts1/1")
    assert h.calculate("dummy_property_1").dtype == np.float64
    assert h.calculate("dummy_property_2").dtype==np.float64

def test_empty_timestep_live_calculation():
    vals, = tangos.get_timestep("sim/ts3").calculate_all("BH_mass")
    assert len(vals)==0


def test_non_existent_redirection_multihalo():
    # See issue #46
    vals1, vals2 = tangos.get_timestep("sim/ts3").calculate_all("BH_mass","later(1).BH_mass")
    assert len(vals1)==0
    assert len(vals2)==0