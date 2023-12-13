import warnings

import numpy as np
import numpy.testing as npt

import tangos
import tangos as db
import tangos.core.halo
import tangos.core.simulation
import tangos.core.timestep
import tangos.testing.simulation_generator
from tangos import properties, testing


def setup_module():
    testing.init_blank_db_for_testing()

    creator = tangos.testing.simulation_generator.SimulationGeneratorForTests()

    halo_offset = 0
    for ts in range(1,4):
        num_halos = 4 if ts<3 else 3
        creator.add_timestep()
        creator.add_objects_to_timestep(num_halos)
        creator.add_properties_to_halos(Mvir=lambda i: i+halo_offset)
        creator.add_properties_to_halos(Rvir=lambda i: (i+halo_offset)*0.1)
        halo_offset+=num_halos

        creator.add_bhs_to_timestep(4)
        creator.add_properties_to_bhs(hole_mass = lambda i: float(i*100))
        creator.add_properties_to_bhs(hole_spin = lambda i: 1000-float(i)*100)
        creator.add_properties_to_bhs(test_array = lambda i: np.array([1.0,2.0,3.0]))
        creator.assign_bhs_to_halos({1:1, 2:2, 3:3, 4:3})


        if ts>1:
            creator.link_last_halos()
            creator.link_last_bhs_using_mapping({1:1})

    # special timestep 5->6 where there is an exact equal weight link
    creator.add_timestep()
    creator.add_objects_to_timestep(2, NDM=[5,5])
    creator.add_timestep()
    creator.add_objects_to_timestep(1, NDM=[10])
    creator.link_last_halos_using_mapping({1:1, 2:1})

def teardown_module():
    tangos.core.close_db()

class _TestProperty(properties.LivePropertyCalculation):
    names = "RvirPlusMvir"

    def requires_property(self):
        return "Mvir", "Rvir"

    def live_calculate(self, halo):
        return halo["Mvir"]+halo["Rvir"]

class _TestErrorProperty(properties.LivePropertyCalculation):
    names = "RvirPlusMvirMiscoded"

    def requires_property(self):
        return "Mvir",

    def live_calculate(self, halo):
        return halo["Mvir"]+halo["Rvir"]

class _TestPropertyWithParameter(properties.LivePropertyCalculation):
    names = "squared"

    def live_calculate(self, halo, value):
        return value**2

class _TestBrokenProperty(properties.PropertyCalculation):
    names = "brokenproperty"

    def __init__(self, simulation):
        raise RuntimeError("This intentionally breaks the property")

class _TestPathChoice(properties.LivePropertyCalculation):
    num_calls = 0
    names = "my_BH"

    def __init__(self, simulation, criterion="hole_mass"):
        super().__init__(simulation, criterion)
        assert isinstance(criterion, str), "Criterion must be a named BH property"
        self.criterion = criterion

    def requires_property(self):
        return "BH", "BH."+self.criterion

    def live_calculate(self, halo, criterion="hole_mass"):
        type(self).num_calls+=1
        bh_links = halo["BH"]
        if isinstance(bh_links,list):
            vals = [lk[criterion] if criterion in lk else self.default_val for lk in bh_links]
            return bh_links[np.argmax(vals)]
        else:
            return bh_links


def test_calculate_all():
    Mv,  = tangos.get_timestep("sim/ts2").calculate_all("Mvir")
    npt.assert_allclose(Mv,[5,6,7,8])

    Mv, Rv  = tangos.get_timestep("sim/ts1").calculate_all("Mvir", "Rvir")
    npt.assert_allclose(Mv,[1,2,3,4])
    npt.assert_allclose(Rv,[0.1,0.2,0.3,0.4])

def test_calculate_all_limit():
    Mv, = tangos.get_timestep("sim/ts2").calculate_all("Mvir",limit=3)
    npt.assert_allclose(Mv, [5, 6, 7])

def test_gather_function():

    Vv, = tangos.get_timestep("sim/ts1").calculate_all("RvirPlusMvir()")
    npt.assert_allclose(Vv,[1.1,2.2,3.3,4.4])

    Vv, = tangos.get_timestep("sim/ts2").calculate_all("RvirPlusMvir()")
    npt.assert_allclose(Vv,[5.5,6.6,7.7,8.8])


def test_gather_function_fails():
    with npt.assert_raises(KeyError):
        # The following should fail.
        # If it does not raise a keyerror, the live calculation has ignored the directive
        # to only load in the named properties.
        Vv, = tangos.get_timestep("sim/ts1").calculate_all("RvirPlusMvirMiscoded()")

def test_gather_function_with_parameter():
    res, = tangos.get_timestep("sim/ts1").calculate_all("squared(Mvir)")
    npt.assert_allclose(res, [1.0, 4.0, 9.0, 16.0])


def test_gather_linked_property():
    BH_mass, = tangos.get_timestep("sim/ts1").calculate_all("BH.hole_mass")
    npt.assert_allclose(BH_mass, [100.,200.,300.])

    BH_mass, Mv = tangos.get_timestep("sim/ts1").calculate_all("BH.hole_mass", "Mvir")
    npt.assert_allclose(BH_mass, [100.,200.,300.])
    npt.assert_allclose(Mv, [1.,2.,3.])

def test_gather_linked_property_with_fn():
    BH_mass, Mv = tangos.get_timestep("sim/ts1").calculate_all('my_BH().hole_mass', "Mvir")
    npt.assert_allclose(BH_mass, [100.,200.,400.])
    npt.assert_allclose(Mv, [1.,2.,3.])

    BH_mass, Mv = tangos.get_timestep("sim/ts1").calculate_all('my_BH("hole_spin").hole_mass', "Mvir")
    npt.assert_allclose(BH_mass, [100.,200.,300.])
    npt.assert_allclose(Mv, [1.,2.,3.])

def test_path_factorisation():

    _TestPathChoice.num_calls = 0

    #desc = lc.MultiCalculationDescription(
    #    'my_BH("hole_spin").hole_mass',
    #    'my_BH("hole_spin").hole_spin',
    #    'Mvir')

    BH_mass, BH_spin, Mv = tangos.get_timestep("sim/ts1").calculate_all('my_BH("hole_spin").(hole_mass, hole_spin)', 'Mvir')
    npt.assert_allclose(BH_mass, [100.,200.,300.])
    npt.assert_allclose(BH_spin, [900.,800.,700.])
    npt.assert_allclose(Mv, [1.,2.,3.])

    # despite being referred to twice, the my_BH function should only actually be called
    # once per halo. Otherwise the factorisation has been done wrong (and in particular,
    # a second call to the DB to retrieve the BH objects has been made, which could be
    # expensive)
    assert _TestPathChoice.num_calls==3


def test_single_quotes():
    BH_mass, Mv = tangos.get_timestep("sim/ts1").calculate_all("my_BH('hole_spin').hole_mass", "Mvir")
    npt.assert_allclose(BH_mass, [100.,200.,300.])
    npt.assert_allclose(Mv, [1.,2.,3.])


def test_calculate_for_descendants():
    h = tangos.get_halo("sim/ts1/1")
    objs, = h.calculate_for_descendants("dbid()")
    assert len(objs)==3
    assert all([objs[i] == tangos.get_halo(x).id for i, x in enumerate(("sim/ts1/1", "sim/ts2/1", "sim/ts3/1"))])

def test_calculate_for_progenitors():
    h = tangos.get_halo("sim/ts3/1")
    objs, = h.calculate_for_progenitors("dbid()")
    assert len(objs)==3
    assert all([objs[i] == tangos.get_halo(x).id for i, x in enumerate(("sim/ts3/1", "sim/ts2/1", "sim/ts1/1"))])

def test_calculate_for_progenitors_bh():
    h = tangos.get_halo("sim/ts3/bh_1")
    objs, = h.calculate_for_progenitors("dbid()")
    testing.assert_halolists_equal(objs, ['sim/ts3/BH_1', 'sim/ts2/BH_1', 'sim/ts1/BH_1'])

def test_match_gather():
    ts1_halos, ts3_halos = tangos.get_timestep("sim/ts1").calculate_all('dbid()', 'match("sim/ts3").dbid()')
    testing.assert_halolists_equal(ts1_halos, ['sim/ts1/1','sim/ts1/2','sim/ts1/3', 'sim/ts1/1.1'])
    testing.assert_halolists_equal(ts3_halos, ['sim/ts3/1','sim/ts3/2','sim/ts3/3', 'sim/ts3/1.1'])

def test_later():
    ts1_halos, ts3_halos = tangos.get_timestep("sim/ts1").calculate_all('dbid()', 'later(2).dbid()')
    testing.assert_halolists_equal(ts1_halos, ['sim/ts1/1', 'sim/ts1/2', 'sim/ts1/3', 'sim/ts1/1.1'])
    testing.assert_halolists_equal(ts3_halos, ['sim/ts3/1', 'sim/ts3/2', 'sim/ts3/3', 'sim/ts3/1.1'])

def test_earlier():
    ts3_halos, ts1_halos = tangos.get_timestep("sim/ts3").calculate_all('dbid()', 'earlier(2).dbid()')
    testing.assert_halolists_equal(ts1_halos, ['sim/ts1/1', 'sim/ts1/2', 'sim/ts1/3', 'sim/ts1/1.1'])
    testing.assert_halolists_equal(ts3_halos, ['sim/ts3/1', 'sim/ts3/2', 'sim/ts3/3', 'sim/ts3/1.1'])

def test_return_nones():
    dbids, masses = tangos.get_timestep("sim/ts3").calculate_all("dbid()","hole_mass")
    assert len(masses)==4 # only the BHs have a hole_mass
    assert len(dbids)==4

    dbids, masses = tangos.get_timestep("sim/ts3").calculate_all("dbid()", "hole_mass", sanitize=False)
    assert len(masses)==7 # BHs + halos all now returning a row
    assert len(dbids)==7

def test_earlier_equal_weight():
    """Regression test for problem where earlier(x) or later(x) failed when there was more than one link
    with the identical same weight, meaning the major progenitor was undefined. Now the match is
    disambiguiated by looking for the link with the highest primary key (not very physical, but at least
    unambiguous)."""
    ts5_halo, ts4_halo = tangos.get_timestep("sim/ts5").calculate_all("dbid()","earlier(1).dbid()")
    testing.assert_halolists_equal(ts5_halo, ['sim/ts5/1'])
    assert testing.halolists_equal(ts4_halo, ['sim/ts4/1']) or testing.halolists_equal(ts4_halo, ['sim/ts4/2'])

def test_cascade_closes_connections():
    h = tangos.get_halo("sim/ts3/1")
    with db.testing.assert_connections_all_closed():
        h.calculate_for_progenitors("Mvir")

def test_redirection_cascade_closes_connections():
    h = tangos.get_halo("sim/ts3/1")
    with db.testing.assert_connections_all_closed():
        h.calculate_for_progenitors("my_BH('hole_spin').hole_mass")

def test_redirection_cascade_efficiency():
    h = tangos.get_halo("sim/ts3/1")
    with testing.SqlExecutionTracker(db.core.get_default_engine()) as track:
        h.calculate_for_progenitors("my_BH('hole_spin').test_array")
    assert "select haloproperties" not in track # array should have been loaded as part of a big join

def test_gather_closes_connections():
    ts = tangos.get_timestep("sim/ts1")
    with db.testing.assert_connections_all_closed():
        ts.calculate_all('Mvir')

def test_gather_restricted_object_type():
    ts = tangos.get_timestep("sim/ts1")
    non_existent, = ts.calculate_all("hole_mass", object_typetag='halo')
    assert len(non_existent)==0
    ok_1, = ts.calculate_all("hole_mass",object_typetag='BH')
    ok_2, = ts.calculate_all("hole_mass")
    npt.assert_allclose(ok_1, [100., 200., 300., 400.])
    npt.assert_allclose(ok_2, [100., 200., 300., 400.])

def test_missing_or_broken_class():
    ts = tangos.get_timestep("sim/ts1")
    for (i,h) in enumerate(ts.halos):
        h["noclass"] = 10.0*i
        h["brokenproperty"] = 10.0*i
    tangos.get_default_session().commit()

    noclass, = ts.calculate_all("noclass")
    npt.assert_allclose(noclass, [0., 10., 20., 30.])

    with warnings.catch_warnings(record=True) as w:
        brokenclass, = ts.calculate_all("brokenproperty")
    npt.assert_allclose(noclass, [0., 10., 20., 30.])
    assert len(w)>0
