import tangos as db
import numpy as np
import numpy.testing as npt

import tangos.core.halo
import tangos.core.simulation
import tangos.core.timestep
import tangos
import properties
from tangos import testing
import os

def setup():
    testing.init_blank_db_for_testing()

    creator = tangos.testing.TestSimulationGenerator()

    halo_offset = 0
    for ts in range(1,4):
        num_halos = 4 if ts<3 else 3
        creator.add_timestep()
        creator.add_halos_to_timestep(num_halos)
        creator.add_properties_to_halos(Mvir=lambda i: i+halo_offset)
        creator.add_properties_to_halos(Rvir=lambda i: (i+halo_offset)*0.1)
        halo_offset+=num_halos

        creator.add_bhs_to_timestep(4)
        creator.add_properties_to_bhs(hole_mass = lambda i: float(i*100))
        creator.add_properties_to_bhs(hole_spin = lambda i: 1000-float(i)*100)

        creator.assign_bhs_to_halos({1:1, 2:2, 3:3, 4:3})


        if ts>1:
            creator.link_last_halos()
            creator.link_last_bhs_using_mapping({1:1})

class TestProperty(properties.LiveHaloProperties):
    @classmethod
    def name(self):
        return "RvirPlusMvir"

    def requires_property(self):
        return "Mvir", "Rvir"

    def live_calculate(self, halo):
        return halo["Mvir"]+halo["Rvir"]

class TestErrorProperty(properties.LiveHaloProperties):

    @classmethod
    def name(self):
        return "RvirPlusMvirMiscoded"

    @classmethod
    def requires_simdata(self):
        return False

    def requires_property(self):
        return "Mvir",

    def live_calculate(self, halo):
        return halo["Mvir"]+halo["Rvir"]

class TestPropertyWithParameter(properties.LiveHaloProperties):
    @classmethod
    def name(cls):
        return "squared"

    def live_calculate(self, halo, value):
        return value**2

class TestPathChoice(properties.LiveHaloProperties):
    num_calls = 0

    def __init__(self, simulation, criterion="hole_mass"):
        super(TestPathChoice, self).__init__(simulation, criterion)
        assert isinstance(criterion, basestring), "Criterion must be a named BH property"
        self.criterion = criterion

    @classmethod
    def name(cls):
        return "my_BH"

    def requires_property(self):
        return "BH", "BH."+self.criterion

    def live_calculate(self, halo, criterion="hole_mass"):
        type(self).num_calls+=1
        bh_links = halo["BH"]
        if isinstance(bh_links,list):
            for lk in bh_links:
                print lk.keys()
            vals = [lk[criterion] if criterion in lk else self.default_val for lk in bh_links]
            return bh_links[np.argmax(vals)]
        else:
            return bh_links


def test_gather_property():
    Mv,  = tangos.get_timestep("sim/ts2").gather_property("Mvir")
    npt.assert_allclose(Mv,[5,6,7,8])

    Mv, Rv  = tangos.get_timestep("sim/ts1").gather_property("Mvir", "Rvir")
    npt.assert_allclose(Mv,[1,2,3,4])
    npt.assert_allclose(Rv,[0.1,0.2,0.3,0.4])

def test_gather_function():

    Vv, = tangos.get_timestep("sim/ts1").gather_property("RvirPlusMvir()")
    npt.assert_allclose(Vv,[1.1,2.2,3.3,4.4])

    Vv, = tangos.get_timestep("sim/ts2").gather_property("RvirPlusMvir()")
    npt.assert_allclose(Vv,[5.5,6.6,7.7,8.8])


def test_gather_function_fails():
    with npt.assert_raises(KeyError):
        # The following should fail.
        # If it does not raise a keyerror, the live calculation has ignored the directive
        # to only load in the named properties.
        Vv, = tangos.get_timestep("sim/ts1").gather_property("RvirPlusMvirMiscoded()")

def test_gather_function_with_parameter():
    res, = tangos.get_timestep("sim/ts1").gather_property("squared(Mvir)")
    npt.assert_allclose(res, [1.0, 4.0, 9.0, 16.0])


def test_gather_linked_property():
    BH_mass, = tangos.get_timestep("sim/ts1").gather_property("BH.hole_mass")
    npt.assert_allclose(BH_mass, [100.,200.,300.])

    BH_mass, Mv = tangos.get_timestep("sim/ts1").gather_property("BH.hole_mass", "Mvir")
    npt.assert_allclose(BH_mass, [100.,200.,300.])
    npt.assert_allclose(Mv, [1.,2.,3.])

def test_gather_linked_property_with_fn():
    BH_mass, Mv = tangos.get_timestep("sim/ts1").gather_property('my_BH().hole_mass', "Mvir")
    npt.assert_allclose(BH_mass, [100.,200.,400.])
    npt.assert_allclose(Mv, [1.,2.,3.]) 

    BH_mass, Mv = tangos.get_timestep("sim/ts1").gather_property('my_BH("hole_spin").hole_mass', "Mvir")
    npt.assert_allclose(BH_mass, [100.,200.,300.])
    npt.assert_allclose(Mv, [1.,2.,3.])

def test_path_factorisation():

    TestPathChoice.num_calls = 0

    #desc = lc.MultiCalculationDescription(
    #    'my_BH("hole_spin").hole_mass',
    #    'my_BH("hole_spin").hole_spin',
    #    'Mvir')

    BH_mass, BH_spin, Mv = tangos.get_timestep("sim/ts1").gather_property('my_BH("hole_spin").(hole_mass, hole_spin)', 'Mvir')
    npt.assert_allclose(BH_mass, [100.,200.,300.])
    npt.assert_allclose(BH_spin, [900.,800.,700.])
    npt.assert_allclose(Mv, [1.,2.,3.])

    # despite being referred to twice, the my_BH function should only actually be called
    # once per halo. Otherwise the factorisation has been done wrong (and in particular,
    # a second call to the DB to retrieve the BH objects has been made, which could be
    # expensive)
    assert TestPathChoice.num_calls==3


def test_single_quotes():
    BH_mass, Mv = tangos.get_timestep("sim/ts1").gather_property("my_BH('hole_spin').hole_mass", "Mvir")
    npt.assert_allclose(BH_mass, [100.,200.,300.])
    npt.assert_allclose(Mv, [1.,2.,3.])


def test_property_cascade():
    h = tangos.get_halo("sim/ts1/1")
    objs, = h.property_cascade("dbid()")
    assert len(objs)==3
    assert all([objs[i] == tangos.get_halo(x).id for i, x in enumerate(("sim/ts1/1", "sim/ts2/1", "sim/ts3/1"))])

def test_reverse_property_cascade():
    h = tangos.get_halo("sim/ts3/1")
    objs, = h.reverse_property_cascade("dbid()")
    assert len(objs)==3
    assert all([objs[i] == tangos.get_halo(x).id for i, x in enumerate(("sim/ts3/1", "sim/ts2/1", "sim/ts1/1"))])

def test_match_gather():
    ts1_halos, ts3_halos = tangos.get_timestep("sim/ts1").gather_property('dbid()', 'match("sim/ts3").dbid()')
    testing.assert_halolists_equal(ts1_halos, ['sim/ts1/1','sim/ts1/2','sim/ts1/3', 'sim/ts1/1.1'])
    testing.assert_halolists_equal(ts3_halos, ['sim/ts3/1','sim/ts3/2','sim/ts3/3', 'sim/ts3/1.1'])

def test_later():
    ts1_halos, ts3_halos = tangos.get_timestep("sim/ts1").gather_property('dbid()', 'later(2).dbid()')
    testing.assert_halolists_equal(ts1_halos, ['sim/ts1/1', 'sim/ts1/2', 'sim/ts1/3', 'sim/ts1/1.1'])
    testing.assert_halolists_equal(ts3_halos, ['sim/ts3/1', 'sim/ts3/2', 'sim/ts3/3', 'sim/ts3/1.1'])

def test_earlier():
    ts3_halos, ts1_halos = tangos.get_timestep("sim/ts3").gather_property('dbid()', 'earlier(2).dbid()')
    testing.assert_halolists_equal(ts1_halos, ['sim/ts1/1', 'sim/ts1/2', 'sim/ts1/3', 'sim/ts1/1.1'])
    testing.assert_halolists_equal(ts3_halos, ['sim/ts3/1', 'sim/ts3/2', 'sim/ts3/3', 'sim/ts3/1.1'])


def test_cascade_closes_connections():
    h = tangos.get_halo("sim/ts3/1")
    with db.testing.assert_connections_all_closed():
        h.reverse_property_cascade("Mvir")

def test_redirection_cascade_closes_connections():
    h = tangos.get_halo("sim/ts3/1")
    with db.testing.assert_connections_all_closed():
        h.reverse_property_cascade("my_BH('hole_spin').hole_mass")

def test_gather_closes_connections():
     with db.testing.assert_connections_all_closed():
        tangos.get_timestep("sim/ts1").gather_property('Mvir')