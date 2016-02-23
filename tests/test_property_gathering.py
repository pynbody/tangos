import halo_db as db
import numpy as np
import numpy.testing as npt
import properties
import sqlalchemy

def setup():
    db.init_db("sqlite://")

    session = db.core.internal_session

    sim = db.Simulation("sim")

    session.add(sim)

    ts1 = db.TimeStep(sim,"ts1",False)
    ts2 = db.TimeStep(sim,"ts2",False)
    ts3 = db.TimeStep(sim,"ts3",False)

    session.add_all([ts1,ts2,ts3])

    ts1.time_gyr = 1
    ts2.time_gyr = 2
    ts3.time_gyr = 3

    ts1.redshift = 10
    ts2.redshift = 5
    ts3.redshift = 0

    ts1_h1 = db.Halo(ts1,1,1000,0,0,0)
    ts1_h2 = db.Halo(ts1,2,900,0,0,0)
    ts1_h3 = db.Halo(ts1,3,800,0,0,0)
    ts1_h4 = db.Halo(ts1,4,300,0,0,0)

    session.add_all([ts1_h1,ts1_h2,ts1_h3,ts1_h4])






    ts2_h1 = db.Halo(ts2,1,1000,0,0,0)
    ts2_h2 = db.Halo(ts2,2,900,0,0,0)
    ts2_h3 = db.Halo(ts2,3,800,0,0,0)
    ts2_h4 = db.Halo(ts2,4,300,0,0,0)



    session.add_all([ts2_h1,ts2_h2,ts2_h3,ts2_h4])



    ts3_h1 = db.Halo(ts3,1,2000,0,0,0)
    ts3_h2 = db.Halo(ts3,2,800,0,0,0)
    ts3_h3 = db.Halo(ts3,3,300,0,0,0)

    session.add_all([ts3_h1,ts3_h2,ts3_h3])

    rel = db.get_or_create_dictionary_item(session, "ptcls_in_common")

    session.add_all([db.HaloLink(ts1_h1,ts2_h1,rel,1.0)])
    session.add_all([db.HaloLink(ts1_h2,ts2_h2,rel,1.0)])
    session.add_all([db.HaloLink(ts1_h3,ts2_h3,rel,1.0)])
    session.add_all([db.HaloLink(ts1_h4,ts2_h4,rel,1.0)])

    session.add_all([db.HaloLink(ts2_h1,ts3_h1,rel,1.0)])
    session.add_all([db.HaloLink(ts2_h2,ts3_h2,rel,1.0)])
    session.add_all([db.HaloLink(ts2_h3,ts3_h3,rel,1.0)])



    for i,h in enumerate([ts1_h1,ts1_h2,ts1_h3,ts1_h4,ts2_h1,ts2_h2,ts2_h3,ts2_h4,ts3_h1,ts3_h2,ts3_h3]):
        h['Mvir'] = float(i+1)
        h['Rvir'] = float(i+1)*0.1





    ts1_h1_bh = db.core.Halo(ts1,1,100,0,0,0)
    ts1_h2_bh = db.core.BH(ts1,2)
    ts1_h3_bh = db.core.BH(ts1,3)
    ts1_h3_bh2 = db.core.BH(ts1,4)


    session.add_all([ts1_h1_bh, ts1_h2_bh, ts1_h3_bh, ts1_h3_bh2])

    for i,h in enumerate([ts1_h1_bh, ts1_h2_bh, ts1_h3_bh, ts1_h3_bh2]):
        h['hole_mass'] = float(i+1)*100


    ts1_h1["BH"] = ts1_h1_bh
    ts1_h2["BH"] = ts1_h2_bh
    ts1_h3["BH"] = ts1_h3_bh, ts1_h3_bh2


    db.core.internal_session.commit()



class TestProperty(properties.HaloProperties):

    @classmethod
    def name(self):
        return "RvirPlusMvir"

    @classmethod
    def requires_simdata(self):
        return False

    def requires_property(self):
        return "Mvir", "Rvir"

    def calculate(self, halo, properties):
        return properties["Mvir"]+properties["Rvir"]

class TestErrorProperty(properties.HaloProperties):

    @classmethod
    def name(self):
        return "RvirPlusMvirMiscoded"

    @classmethod
    def requires_simdata(self):
        return False

    def requires_property(self):
        return "Mvir",

    def calculate(self, halo, properties):
        return properties["Mvir"]+properties["Rvir"]

class TestPropertyWithParameter(properties.HaloProperties):
    @classmethod
    def name(cls):
        return "squared"

    def __init__(self, value):
        self.value = value

    def calculate(self, halo, properties):
        return self.value**2


def test_gather_property():
    Mv,  = db.get_timestep("sim/ts2").gather_property("Mvir")
    npt.assert_allclose(Mv,[5,6,7,8])

    Mv, Rv  = db.get_timestep("sim/ts1").gather_property("Mvir", "Rvir")
    npt.assert_allclose(Mv,[1,2,3,4])
    npt.assert_allclose(Rv,[0.1,0.2,0.3,0.4])

def test_gather_function():

    Vv, = db.get_timestep("sim/ts1").gather_property("RvirPlusMvir()")
    npt.assert_allclose(Vv,[1.1,2.2,3.3,4.4])

    Vv, = db.get_timestep("sim/ts2").gather_property("RvirPlusMvir()")
    npt.assert_allclose(Vv,[5.5,6.6,7.7,8.8])


    with npt.assert_raises(KeyError):
        # The following should fail.
        # If it does not raise a keyerror, the live calculation has ignored the directive
        # to only load in the named properties.
        Vv, = db.get_timestep("sim/ts1").gather_property("RvirPlusMvirMiscoded()")

def test_gather_function_with_parameter():
    res, = db.get_timestep("sim/ts1").gather_property("squared(Mvir)")
    npt.assert_allclose(res, [1.0, 4.0, 9.0, 16.0])


def test_gather_linked_property():
    BH_mass, = db.get_timestep("sim/ts1").gather_property("BH.hole_mass")
    npt.assert_allclose(BH_mass, [100.,200.,300.])

    BH_mass, Mv = db.get_timestep("sim/ts1").gather_property("BH.hole_mass","Mvir")
    npt.assert_allclose(BH_mass, [100.,200.,300.])
    npt.assert_allclose(Mv, [1.,2.,3.])
