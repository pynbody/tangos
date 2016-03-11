from nose.tools import assert_raises

import halo_db as db
import halo_db.consistent_collection as cc

def setup():
    global sim1, sim2, collection, h1, h2
    db.init_db("sqlite://")

    session = db.core.internal_session

    sim1 = db.Simulation("sim1")
    sim2 = db.Simulation("sim2")
    ts1 = db.TimeStep(sim1, "ts", autoload=False)
    h1  = db.Halo(ts1, 1,1000,0,0,0)
    ts2 = db.TimeStep(sim2, "ts", autoload=False)
    h2  = db.Halo(ts2, 1,1000,0,0,0)

    session.add_all([sim1,sim2,ts1,ts2,h1,h2])


    sim1['consistent_property'] = 1.0
    sim2['consistent_property'] = 1.0

    sim1['inconsistent_property'] = 0.5
    sim2['inconsistent_property'] = 1.0
    collection = cc.ConsistentCollection([sim1, sim2])


def test_consistent():
    global collection
    assert collection['consistent_property'] == 1.0
    assert collection.get('consistent_property',2.0) == 1.0
    assert collection.get('nonexistent_property',2.0) == 2.0

def test_inconsistent():
    global collection
    with assert_raises(ValueError):
        collection['inconsistent_property']

    with assert_raises(ValueError):
        collection.get('inconsistent_property')

def test_set_pruning():
    col2 = cc.ConsistentCollection([sim1, sim1, sim2])
    assert len(col2._objects)==2

def test_generate_from_halos():
    col2 = cc.consistent_simulation_from_halos([h1,h2,h2])
    assert col2['consistent_property']==1.0