__author__ = 'app'

import halo_db as db
import halo_db.relation_finding_strategies as halo_finding
import halo_db.temporary_halolist as thl
import halo_db.testing as testing
import os
import sqlalchemy, sqlalchemy.orm

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
    ts1_h5 = db.Halo(ts1,5,10,0,0,0) # intentional "orphan" halo with no progenitors for test_multisource

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

    # ts1_h1 becomes ts2_h2 but loses 10% of its mass to ts2_h1
    # ts1_h2 becomes ts2_h1
    # ts1_h3 becomes ts2_h3
    # ts1_h4 becomes ts2_h4 but loses 1% of its mass to ts2_h3
    session.add_all([db.HaloLink(ts1_h1,ts2_h2,rel,0.90),
                     db.HaloLink(ts1_h1,ts2_h1,rel,0.10),
                     db.HaloLink(ts1_h2,ts2_h1,rel,1.00),
                     db.HaloLink(ts1_h3,ts2_h3,rel,1.00),
                     db.HaloLink(ts1_h4,ts2_h4,rel,0.99),
                     db.HaloLink(ts1_h4,ts2_h3,rel,0.01)])

    session.add_all([db.HaloLink(ts2_h2,ts1_h1,rel,1.00),
                     db.HaloLink(ts2_h1,ts1_h1,rel,0.10),
                     db.HaloLink(ts2_h1,ts1_h2,rel,0.90),
                     db.HaloLink(ts2_h3,ts1_h3,rel,0.99),
                     db.HaloLink(ts2_h4,ts1_h4,rel,1.00),
                     db.HaloLink(ts2_h3,ts1_h4,rel,0.01)])

    # ts2_h1 and ts2_h2 merge to ts3_h1
    # ts2_h3 becomes ts3_h2
    # ts2_h4 becomes ts3_h3 but loses 5% of its mass to ts2_h2

    session.add_all([db.HaloLink(ts2_h1,ts3_h1,rel,1.00),
                     db.HaloLink(ts2_h2,ts3_h1,rel,1.00),
                     db.HaloLink(ts2_h3,ts3_h2,rel,1.00),
                     db.HaloLink(ts2_h4,ts3_h3,rel,0.9),
                     db.HaloLink(ts2_h4,ts3_h2,rel,0.1)])

    session.add_all([db.HaloLink(ts3_h1,ts2_h1,rel,950./(950+940)),
                     db.HaloLink(ts3_h1,ts2_h2,rel,940./(950+940)),
                     db.HaloLink(ts3_h3,ts2_h4,rel,1.00),
                     db.HaloLink(ts3_h2,ts2_h4,rel,0.05),
                     db.HaloLink(ts3_h2,ts2_h3,rel,0.95)])



    # A second simulation to test linking across
    sim2 = db.Simulation("sim2")
    session.add(sim2)
    s2_ts2 = db.TimeStep(sim2,"ts2",False)
    session.add(s2_ts2)
    s2_ts2.time_gyr = 2
    s2_ts2.redshift = 5

    s2_ts2_h1 = db.Halo(s2_ts2, 1, 1000, 0, 0, 0)
    s2_ts2_h2 = db.Halo(s2_ts2, 2, 500, 0, 0, 0)
    session.add_all([s2_ts2_h1, s2_ts2_h2])
    testing.add_symmetric_link(s2_ts2_h1, ts2_h2)
    testing.add_symmetric_link(s2_ts2_h2, ts2_h1)

    session.commit()


def test_get_halo():
    assert isinstance(db.get_item("sim/ts1/1"), db.Halo)
    assert db.get_item("sim/ts1/1").NDM==1000

def test_ts_next():
    assert db.get_item("sim/ts1").next == db.get_item("sim/ts2")
    assert db.get_item("sim/ts1").next.next == db.get_item("sim/ts3")
    assert db.get_item("sim/ts1").next.next.next is None

def test_ts_previous():
    assert db.get_item("sim/ts3").previous == db.get_item("sim/ts2")
    assert db.get_item("sim/ts3").previous.previous == db.get_item("sim/ts1")
    assert db.get_item("sim/ts3").previous.previous.previous is None


def test_next():
    assert db.get_item("sim/ts1/1").next == db.get_item("sim/ts2/2")
    assert db.get_item("sim/ts1/1").next.next == db.get_item("sim/ts3/1")
    assert db.get_item("sim/ts1/1").next.next.next is None

def test_previous():
    assert db.get_item("sim/ts3/3").previous == db.get_item("sim/ts2/4")
    assert db.get_item("sim/ts3/3").previous.previous == db.get_item("sim/ts1/4")
    assert db.get_item("sim/ts3/3").previous.previous.previous is None

def test_previous_finds_major_progenitor():
    assert db.get_item("sim/ts3/2").previous == db.get_item("sim/ts2/3")


def test_simple_twostep_hop():
    strategy = halo_finding.MultiHopStrategy(db.get_item("sim/ts3/3"), 2, 'backwards')
    assert strategy.count()==2
    all, weights = strategy.all_and_weights()

    assert db.get_item("sim/ts1/4") in all
    assert db.get_item("sim/ts2/4") in all
    assert weights[0]==1.0
    assert weights[1]==1.0

def test_twostep_ordering():
    strategy = halo_finding.MultiHopStrategy(db.get_item("sim/ts3/3"), 2, 'backwards', order_by="time_asc")

    all = strategy.all()
    print all
    assert db.get_item("sim/ts1/4")==all[0]
    assert db.get_item("sim/ts2/4")==all[1]

    strategy = halo_finding.MultiHopStrategy(db.get_item("sim/ts3/3"), 2, 'backwards', order_by="time_desc")
    all = strategy.all()
    assert db.get_item("sim/ts2/4")==all[0]
    assert db.get_item("sim/ts1/4")==all[1]

    strategy = halo_finding.MultiHopStrategy(db.get_item("sim/ts3/1"), 2, 'backwards', order_by=["time_asc", "weight"])
    all, weights = strategy.all_and_weights()

    I = db.get_item

    assert all==[I("sim/ts1/1"),
                 I("sim/ts1/2"),
                 # I("sim/ts1/1"), weaker route should NOT be returned by default
                 I("sim/ts2/1"),
                 I("sim/ts2/2")]

    #assert strategy.link_ids()==[[19,7], [18,9], [18],[19]]

    #assert strategy.node_ids()==[[9, 6, 1], [9, 5, 2], [9, 5], [9, 6]]


def test_twostep_multiroute():
    strategy = halo_finding.MultiHopStrategy(db.get_item("sim/ts3/1"), 2, 'backwards', order_by=["time_asc", "weight"], combine_routes=False)
    all, weights = strategy.all_and_weights()

    I = db.get_item

    assert all==[I("sim/ts1/1"),
                 I("sim/ts1/2"),
                 I("sim/ts1/1"), # route 2
                 I("sim/ts2/1"),
                 I("sim/ts2/2")]

    #assert strategy.link_ids()==[[19,7], [18,9], [18,8], [18],[19]]

    #assert strategy.node_ids()==[[9, 6, 1], [9, 5, 2], [9, 5, 1], [9, 5], [9, 6]]

def test_twostep_direction():
    strategy = halo_finding.MultiHopStrategy(db.get_item("sim/ts2/1"), 2, 'backwards')
    timesteps = set([x.timestep for x in strategy.all()])
    assert db.get_item("sim/ts1") in timesteps
    assert db.get_item("sim/ts2") not in timesteps
    assert db.get_item("sim/ts3") not in timesteps

def test_results_as_temptable():
    standard_results = halo_finding.MultiHopStrategy(db.get_item("sim/ts2/1"), 2, 'backwards').all()
    with halo_finding.MultiHopStrategy(db.get_item("sim/ts2/1"), 2, 'backwards').temp_table() as table:
        thl_results = thl.halo_query(table).all()

    assert standard_results==thl_results

def test_self_inclusion():
    # default: include_startpoint = False
    results = halo_finding.MultiHopStrategy(db.get_item("sim/ts1/1"), 5, 'forwards').all()
    assert db.get_item("sim/ts1/1") not in results

    results = halo_finding.MultiHopStrategy(db.get_item("sim/ts1/1"), 5, 'forwards', include_startpoint=True).all()
    assert db.get_item("sim/ts1/1") in results

def test_major_progenitors():
    results = halo_finding.MultiHopMajorProgenitorsStrategy(db.get_item("sim/ts3/1"),include_startpoint=True).all()
    testing.assert_halolists_equal(results, ["sim/ts3/1","sim/ts2/1","sim/ts1/2"])

def test_major_descendants():
    results = halo_finding.MultiHopMajorDescendantsStrategy(db.get_item("sim/ts1/2"),include_startpoint=True).all()
    testing.assert_halolists_equal(results, ["sim/ts1/2","sim/ts2/1","sim/ts3/1"])

def test_multisource():
    results = halo_finding.MultiSourceMultiHopStrategy(db.core.get_items(["sim/ts1/1","sim/ts1/3"]),
                                                       db.core.get_item("sim/ts3")).all()
    testing.assert_halolists_equal(results,["sim/ts3/1","sim/ts3/2"])

def test_multisource_with_duplicates():
    results = halo_finding.MultiSourceMultiHopStrategy(db.core.get_items(["sim/ts1/1","sim/ts1/2","sim/ts1/3"]),
                                                       db.core.get_item("sim/ts3")).all()
    testing.assert_halolists_equal(results,["sim/ts3/1","sim/ts3/1","sim/ts3/2"])

def test_multisource_with_nones():
    strategy = halo_finding.MultiSourceMultiHopStrategy(db.core.get_items(["sim/ts1/1","sim/ts1/2","sim/ts1/3","sim/ts1/5"]),
                                                       db.core.get_item("sim/ts3"))
    results = strategy.all()
    testing.assert_halolists_equal(results,["sim/ts3/1","sim/ts3/1","sim/ts3/2",None])
    assert strategy._nhops_taken==2 # despite not finding a match for ts1/5, the strategy should halt after 2 steps

def test_multisource_with_nones_as_temptable():
    strategy = halo_finding.MultiSourceMultiHopStrategy(db.core.get_items(["sim/ts1/1","sim/ts1/2","sim/ts1/3","sim/ts1/5"]),
                                                     db.core.get_item("sim/ts3"))
    with strategy.temp_table() as table:
        results = thl.all_halos_with_duplicates(table)
    testing.assert_halolists_equal(results,["sim/ts3/1","sim/ts3/1","sim/ts3/2",None])

def test_multisource_backwards():
    results = halo_finding.MultiSourceMultiHopStrategy(db.core.get_items(["sim/ts3/1","sim/ts3/2","sim/ts3/3"]),
                                                       db.core.get_item("sim/ts1")).all()
    testing.assert_halolists_equal(results,["sim/ts1/1","sim/ts1/3","sim/ts1/4"])

def test_multisource_across():
    strategy = halo_finding.MultiSourceMultiHopStrategy(db.core.get_items(["sim/ts2/1","sim/ts2/2","sim/ts2/3"]),
                                                       db.core.get_item("sim2"))
    results = strategy.all()
    testing.assert_halolists_equal(results, ["sim2/ts2/2", "sim2/ts2/1", None])
    assert strategy._nhops_taken==1