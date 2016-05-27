import halo_db.core.dictionary
import halo_db.core.halo
import halo_db.core.halo_data
import halo_db.core.simulation
import halo_db.core.timestep
import halo_db

__author__ = 'app'

import halo_db as db
import halo_db.relation_finding as halo_finding
import halo_db.temporary_halolist as thl
import halo_db.testing as testing
from nose.tools import assert_raises

def setup():
    db.init_db("sqlite://")

    generator = testing.TestSimulationGenerator()
    # A second simulation to test linking across
    generator_2 = testing.TestSimulationGenerator("sim2")

    generator.add_timestep()
    generator_2.add_timestep()
    generator.add_halos_to_timestep(7)

    generator.add_timestep()
    generator_2.add_timestep()
    generator.add_halos_to_timestep(5)
    generator_2.add_halos_to_timestep(2)

    generator_2.link_last_halos_across_using_mapping(generator, {1:2, 2:1})

    # ts1_h1 becomes ts2_h2 but loses 10% of its mass to ts2_h1
    # ts1_h2 becomes ts2_h1
    # ts1_h3 becomes ts2_h3
    # ts1_h4 becomes ts2_h4 but loses 1% of its mass to ts2_h3
    # ts1_h6 and ts1_h7 merge into ts2_h5
    # ts1_h5 is ORPHANED, i.e has no counterpart in ts2
    #
    # We do not adjust the masses here because, despite the halos crossing
    # over in ordering, the tests were constructed with an earlier version
    # of the code which did not ensure this consistency
    generator.link_last_halos_using_mapping({1: 2,
                                             2: 1,
                                             3: 3,
                                             4: 4,
                                             6: 5,
                                             7: 5}, consistent_masses=False)

    generator.add_mass_transfer(1,1,0.1)
    generator.add_mass_transfer(4,3,0.01)

    generator.add_timestep()
    generator.add_halos_to_timestep(4)

    # ts2_h1 and ts2_h2 merge to ts3_h1
    # ts2_h3 becomes ts3_h2
    # ts2_h4 becomes ts3_h3 but loses 5% of its mass to ts3_h2
    # ts2_h5 becomes ts3_h4
    generator.link_last_halos_using_mapping({1: 1,
                                             2: 1,
                                             3: 2,
                                             4: 3,
                                             5: 4})
    generator.add_mass_transfer(4,2,0.05)








def test_ts_next():
    assert halo_db.get_item("sim/ts1").next == halo_db.get_item("sim/ts2")
    assert halo_db.get_item("sim/ts1").next.next == halo_db.get_item("sim/ts3")
    assert halo_db.get_item("sim/ts1").next.next.next is None

def test_ts_previous():
    assert halo_db.get_item("sim/ts3").previous == halo_db.get_item("sim/ts2")
    assert halo_db.get_item("sim/ts3").previous.previous == halo_db.get_item("sim/ts1")
    assert halo_db.get_item("sim/ts3").previous.previous.previous is None


def test_next():
    assert halo_db.get_item("sim/ts1/1").next == halo_db.get_item("sim/ts2/2")
    assert halo_db.get_item("sim/ts1/1").next.next == halo_db.get_item("sim/ts3/1")
    assert halo_db.get_item("sim/ts1/1").next.next.next is None

def test_previous():
    assert halo_db.get_item("sim/ts3/3").previous == halo_db.get_item("sim/ts2/4")
    assert halo_db.get_item("sim/ts3/3").previous.previous == halo_db.get_item("sim/ts1/4")
    assert halo_db.get_item("sim/ts3/3").previous.previous.previous is None

def test_previous_finds_major_progenitor():
    assert halo_db.get_item("sim/ts3/2").previous == halo_db.get_item("sim/ts2/3")


def test_simple_twostep_hop():
    strategy = halo_finding.MultiHopStrategy(halo_db.get_item("sim/ts3/3"), 2, 'backwards')
    assert strategy.count()==2
    all, weights = strategy.all_and_weights()

    assert halo_db.get_item("sim/ts1/4") in all
    assert halo_db.get_item("sim/ts2/4") in all
    assert weights[0]==1.0
    assert weights[1]==1.0

def test_twostep_ordering():
    strategy = halo_finding.MultiHopStrategy(halo_db.get_item("sim/ts3/3"), 2, 'backwards', order_by="time_asc")

    all = strategy.all()
    print all
    assert halo_db.get_item("sim/ts1/4") == all[0]
    assert halo_db.get_item("sim/ts2/4") == all[1]

    strategy = halo_finding.MultiHopStrategy(halo_db.get_item("sim/ts3/3"), 2, 'backwards', order_by="time_desc")
    all = strategy.all()
    assert halo_db.get_item("sim/ts2/4") == all[0]
    assert halo_db.get_item("sim/ts1/4") == all[1]

    strategy = halo_finding.MultiHopStrategy(halo_db.get_item("sim/ts3/1"), 2, 'backwards', order_by=["time_asc", "weight"])
    all, weights = strategy.all_and_weights()

    I = halo_db.get_item

    testing.assert_halolists_equal(all, [("sim/ts1/1"),
                                         ("sim/ts1/2"),
                                         # ("sim/ts1/1"), weaker route should NOT be returned by default
                                         ("sim/ts2/1"),
                                         ("sim/ts2/2")])


    #assert strategy.link_ids()==[[19,7], [18,9], [18],[19]]

    #assert strategy.node_ids()==[[9, 6, 1], [9, 5, 2], [9, 5], [9, 6]]


def test_twostep_multiroute():
    strategy = halo_finding.MultiHopStrategy(halo_db.get_item("sim/ts3/1"), 2, 'backwards', order_by=["time_asc", "weight"], combine_routes=False)
    all, weights = strategy.all_and_weights()

    I = halo_db.get_item

    assert all==[I("sim/ts1/1"),
                 I("sim/ts1/2"),
                 I("sim/ts1/1"), # route 2
                 I("sim/ts2/1"),
                 I("sim/ts2/2")]

    #assert strategy.link_ids()==[[19,7], [18,9], [18,8], [18],[19]]

    #assert strategy.node_ids()==[[9, 6, 1], [9, 5, 2], [9, 5, 1], [9, 5], [9, 6]]

def test_twostep_direction():
    strategy = halo_finding.MultiHopStrategy(halo_db.get_item("sim/ts2/1"), 2, 'backwards')
    timesteps = set([x.timestep for x in strategy.all()])
    assert halo_db.get_item("sim/ts1") in timesteps
    assert halo_db.get_item("sim/ts2") not in timesteps
    assert halo_db.get_item("sim/ts3") not in timesteps

def test_results_as_temptable():
    standard_results = halo_finding.MultiHopStrategy(halo_db.get_item("sim/ts2/1"), 2, 'backwards').all()
    with halo_finding.MultiHopStrategy(halo_db.get_item("sim/ts2/1"), 2, 'backwards').temp_table() as table:
        thl_results = thl.halo_query(table).all()

    assert standard_results==thl_results

def test_temptable_exceptions():
    strategy = halo_finding.MultiHopStrategy(halo_db.get_item("sim/ts2/1"), 2, 'backwards')

    class TestException(Exception):
        pass

    def raise_exception():
        raise TestException

    strategy._prepare_query = raise_exception
    with assert_raises(TestException):
        with strategy.temp_table() as table:
            assert False, "This point should not be reached"

def test_self_inclusion():
    # default: include_startpoint = False
    results = halo_finding.MultiHopStrategy(halo_db.get_item("sim/ts1/1"), 5, 'forwards').all()
    assert halo_db.get_item("sim/ts1/1") not in results

    results = halo_finding.MultiHopStrategy(halo_db.get_item("sim/ts1/1"), 5, 'forwards', include_startpoint=True).all()
    assert halo_db.get_item("sim/ts1/1") in results

def test_major_progenitors():
    results = halo_finding.MultiHopMajorProgenitorsStrategy(halo_db.get_item("sim/ts3/1"), include_startpoint=True).all()
    testing.assert_halolists_equal(results, ["sim/ts3/1","sim/ts2/1","sim/ts1/2"])

def test_major_descendants():
    results = halo_finding.MultiHopMajorDescendantsStrategy(halo_db.get_item("sim/ts1/2"), include_startpoint=True).all()
    testing.assert_halolists_equal(results, ["sim/ts1/2","sim/ts2/1","sim/ts3/1"])

def test_multisource():
    results = halo_finding.MultiSourceMultiHopStrategy(halo_db.get_items(["sim/ts1/1", "sim/ts1/3"]),
                                                       halo_db.get_item("sim/ts3")).all()
    testing.assert_halolists_equal(results,["sim/ts3/1","sim/ts3/2"])

def test_multisource_with_duplicates():
    results = halo_finding.MultiSourceMultiHopStrategy(halo_db.get_items(["sim/ts1/1", "sim/ts1/2", "sim/ts1/3"]),
                                                       halo_db.get_item("sim/ts3")).all()
    testing.assert_halolists_equal(results,["sim/ts3/1","sim/ts3/1","sim/ts3/2"])

def test_multisource_with_nones():
    strategy = halo_finding.MultiSourceMultiHopStrategy(
        halo_db.get_items(["sim/ts1/1", "sim/ts1/2", "sim/ts1/3", "sim/ts1/5"]),
        halo_db.get_item("sim/ts3"))
    results = strategy.all()
    testing.assert_halolists_equal(results,["sim/ts3/1","sim/ts3/1","sim/ts3/2",None])
    assert strategy._nhops_taken==2 # despite not finding a match for ts1/5, the strategy should halt after 2 steps

def test_multisource_with_nones_as_temptable():
    strategy = halo_finding.MultiSourceMultiHopStrategy(
        halo_db.get_items(["sim/ts1/1", "sim/ts1/2", "sim/ts1/3", "sim/ts1/5"]),
        halo_db.get_item("sim/ts3"))
    with strategy.temp_table() as table:
        results = thl.all_halos_with_duplicates(table)
    testing.assert_halolists_equal(results,["sim/ts3/1","sim/ts3/1","sim/ts3/2",None])

def test_multisource_backwards():
    results = halo_finding.MultiSourceMultiHopStrategy(halo_db.get_items(["sim/ts3/1", "sim/ts3/2", "sim/ts3/3"]),
                                                       halo_db.get_item("sim/ts1")).all()
    testing.assert_halolists_equal(results,["sim/ts1/1","sim/ts1/3","sim/ts1/4"])

def test_multisource_across():
    strategy = halo_finding.MultiSourceMultiHopStrategy(
        halo_db.get_items(["sim/ts2/1", "sim/ts2/2", "sim/ts2/3"]),
        halo_db.get_item("sim2"))
    results = strategy.all()
    testing.assert_halolists_equal(results, ["sim2/ts2/2", "sim2/ts2/1", None])
    assert strategy._nhops_taken==1


def test_find_merger():
    strategy = halo_finding.MultiHopMostRecentMergerStrategy(halo_db.get_item("sim/ts3/1"))
    results = strategy.all()
    testing.assert_halolists_equal(results, ["sim/ts2/1","sim/ts2/2"] )

    strategy = halo_finding.MultiHopMostRecentMergerStrategy(halo_db.get_item("sim/ts3/4"))
    results = strategy.all()
    testing.assert_halolists_equal(results, ["sim/ts1/6", "sim/ts1/7"])