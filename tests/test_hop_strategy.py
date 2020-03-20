from __future__ import absolute_import
from __future__ import print_function
import tangos.core.dictionary
import tangos.core.halo
import tangos.core.halo_data
import tangos.core.simulation
import tangos.core.timestep
import tangos
import tangos.testing.simulation_generator

__author__ = 'app'

import tangos, tangos.live_calculation
import tangos.relation_finding as halo_finding
import tangos.temporary_halolist as thl
import tangos.testing as testing
from nose.tools import assert_raises

def setup():
    testing.init_blank_db_for_testing()

    generator = tangos.testing.simulation_generator.TestSimulationGenerator()
    # A second simulation to test linking across
    generator_2 = tangos.testing.simulation_generator.TestSimulationGenerator("sim2")

    generator.add_timestep()
    generator_2.add_timestep()
    generator.add_objects_to_timestep(7)
    generator.add_bhs_to_timestep(2)
    generator.assign_bhs_to_halos({1:1, 2:2})

    generator.add_timestep()
    generator_2.add_timestep()
    generator.add_objects_to_timestep(5)
    generator.add_bhs_to_timestep(2)
    generator.assign_bhs_to_halos({1: 1, 2: 2})
    generator_2.add_objects_to_timestep(2)

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
                                             7: 5}, adjust_masses=False)

    generator.add_mass_transfer(1,1,0.1)
    generator.add_mass_transfer(4,3,0.01)

    generator.add_timestep()
    generator.add_objects_to_timestep(5)
    generator.add_bhs_to_timestep(2)
    generator.assign_bhs_to_halos({1: 1, 2: 2})

    # ts2_h1 and ts2_h2 merge to ts3_h1
    # ts2_h3 becomes ts3_h2
    # ts2_h4 becomes ts3_h3 but loses 5% of its mass to ts3_h2
    # ts2_h5 becomes ts3_h4
    # ts3_h5 has no counterpart
    generator.link_last_halos_using_mapping({1: 1,
                                             2: 1,
                                             3: 2,
                                             4: 3,
                                             5: 4})
    generator.add_mass_transfer(4,2,0.05)






def test_ts_next():
    assert tangos.get_item("sim/ts1").next == tangos.get_item("sim/ts2")
    assert tangos.get_item("sim/ts1").next.next == tangos.get_item("sim/ts3")
    assert tangos.get_item("sim/ts1").next.next.next is None

def test_ts_previous():
    assert tangos.get_item("sim/ts3").previous == tangos.get_item("sim/ts2")
    assert tangos.get_item("sim/ts3").previous.previous == tangos.get_item("sim/ts1")
    assert tangos.get_item("sim/ts3").previous.previous.previous is None


def test_next():
    assert tangos.get_item("sim/ts1/1").next == tangos.get_item("sim/ts2/2")
    assert tangos.get_item("sim/ts1/1").next.next == tangos.get_item("sim/ts3/1")
    assert tangos.get_item("sim/ts1/1").next.next.next is None

def test_previous():
    assert tangos.get_item("sim/ts3/3").previous == tangos.get_item("sim/ts2/4")
    assert tangos.get_item("sim/ts3/3").previous.previous == tangos.get_item("sim/ts1/4")
    assert tangos.get_item("sim/ts3/3").previous.previous.previous is None

def test_previous_finds_major_progenitor():
    assert tangos.get_item("sim/ts3/2").previous == tangos.get_item("sim/ts2/3")


def test_simple_twostep_hop():
    strategy = halo_finding.MultiHopStrategy(tangos.get_item("sim/ts3/3"), 2, 'backwards')
    assert strategy.count()==2
    all, weights = strategy.all_and_weights()

    assert tangos.get_item("sim/ts1/4") in all
    assert tangos.get_item("sim/ts2/4") in all
    assert weights[0]==1.0
    assert weights[1]==1.0

def test_twostep_ordering():
    strategy = halo_finding.MultiHopStrategy(tangos.get_item("sim/ts3/3"), 2, 'backwards', order_by="time_asc")

    all = strategy.all()
    assert tangos.get_item("sim/ts1/4") == all[0]
    assert tangos.get_item("sim/ts2/4") == all[1]

    strategy = halo_finding.MultiHopStrategy(tangos.get_item("sim/ts3/3"), 2, 'backwards', order_by="time_desc")
    all = strategy.all()
    assert tangos.get_item("sim/ts2/4") == all[0]
    assert tangos.get_item("sim/ts1/4") == all[1]

    strategy = halo_finding.MultiHopStrategy(tangos.get_item("sim/ts3/1"), 2, 'backwards', order_by=["time_asc", "weight"])
    all, weights = strategy.all_and_weights()

    I = tangos.get_item

    testing.assert_halolists_equal(all, [("sim/ts1/1"),
                                         ("sim/ts1/2"),
                                         # ("sim/ts1/1"), weaker route should NOT be returned by default
                                         ("sim/ts2/1"),
                                         ("sim/ts2/2")])


    #assert strategy.link_ids()==[[19,7], [18,9], [18],[19]]

    #assert strategy.node_ids()==[[9, 6, 1], [9, 5, 2], [9, 5], [9, 6]]


def test_twostep_multiroute():
    strategy = halo_finding.MultiHopStrategy(tangos.get_item("sim/ts3/1"), 2, 'backwards', order_by=["time_asc", "weight"], combine_routes=False)
    all, weights = strategy.all_and_weights()

    I = tangos.get_item

    assert all==[I("sim/ts1/1"),
                 I("sim/ts1/2"),
                 I("sim/ts1/1"), # route 2
                 I("sim/ts2/1"),
                 I("sim/ts2/2")]

    #assert strategy.link_ids()==[[19,7], [18,9], [18,8], [18],[19]]

    #assert strategy.node_ids()==[[9, 6, 1], [9, 5, 2], [9, 5, 1], [9, 5], [9, 6]]

def test_twostep_direction():
    strategy = halo_finding.MultiHopStrategy(tangos.get_item("sim/ts2/1"), 2, 'backwards')
    timesteps = set([x.timestep for x in strategy.all()])
    assert tangos.get_item("sim/ts1") in timesteps
    assert tangos.get_item("sim/ts2") not in timesteps
    assert tangos.get_item("sim/ts3") not in timesteps

def test_results_as_temptable():
    standard_results = halo_finding.MultiHopStrategy(tangos.get_item("sim/ts2/1"), 2, 'backwards').all()
    with halo_finding.MultiHopStrategy(tangos.get_item("sim/ts2/1"), 2, 'backwards').temp_table() as table:
        thl_results = thl.halo_query(table).all()

    assert standard_results==thl_results

def test_temptable_exceptions():
    strategy = halo_finding.MultiHopStrategy(tangos.get_item("sim/ts2/1"), 2, 'backwards')

    class TestException(Exception):
        pass

    def raise_exception():
        raise TestException

    strategy._generate_multihop_results = raise_exception
    with assert_raises(TestException):
        with strategy.temp_table() as table:
            assert False, "This point should not be reached"

def test_self_inclusion():
    # default: include_startpoint = False
    results = halo_finding.MultiHopStrategy(tangos.get_item("sim/ts1/1"), 5, 'forwards').all()
    assert tangos.get_item("sim/ts1/1") not in results

    results = halo_finding.MultiHopStrategy(tangos.get_item("sim/ts1/1"), 5, 'forwards', include_startpoint=True).all()
    assert tangos.get_item("sim/ts1/1") in results

def test_major_progenitors():
    results = halo_finding.MultiHopMajorProgenitorsStrategy(tangos.get_item("sim/ts3/1"), include_startpoint=True).all()
    testing.assert_halolists_equal(results, ["sim/ts3/1","sim/ts2/1","sim/ts1/2"])

def test_major_descendants():
    results = halo_finding.MultiHopMajorDescendantsStrategy(tangos.get_item("sim/ts1/2"), include_startpoint=True).all()
    testing.assert_halolists_equal(results, ["sim/ts1/2","sim/ts2/1","sim/ts3/1"])


def test_across():
    results = halo_finding.MultiHopStrategy(tangos.get_item("sim/ts2/2"),directed='across').all()
    testing.assert_halolists_equal(results, ["sim2/ts2/1"])

def test_multisource():
    results = halo_finding.MultiSourceMultiHopStrategy(tangos.get_items(["sim/ts1/1", "sim/ts1/3"]),
                                                       tangos.get_item("sim/ts3")).all()
    testing.assert_halolists_equal(results,["sim/ts3/1","sim/ts3/2"])

def test_multisource_with_duplicates():
    results = halo_finding.MultiSourceMultiHopStrategy(tangos.get_items(["sim/ts1/1", "sim/ts1/2", "sim/ts1/3"]),
                                                       tangos.get_item("sim/ts3")).all()
    testing.assert_halolists_equal(results,["sim/ts3/1","sim/ts3/1","sim/ts3/2"])

def test_multisource_with_nones():
    strategy = halo_finding.MultiSourceMultiHopStrategy(
        tangos.get_items(["sim/ts1/1", "sim/ts1/2", "sim/ts1/3", "sim/ts1/5"]),
        tangos.get_item("sim/ts3"))
    results = strategy.all()
    testing.assert_halolists_equal(results,["sim/ts3/1","sim/ts3/1","sim/ts3/2",None])
    assert strategy._nhops_taken==2 # despite not finding a match for ts1/5, the strategy should halt after 2 steps

def test_multisource_with_nones_as_temptable():
    strategy = halo_finding.MultiSourceMultiHopStrategy(
        tangos.get_items(["sim/ts1/1", "sim/ts1/2", "sim/ts1/3", "sim/ts1/5"]),
        tangos.get_item("sim/ts3"))
    with strategy.temp_table() as table:
        results = thl.all_halos_with_duplicates(table)
    testing.assert_halolists_equal(results,["sim/ts3/1","sim/ts3/1","sim/ts3/2",None])

def test_multisource_preserves_order():
    strategy = halo_finding.MultiSourceMultiHopStrategy(
        tangos.get_items([ "sim/ts1/3", "sim/ts1/2", "sim/ts1/1", "sim/ts1/5"]),
        tangos.get_item("sim/ts3"))
    results = strategy.all()
    testing.assert_halolists_equal(results, ["sim/ts3/2", "sim/ts3/1", "sim/ts3/1",  None])

def test_multisource_backwards():
    sources = tangos.get_items(["sim/ts3/1", "sim/ts3/2", "sim/ts3/3"])
    results = halo_finding.MultiSourceMultiHopStrategy(sources,
                                                       tangos.get_item("sim/ts1")).all()

    testing.assert_halolists_equal(results,["sim/ts1/2","sim/ts1/3","sim/ts1/4"])

    single_earliest = [i.earliest for i in sources]
    testing.assert_halolists_equal(single_earliest, ["sim/ts1/2", "sim/ts1/3", "sim/ts1/4"])

def test_multisource_forwards():
    sources = tangos.get_items(["sim/ts1/1","sim/ts1/2","sim/ts1/3","sim/ts1/4"])
    results = halo_finding.MultiSourceMultiHopStrategy(sources,
                                                       tangos.get_item("sim/ts3")).all()
    testing.assert_halolists_equal(results, ["sim/ts3/1", "sim/ts3/1", "sim/ts3/2", "sim/ts3/3"])

    single_latest = [i.latest for i in sources]
    testing.assert_halolists_equal(single_latest, ["sim/ts3/1", "sim/ts3/1", "sim/ts3/2", "sim/ts3/3"])


def test_multisource_performance():
    ts_targ = tangos.get_item("sim/ts1")
    sources = tangos.get_items(["sim/ts3/1", "sim/ts3/2", "sim/ts3/3"])
    with testing.SqlExecutionTracker() as track:
        halo_finding.MultiSourceMultiHopStrategy(sources, ts_targ).all()

    assert track.count_statements_containing("select halos.")==0


def test_multisource_across():
    strategy = halo_finding.MultiSourceMultiHopStrategy(
        tangos.get_items(["sim/ts2/1", "sim/ts2/2", "sim/ts2/3"]),
        tangos.get_item("sim2"))
    results = strategy.all()
    testing.assert_halolists_equal(results, ["sim2/ts2/2", "sim2/ts2/1", None])
    assert strategy._nhops_taken==1

def test_find_merger():
    strategy = halo_finding.MultiHopMostRecentMergerStrategy(tangos.get_item("sim/ts3/1"))
    results = strategy.all()
    testing.assert_halolists_equal(results, ["sim/ts2/1","sim/ts2/2"] )

    strategy = halo_finding.MultiHopMostRecentMergerStrategy(tangos.get_item("sim/ts3/4"))
    results = strategy.all()
    testing.assert_halolists_equal(results, ["sim/ts1/6", "sim/ts1/7"])

def test_major_progenitor_from_minor_progenitor():
    generator = tangos.testing.simulation_generator.TestSimulationGenerator("sim3")
    ts1 = generator.add_timestep()
    generator.add_objects_to_timestep(4)
    ts2 = generator.add_timestep()
    generator.add_objects_to_timestep(3)
    generator.link_last_halos_using_mapping({1:2, 2:1, 3:3, 4:1}, adjust_masses=True)
    # ts1->ts2: most massive and second most massive halos swap rank ordering by mass because of the
    #           merger with ts1/h4.
    ts3 = generator.add_timestep()
    generator.add_objects_to_timestep(2)
    # ts2->ts3: there is a major merger of the most massive halos (ts2/h1+ts2/h2)->ts3/h1
    generator.link_last_halos_using_mapping({1:1, 2:1, 3:2}, adjust_masses=True)

    # Check major progenitor correctly reported one step back by MultiSourceMultiHopStrategy
    progen_in_ts2 = halo_finding.MultiSourceMultiHopStrategy(
        tangos.get_items(["sim3/ts3/1"]),
        tangos.get_item("sim3/ts2")).all()

    testing.assert_halolists_equal(progen_in_ts2,['sim3/ts2/1'])

    # Check major progenitor correctly reported two steps back by MultiHopMajorProgenitorsStrategy

    all_progenitors, weights = halo_finding.MultiHopMajorProgenitorsStrategy(tangos.get_item("sim3/ts3/1")).all_and_weights()

    testing.assert_halolists_equal(all_progenitors, ['sim3/ts2/1','sim3/ts1/2'])

    # Check major progenitor correctly reported two steps back by MultiSourceMultiHopStrategy
    # This is where a failure occurred in the past -- the behaviour was inequivalent to always choosing
    # the highest weight branch (which is what MultiHopMajorProgenitorsStrategy does).
    #
    # In the example constructed above, the mapping from ts3 halo 1 reaches:
    #  ts1, h4 (weight 0.26)
    #  ts1, h2 (weight 0.35)
    #  ts1, h1 (weight 0.39)
    #
    # It looks from these weights like we ought to be picking ts1/h1.
    #
    # However the correct major progenitor is h2, because one identifies a major progenitor at each step.
    # In step ts2, h1 has weight 0.61 and h2 has weight 0.39. So ts2/h1 is the major progenitor. And then,
    # going back to ts3, ts1/h2 (weight 0.57) is the major progenitor to ts2/h1.

    progen_in_ts1 = halo_finding.MultiSourceMultiHopStrategy(
        tangos.get_items(["sim3/ts3/1"]),
        tangos.get_item("sim3/ts1")).all()

    testing.assert_halolists_equal(progen_in_ts1, ['sim3/ts1/2'])

def test_offset_outputs_dont_confuse_match():
    # This tests for a bug where crosslinked timesteps at slightly different times could confuse the
    # search for a progentior or descendant because the recursive search strayed into a different simulation
    tangos.get_timestep("sim2/ts2").time_gyr*=1.01
    try:
        ts1_2_next = tangos.get_item("sim/ts2/2").next
        ts1_2_later = tangos.get_item("sim/ts2/2").calculate("later(1)")
        correct = ["sim/ts3/1"]
        testing.assert_halolists_equal([ts1_2_next], correct)
        testing.assert_halolists_equal([ts1_2_later], correct)
    finally:
        tangos.get_timestep("sim2/ts2").time_gyr /= 1.01

def test_earliest_no_ancestor():
    # Test that earliest returns self when there is not actually an ancestor to find
    no_ancestor = tangos.get_halo("sim/ts3/5")

    earliest = no_ancestor.earliest
    assert earliest==no_ancestor

