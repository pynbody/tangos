"""This test is for a specific merger scenario where a halo partially merges (i.e. is heavily stripped)
but survives; and then the remnant potentially merges later on.

Further difficult scenarios could be added here in future and tested against various tools"""

from __future__ import absolute_import
from __future__ import print_function
import tangos.core.dictionary
import tangos.core.halo
import tangos.core.halo_data
import tangos.core.simulation
import tangos.core.timestep
import tangos
import tangos.testing.simulation_generator

from tangos.relation_finding import tree
import tangos.testing as testing
import numpy as np

def setup():
    testing.init_blank_db_for_testing()

    generator = tangos.testing.simulation_generator.TestSimulationGenerator()
    generator.add_timestep() # ts1
    generator.add_objects_to_timestep(2, NDM=[1000,600])

    generator.add_timestep() # ts2
    generator.add_objects_to_timestep(2)
    generator.link_last_halos(adjust_masses=True)

    generator.add_timestep() # ts3, in which halo 1 and 2 collide
    generator.add_objects_to_timestep(2)
    generator.link_last_halos_using_mapping({1:2, 2:1}, adjust_masses=True) # halo 1 becomes 2, 2 becomes 1 due to mass ordering
    generator.add_mass_transfer(1,1,0.3, adjust_masses=True) # halo 1 in previous step loses 30% of its mass to halo 1 in this step... but survives

    generator.add_timestep() # ts4
    generator.add_objects_to_timestep(2)
    generator.link_last_halos(adjust_masses=True)
    generator.add_mass_transfer(2,1,0.05, adjust_masses=True) # continued minor stripping of halo 2

    generator.add_timestep() #ts5, in which halo 1 and 2 finally merge completely
    generator.add_objects_to_timestep(1)
    generator.link_last_halos_using_mapping({1:1, 2:1}, adjust_masses=True)

def test_setup():
    ndm_test = [[x.NDM for x in tangos.get_timestep(ts).halos] for ts in range(1,6)]
    assert np.all(ndm_test[0] == [1000, 600])
    assert np.all(ndm_test[1] == [1000, 600])
    assert np.all(ndm_test[2] == [900, 700])
    assert np.all(ndm_test[3] == [935, 665])
    assert np.all(ndm_test[4] == [1600])

def test_major_progenitor_branch():
    halo_num, = tangos.get_halo("sim/ts5/1").calculate_for_progenitors("halo_number()")
    assert np.all(halo_num==[1,1,1,2,2])

def test_major_progenitor_branch_from_merger_example():
    import tangos.examples.mergers as mer
    z_arr, ratio_arr, halo_arr = mer.get_mergers_of_major_progenitor(tangos.get_halo("sim/ts5/1"))
    # the mergers example will see two major mergers happen - hard to avoid this without a lot of ad hoc rules
    # Note also the major progenitor in the high-z collision is halo 2, not halo 1 (despite halo 1 being more massive).
    # This is because most of the material in the remnant comes from halo 2.
    testing.assert_halolists_equal(halo_arr, [("sim/ts4/1", "sim/ts4/2"), ("sim/ts2/2", "sim/ts2/1")])

def test_tree_from_ts5():
    t = tree.MergerTree(tangos.get_halo("sim/ts5/1"))
    t.construct()
    # should 'see' the final merger
    assert t.summarise()=="1(1(1(2(2))),2(2(1(1))))"

def test_tree_from_ts4():
    t = tree.MergerTree(tangos.get_halo("sim/ts4/1"))
    t.construct()
    # should now 'see' the earlier partial merger, but not the 5% mass transfer
    assert t.summarise()=="1(1(1(1),2(2)))"
