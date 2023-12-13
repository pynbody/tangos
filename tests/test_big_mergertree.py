import numpy as np

import tangos
import tangos.live_calculation
import tangos.relation_finding as halo_finding
import tangos.temporary_halolist as thl
import tangos.testing as testing
import tangos.testing.simulation_generator as sg
from tangos.relation_finding import tree


def setup_module():
    is_blank = testing.init_blank_db_for_testing(erase_if_exists=False)
    # Creating this test database is a bit time-consuming, so if it already
    # exists we assume it's OK to use it. If you want to force a rebuild,
    # drop or delete the database and re-run the tests. (Or use the
    # erase_if_exists=True option above.)

    if is_blank:
        N_TIMESTEPS = 15
        N_HALOS_FINAL = 2
        N_BRANCHES_PER_TIMESTEP = 2
        MAX_HALOS = 10000

        generator = sg.SimulationGeneratorForTests(max_steps=N_TIMESTEPS)

        n_halos_previous_timestep = None

        for i in range(N_TIMESTEPS):
            ts = generator.add_timestep()
            nhalos_this_timestep = min(N_HALOS_FINAL *
                                       N_BRANCHES_PER_TIMESTEP**(N_TIMESTEPS-i)
                                       , MAX_HALOS)
            generator.add_objects_to_timestep(nhalos_this_timestep, NDM=np.arange(1,nhalos_this_timestep+1)[::-1])
            if n_halos_previous_timestep is not None:
                assert nhalos_this_timestep <= n_halos_previous_timestep
                halomap = {}
                for j in range(N_BRANCHES_PER_TIMESTEP):
                    halomap.update({i+j*nhalos_this_timestep:i for i in range(1,nhalos_this_timestep+1)
                                    if i+j*nhalos_this_timestep<n_halos_previous_timestep})
                generator.link_last_halos_using_mapping(halomap)

            n_halos_previous_timestep = nhalos_this_timestep


def test_major_progenitors():
    results = halo_finding.MultiHopMajorProgenitorsStrategy(tangos.get_item("sim/ts3/1"), include_startpoint=True).all()

    testing.assert_halolists_equal(results, ["sim/ts3/1","sim/ts2/1","sim/ts1/1"])

def test_merger_tree():
    mt = tree.MergerTree(tangos.get_halo("%/ts15/1"))
    mt.construct()


    assert mt.summarise() == ("1(1(1(1(1(1(1(1(1(1(1(1(1(1(1))))))))),65(65(65(65(65(65(65(65(65(65)))))))))),"
                            "33(33(33(33(33(33(33(33(33(33(33))))))))),97(97(97(97(97(97(97(97(97(97))))))))))),"
                            "17(17(17(17(17(17(17(17(17(17(17(17))))))))),81(81(81(81(81(81(81(81(81(81)))))))))),"
                            "49(49(49(49(49(49(49(49(49(49(49))))))))),113(113(113(113(113(113(113(113(113(113)))))))))))),"
                            "9(9(9(9(9(9(9(9(9(9(9(9(9))))))))),73(73(73(73(73(73(73(73(73(73)))))))))),"
                            "41(41(41(41(41(41(41(41(41(41(41))))))))),105(105(105(105(105(105(105(105(105(105))))))))))),"
                            "25(25(25(25(25(25(25(25(25(25(25(25))))))))),89(89(89(89(89(89(89(89(89(89)))))))))),"
                            "57(57(57(57(57(57(57(57(57(57(57))))))))))))),5(5(5(5(5(5(5(5(5(5(5(5(5(5))))))))),"
                            "69(69(69(69(69(69(69(69(69(69)))))))))),37(37(37(37(37(37(37(37(37(37(37))))))))),"
                            "101(101(101(101(101(101(101(101(101(101))))))))))),21(21(21(21(21(21(21(21(21(21(21(21))))))))),"
                            "85(85(85(85(85(85(85(85(85(85)))))))))),53(53(53(53(53(53(53(53(53(53(53)))))))))))),"
                            "13(13(13(13(13(13(13(13(13(13(13(13(13))))))))),77(77(77(77(77(77(77(77(77(77)))))))))),"
                            "45(45(45(45(45(45(45(45(45(45(45))))))))),109(109(109(109(109(109(109(109(109(109))))))))))),"
                            "29(29(29(29(29(29(29(29(29(29(29(29))))))))),93(93(93(93(93(93(93(93(93(93)))))))))),"
                            "61(61(61(61(61(61(61(61(61(61(61))))))))))))))")


def manual_test():
    setup_module()
    import time
    start = time.perf_counter()
    test_merger_tree()
    print("Time taken = ", time.perf_counter() - start)

if __name__=="__main__":
    manual_test()
