import numpy as np

import tangos
from tangos.relation_finding import tree
import tangos.live_calculation
import tangos.relation_finding as halo_finding
import tangos.temporary_halolist as thl
import tangos.testing as testing
import tangos.testing.simulation_generator as sg

def setup_module():




    #from tangos.config import testing_db_backend, testing_db_password, testing_db_user
    #db_url = f"{testing_db_backend}://{testing_db_user}:{testing_db_password}@localhost"
    #testing_db_name = "test_big_mergertree"
    #tangos.core.init_db(f"{db_url}/{testing_db_name}")

    is_blank = testing.init_blank_db_for_testing(erase_if_exists=False)

    if not is_blank:
        print("NOT GENERATING...")

    if is_blank:
        print("GENERATING...")

        generator = sg.SimulationGeneratorForTests()

        N_TIMESTEPS = 15
        N_HALOS_FINAL = 2
        N_BRANCHES_PER_TIMESTEP = 2
        MAX_HALOS = 10000

        n_halos_previous_timestep = None

        for i in range(N_TIMESTEPS):
            ts = generator.add_timestep()
            nhalos_this_timestep = min(N_HALOS_FINAL *
                                       N_BRANCHES_PER_TIMESTEP**(N_TIMESTEPS-i)
                                       , MAX_HALOS)
            print("TS", i, ts)
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
    print(results)
    testing.assert_halolists_equal(results, ["sim/ts3/1","sim/ts2/1","sim/ts1/1"])

def test_merger_tree():
    mt = tree.MergerTree(tangos.get_halo("%/ts15/1"))
    import time
    start = time.perf_counter()
    mt.construct()
    print("TIME=",time.perf_counter()-start)

    assert False

def manual_test():
    setup_module()
    test_merger_tree()

if __name__=="__main__":
    manual_test()
