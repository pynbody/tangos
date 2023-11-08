import sys
import time

import tangos
import tangos.testing.simulation_generator
from tangos import parallel_tasks as pt
from tangos import testing
from tangos.parallel_tasks import testing as pt_testing


def setup_module():
    pt.use("multiprocessing")
    testing.init_blank_db_for_testing(timeout=5, verbose=False)

    generator = tangos.testing.simulation_generator.SimulationGeneratorForTests()
    generator.add_timestep()
    generator.add_objects_to_timestep(9)

    tangos.core.get_default_session().commit()

def teardown_module():
    tangos.core.close_db()
    pt.use("multiprocessing-6")
    pt.launch(tangos.core.close_db)


def _add_property():
    for i in pt.distributed(list(range(1,10))):
        with pt.ExclusiveLock('insert', 0.05):
            tangos.get_halo(i)['my_test_property']=i
            tangos.core.get_default_session().commit()


def test_add_property():
    pt.use("multiprocessing-3")
    pt.launch(_add_property)
    for i in range(1,10):
        assert tangos.get_halo(i)['my_test_property']==i



def _add_two_properties_different_ranges():
    for i in pt.distributed(list(range(1,10))):
        with pt.ExclusiveLock('insert', 0.05):
            tangos.get_halo(i)['my_test_property_2']=i
            tangos.core.get_default_session().commit()

    for i in pt.distributed(list(range(1,8))):
        with pt.ExclusiveLock('insert', 0.05):
            tangos.get_halo(i)['my_test_property_3'] = i
            tangos.core.get_default_session().commit()

def test_add_two_properties_different_ranges():
    pt.use("multiprocessing-3")
    pt.launch(_add_two_properties_different_ranges)
    for i in range(1,10):
        assert tangos.get_halo(i)['my_test_property_2']==i
        if i<8:
            assert 'my_test_property_3' in tangos.get_halo(i)
            assert tangos.get_halo(i)['my_test_property_3'] == i
        else:
            assert 'my_test_property_3' not in tangos.get_halo(i)


def _test_not_run_twice():
    import time

    # For this test we want a staggered start
    time.sleep(pt.backend.rank()*0.05)

    for i in pt.distributed(list(range(3))):
        with pt.ExclusiveLock("lock"):
            tangos.get_halo(1)['test_count']+=1
            tangos.get_default_session().commit()

def test_for_loop_is_not_run_twice():
    """This test checks for an issue where if the number of CPUs exceeded the number of jobs for a task, the
    entire task could be run twice"""
    tangos.get_halo(1)['test_count'] = 0
    tangos.get_default_session().commit()
    pt.use("multiprocessing-5")
    pt.launch(_test_not_run_twice)
    assert tangos.get_halo(1)['test_count']==3



def _test_empty_loop():
    for _ in pt.distributed([]):
        assert False


def test_empty_loop():
    pt.use("multiprocessing-3")
    pt.launch(_test_empty_loop)

def _test_empty_then_non_empty_loop():
    for _ in pt.distributed([]):
        pass

    for _ in pt.distributed([1,2,3]):
        pass

def test_empty_then_non_empty_loop():
    pt.use("multiprocessing-3")
    pt.launch(_test_empty_then_non_empty_loop)


def _test_synchronize_db_creator():
    rank = pt.backend.rank()
    import tangos.parallel_tasks.database

    # hack: MultiProcessing backend forks so has already "synced" the current creator.
    tangos.core.creator._current_creator = None
    pt.database.synchronize_creator_object(tangos.core.get_default_session())
    with pt.ExclusiveLock('insert', 0.05):
        tangos.get_halo(rank)['db_creator_test_property'] = 1.0
    tangos.core.get_default_session().commit()

def test_synchronize_db_creator():
    pt.use("multiprocessing-3")
    pt.launch(_test_synchronize_db_creator)
    assert tangos.get_halo(1)['db_creator_test_property']==1.0
    assert tangos.get_halo(2)['db_creator_test_property'] == 1.0
    creator_1, creator_2 = (tangos.get_halo(i).get_objects('db_creator_test_property')[0].creator for i in (1,2))
    assert creator_1==creator_2



def _test_shared_locks():
    pt.backend.barrier()
    start_time = time.time()
    if pt.backend.rank()==1:
        # exclusive mode
        time.sleep(0.05) # make sure the exclusive lock isn't claimed before the first shared locks
        with pt.lock.ExclusiveLock("lock"):
            pt_testing.log("exclusive lock acquired")
            # should be running after the shared locks are done
    else:
        # shared mode
        with pt.lock.SharedLock("lock"):
            # should not have waited for the other shared locks
            pt_testing.log("shared lock acquired")
    pt.backend.barrier()

def _test_shared_locks_in_queue():
    start_time = time.time()
    if pt.backend.rank() <=2 :
        # two different processes going for the exclusive lock
        with pt.lock.ExclusiveLock("lock", 0):
            pt.backend.barrier()
            pt_testing.log("exclusive lock acquired")
            time.sleep(0.1)
            pt_testing.log("exclusive lock about to be released")
    else:
        # shared mode
        pt.backend.barrier() # make sure the exclusive locks get requested first
        with pt.lock.SharedLock("lock",0):
            # should be running after the exclusive locks are done
            pt_testing.log("shared lock acquired")
            time.sleep(0.1)
            pt_testing.log("shared lock about to be released")
        # should all have run in parallel
        assert time.time()-start_time<0.5
    pt.backend.barrier()

def test_shared_locks():
    pt_testing.initialise_log()
    pt.use("multiprocessing-4")
    pt.launch(_test_shared_locks)
    log = pt_testing.get_log()
    print("log:")
    print("".join(log))
    for i in range(2):
      assert log[i].strip() in ("[2] shared lock acquired", "[3] shared lock acquired")
    assert log[2].strip() == "[1] exclusive lock acquired"

def test_shared_locks_in_queue():
    pt_testing.initialise_log()
    pt.use("multiprocessing-6")
    pt.launch(_test_shared_locks_in_queue)
    log = pt_testing.get_log()
    print("log:")
    print("".join(log))
    for i in range(0,4,2):
        assert log[i].strip()[4:] == "exclusive lock acquired"
        assert log[i].strip()[:3] == log[i+1].strip()[:3]
        assert log[i+1].strip()[4:] == "exclusive lock about to be released"
    for i in range(4, 7):
        assert log[i].strip()[4:] == "shared lock acquired"
    for i in range(7, 10):
        assert log[i].strip()[4:] == "shared lock about to be released"
