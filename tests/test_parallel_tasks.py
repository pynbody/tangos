from __future__ import absolute_import

import tangos.testing.simulation_generator
from tangos import parallel_tasks as pt
from tangos import testing
import tangos
import sys
from six.moves import range

def setup():
    pt.use("multiprocessing")
    testing.init_blank_db_for_testing(timeout=5.0, verbose=False)

    generator = tangos.testing.simulation_generator.TestSimulationGenerator()
    generator.add_timestep()
    generator.add_objects_to_timestep(9)

    tangos.core.get_default_session().commit()


def _add_property():
    for i in pt.distributed(list(range(1,10))):
        tangos.get_halo(i)['my_test_property']=i
        tangos.core.get_default_session().commit()

def test_add_property():
    pt.launch(_add_property,3)
    for i in range(1,10):
        assert tangos.get_halo(i)['my_test_property']==i



def _add_two_properties_different_ranges():
    for i in pt.distributed(list(range(1,10))):
        tangos.get_halo(i)['my_test_property_2']=i
        tangos.core.get_default_session().commit()

    for i in pt.distributed(list(range(1,8))):
        tangos.get_halo(i)['my_test_property_3'] = i
        tangos.core.get_default_session().commit()

def test_add_two_properties_different_ranges():
    pt.launch(_add_two_properties_different_ranges,3)
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
        with pt.RLock("lock"):
            tangos.get_halo(1)['test_count']+=1
            tangos.get_default_session().commit()

def test_for_loop_is_not_run_twice():
    """This test checks for an issue where if the number of CPUs exceeded the number of jobs for a task, the
    entire task could be run twice"""
    tangos.get_halo(1)['test_count'] = 0
    tangos.get_default_session().commit()
    pt.launch(_test_not_run_twice, 5)
    assert tangos.get_halo(1)['test_count']==3



def _test_empty_loop():
    for _ in pt.distributed([]):
        assert False


def test_empty_loop():
    pt.launch(_test_empty_loop,3)

def _test_empty_then_non_empty_loop():
    for _ in pt.distributed([]):
        pass

    for _ in pt.distributed([1,2,3]):
        pass

def test_empty_then_non_empty_loop():
    pt.launch(_test_empty_then_non_empty_loop, 3)


def _test_synchronize_db_creator():
    rank = pt.backend.rank()
    import tangos.parallel_tasks.database
    # hack: MultiProcessing backend forks so has already "synced" the current creator.
    tangos.core.creator._current_creator = None
    pt.database.synchronize_creator_object(tangos.core.get_default_session())
    tangos.get_halo(rank)['db_creator_test_property'] = 1.0
    tangos.core.get_default_session().commit()

def test_synchronize_db_creator():
    pt.launch(_test_synchronize_db_creator,3)
    assert tangos.get_halo(1)['db_creator_test_property']==1.0
    assert tangos.get_halo(2)['db_creator_test_property'] == 1.0
    creator_1, creator_2 = [tangos.get_halo(i).get_objects('db_creator_test_property')[0].creator for i in (1,2)]
    assert creator_1==creator_2
