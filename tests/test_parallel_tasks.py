import os
import time

import pytest

import tangos
import tangos.testing.simulation_generator
from tangos import parallel_tasks as pt, testing
from tangos.log import logger
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


def _test_barrier():
    if pt.backend.rank()==1:
        # only sleep on one process, to check barrier works
        time.sleep(0.3)
    pt_testing.log("Before barrier")
    pt.barrier()
    pt_testing.log("After barrier")

def test_barrier():
    pt.use("multiprocessing-3")
    pt_testing.initialise_log()
    pt.launch(_test_barrier)
    log = pt_testing.get_log(remove_process_ids=True)
    assert log == ["Before barrier"]*2+["After barrier"]*2




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


def _test_loops_different_length():
    for i in pt.distributed(list(range(pt.backend.rank()*10))):
        pass

def _test_loops_different_backtrace():
    if pt.backend.rank()==1:
        for i in pt.distributed(list(range(10))):
            pass
    else:
        for i in pt.distributed(list(range(10))):
            pass
def test_inconsistent_loops_rejected():
    pt.use("multiprocessing-3")
    with pytest.raises(pt.jobs.InconsistentJobList):
        pt.launch(_test_loops_different_length)

    with pytest.raises(pt.jobs.InconsistentContext):
        pt.launch(_test_loops_different_backtrace)


def _test_synchronized_loop():
    for i in pt.synchronized(list(range(10))):
        pt_testing.log(f"Doing task {i}")
        pass

def test_synchronized_loop():
    pt.use('multiprocessing-3')
    pt_testing.initialise_log()
    pt.launch(_test_synchronized_loop)
    log = pt_testing.get_log()
    assert len(log) == 20
    for i in range(10):
        for r in (1,2):
            assert log.count(f"[{r}] Doing task {i}") == 1


def _test_resume_loop(attempt, mode='distributed'):
    if mode=='distributed':
        iterator = pt.distributed(list(range(10)), allow_resume=True, resumption_id=1)
        # must provide a resumption_id because when we resume the stack trace is different
    elif mode=='synchronized':
        iterator = pt.synchronized(list(range(10)), allow_resume=True, resumption_id=2)
    else:
        raise ValueError("Unknown test mode")

    for i in iterator:
        pt_testing.log(f"Start job {i}")
        pt.barrier() # make sure start is logged before kicking up a fuss

        if i==5 and attempt==0:
            raise ValueError("Suspend processing")

        pt.barrier()
        pt_testing.log(f"Finish job {i}")



@pytest.mark.parametrize("mode", ("distributed", "synchronized"))
def test_resume_loop(mode):
    pt.use("multiprocessing-3")

    pt.jobs.IterationState.clear_resume_state()

    pt_testing.initialise_log()

    with pytest.raises(ValueError):
        pt.launch(_test_resume_loop, args=(0, mode))

    lines = pt_testing.get_log(remove_process_ids=True)

    expected_when_distributed = [
        (f"Start job {i}", 1) for i in range(6)
    ] + [
        (f"Finish job {i}", 1) for i in range(4)
    ] + [
        (f"Finish job {i}", 0) for i in range(4,6)
    ]

    expected_when_synchronized = [
        (f"Start job {i}", 2) for i in range(6)
    ] + [
        (f"Finish job {i}", 2) for i in range(5)
    ] + [
        (f"Finish job 5", 0)
    ]

    expected = expected_when_distributed if mode=='distributed' else expected_when_synchronized

    for line, count in expected:
        assert lines.count(line) == count

    pt_testing.initialise_log()

    pt.launch(_test_resume_loop, args=(1, mode))

    lines = pt_testing.get_log(remove_process_ids=True)

    expected_when_distributed = [
                                    (f"Start job {i}", 1) for i in range(4, 10)
                                ] + [
                                    (f"Finish job {i}", 1) for i in range(4, 10)
                                ]  + [
                                    (f"Start job {i}", 0) for i in range(4)
                                ]

    expected_when_synchronized = [
                                     (f"Start job {i}", 2) for i in range(5, 10)
                                 ] + [
                                     (f"Finish job {i}", 2) for i in range(5, 10)
                                 ] + [
                                    (f"Start job {i}", 0) for i in range(5)
                                 ]

    expected = expected_when_distributed if mode == 'distributed' else expected_when_synchronized

    for line, count in expected:
        assert lines.count(line) == count



def _test_empty_loop(mode):
    if mode=='distributed':
        for _ in pt.distributed([]):
            assert False
    elif mode=='synchronized':
        for _ in pt.synchronized([]):
            assert False
    else:
        raise ValueError("Unknown test mode")

@pytest.mark.parametrize("mode", ("distributed", "synchronized"))
def test_empty_loop(mode):
    pt.use("multiprocessing-3")
    pt.launch(_test_empty_loop, args=(mode,))

def _test_empty_then_non_empty_loop(mode):
    if mode=='distributed':
        iterator = pt.distributed
    elif mode=='synchronized':
        iterator = pt.synchronized
    else:
        raise ValueError("Unknown test mode")

    for _ in iterator([]):
        pt_testing.log(f"Should not appear")

    for i in iterator([1,2,3]):
        pt_testing.log(f"Doing task {i}")

@pytest.mark.parametrize("mode", ("distributed", "synchronized"))
def test_empty_then_non_empty_loop(mode):
    pt.use("multiprocessing-3")
    pt_testing.initialise_log()
    pt.launch(_test_empty_then_non_empty_loop, args=(mode,))
    log = pt_testing.get_log(remove_process_ids=True)
    if mode=='distributed':
        assert len(log)==3
        assert "Doing task 3" in log
        assert "Should not appear" not in log
    elif mode=='synchronized':
        assert len(log)==6
        assert log.count("Doing task 3")==2
        assert "Should not appear" not in log


def _test_overtaking_synchronized_loop():
    for i in pt.synchronized([0,1,2]):
        pt_testing.log(f"Doing task {i}")
        if pt.backend.rank()==1:
            time.sleep(0.02)

def test_overtaking_synchronized_loop():
    # test that iterations stay synchronized even if one process tries to overtake the other
    pt.use("multiprocessing-3")
    pt_testing.initialise_log()
    pt.launch(_test_overtaking_synchronized_loop)
    log = pt_testing.get_log()
    assert len(log)==6

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


def _test_nested_loop():
    for i in pt.synchronized(list(range(3))):
        for j in pt.distributed(list(range(3))):
            pt_testing.log(f"Task {i},{j}")

def test_nested_loop():
    pt.use("multiprocessing-3")
    pt_testing.initialise_log()
    pt.launch(_test_nested_loop)

    log = pt_testing.get_log(remove_process_ids=True)

    for i in range(3):
        for j in range(3):
            assert log.count(f"Task {i},{j}")==1



def _test_shared_locks():
    if pt.backend.rank()==1:
        # exclusive mode
        pt.barrier() # make sure the exclusive lock isn't claimed before the first shared locks
        with pt.lock.ExclusiveLock("lock"):
            pt_testing.log("exclusive lock acquired")
            # should be running after the shared locks are done
    else:
        # shared mode
        with pt.lock.SharedLock("lock"):
            # should not have waited for the other shared locks
            pt_testing.log("shared lock acquired")
            pt.barrier()
    pt.barrier()

def _test_shared_locks_in_queue():
    start_time = time.time()
    if pt.backend.rank() <=2 :
        # two different processes going for the exclusive lock
        with pt.lock.ExclusiveLock("lock", 0):
            pt_testing.log("exclusive lock acquired")
            time.sleep(0.1)
            pt_testing.log("exclusive lock about to be released")
    else:
        # shared mode
        with pt.lock.SharedLock("lock",0):
            # should be running after the exclusive locks are done
            pt_testing.log("shared lock acquired")
            time.sleep(0.1)
            pt_testing.log("shared lock about to be released")
        # should all have run in parallel
        assert time.time()-start_time<0.5
    pt.barrier()

def test_shared_locks():
    pt_testing.initialise_log()
    pt.use("multiprocessing-4")
    pt.launch(_test_shared_locks)
    log = pt_testing.get_log()
    for i in range(2):
      assert log[i].strip() in ("[2] shared lock acquired", "[3] shared lock acquired")
    assert log[2].strip() == "[1] exclusive lock acquired"

def test_shared_locks_in_queue():
    pt_testing.initialise_log()
    pt.use("multiprocessing-6")
    pt.launch(_test_shared_locks_in_queue)
    log = pt_testing.get_log()


    # we want to verify that shared locks were held simultaneously, but exclusive locks never were
    lock_held = 0
    for line in log:
        if "exclusive lock acquired" in line:
            assert lock_held==0
            lock_held = 'exclusive'
        elif "shared lock acquired" in line:
            assert isinstance(lock_held, int)
            lock_held += 1
        elif "exclusive lock about to be released" in line:
            assert lock_held=='exclusive'
            lock_held = 0
        elif "shared lock about to be released" in line:
            assert isinstance(lock_held, int)
            lock_held-=1
        else:
            assert False, "Unexpected line in log: "+line

class ErrorOnServer(pt.message.Message):
    def process(self):
        raise RuntimeError("Error on server")

def test_error_on_server():
    pt.use("multiprocessing-2")
    with pytest.raises(RuntimeError) as e:
        pt.launch(lambda: ErrorOnServer().send(0))
    assert "Error on server" in str(e.value)

def test_error_on_client():
    pt.use("multiprocessing-2")
    def _error_on_client():
        raise RuntimeError("Error on client")

    with pytest.raises(RuntimeError) as e:
        pt.launch(_error_on_client)
    assert "Error on client" in str(e.value)

def _test_remote_set():
    set = pt.shared_set.SharedSet("test_set", allow_parallel=True)
    assert isinstance(set, pt.shared_set.RemoteSet)
    if pt.backend.rank()==1:
        result = set.add_if_not_exists("foo")
        assert not result
        pt.barrier()
        pt.barrier()
        result = set.add_if_not_exists("bar")
        assert result
        set2 = pt.shared_set.SharedSet("test_set2")
        result = set2.add_if_not_exists("foo")
        assert not result
    elif pt.backend.rank()==2:
        pt.barrier()
        result = set.add_if_not_exists("foo")
        assert result
        result = set.add_if_not_exists("bar")
        assert not result
        pt.barrier()


def test_remote_set():
    pt.use("multiprocessing-3")
    pt.launch(_test_remote_set)

def test_local_set():
    set = pt.shared_set.SharedSet("test_local_set")
    assert not set.add_if_not_exists("foo")
    assert set.add_if_not_exists("foo")

    set = pt.shared_set.SharedSet("test_local_set")
    assert set.add_if_not_exists("foo")


class MessageTestServerLock(pt.message.Message):
    """Message to test server-side lock acquisition"""
    def __init__(self, lock_name, delay):
        self.lock_name = lock_name
        self.delay = delay
    
    @classmethod
    def deserialize(cls, source, message):
        obj = cls(*message)
        obj.source = source
        return obj
    
    def serialize(self):
        return (self.lock_name, self.delay)
    
    def process(self):
        import threading
        import time
        
        def run_server_test():
            time.sleep(self.delay)
            pt_testing.log("Server requesting exclusive lock")
            with pt.ExclusiveLock(self.lock_name, 0):
                pt_testing.log("Server acquired exclusive lock")
                time.sleep(0.1)
                pt_testing.log("Server releasing exclusive lock")
        
        # Run the test in a separate thread so server can continue processing messages
        thread = threading.Thread(target=run_server_test)
        thread.start()


def _test_server_exclusive_lock(server_first):
    """Test server exclusive lock acquisition with different orderings"""
    rank = pt.backend.rank()
    server_delay = 0.0 if server_first else 0.01
    if rank == 1:
        # Server gets lock first
        MessageTestServerLock('server_exclusive_test', server_delay).send(0)

    if server_first:
        time.sleep(0.02*rank)
    else:
        time.sleep(0.02*(rank-1))

    # Now client tries to get the same lock
    pt_testing.log("Client requesting exclusive lock")
    with pt.ExclusiveLock('server_exclusive_test', 0):
        pt_testing.log("Client acquired exclusive lock")
        time.sleep(0.05)
        pt_testing.log("Client releasing exclusive lock")
    time.sleep(0.1)


@pytest.mark.parametrize("server_first", [True, False])
def test_server_exclusive_lock(server_first):
    """Test that server can acquire exclusive locks directly in different orderings"""
    nclients = 3
    pt.use(f"multiprocessing-{nclients+1}")
    pt_testing.initialise_log()
    pt.launch(_test_server_exclusive_lock, args=(server_first,))
    log = pt_testing.get_log()
    
    # Check that both server and client acquired locks
    server_acquired = None
    client_acquired = None
    for i, line in enumerate(log):
        if "Server acquired exclusive lock" in line:
            server_acquired = i
        elif "Client acquired exclusive lock" in line and client_acquired is None:
            client_acquired = i
    
    assert server_acquired is not None, f"Server should have acquired lock. Log: {log}"
    assert client_acquired is not None, f"Client should have acquired lock. Log: {log}"


    if server_first:
        assert server_acquired < client_acquired, "When server_first=True, server should acquire lock before client"
    else:
        assert client_acquired < server_acquired, "When server_first=False, client should acquire lock before server"


class MessageTestServerSharedLock(pt.message.Message):
    """Message to test that server shared lock raises an exception"""
    def __init__(self, lock_name):
        self.lock_name = lock_name
    
    @classmethod
    def deserialize(cls, source, message):
        obj = cls(*message)
        obj.source = source
        return obj
    
    def serialize(self):
        return (self.lock_name,)
    
    def process(self):
        import threading
        
        def run_server_shared_test():
            try:
                with pt.lock.SharedLock(self.lock_name, 0):
                    pt_testing.log("Server acquired shared lock - should not happen!")
            except RuntimeError as e:
                pt_testing.log(f"Server shared lock correctly raised: {e}")
        
        # Run the test in a separate thread so server can continue processing messages
        thread = threading.Thread(target=run_server_shared_test)
        thread.start()


def _test_server_shared_lock_prohibition():
    """Test that server cannot use shared locks"""
    rank = pt.backend.rank()
    
    if rank == 1:
        # Client tells server to try shared lock (should fail)
        MessageTestServerSharedLock('server_shared_prohibition_test').send(0)
        
        # Client can still use shared locks normally
        with pt.lock.SharedLock('server_shared_prohibition_test', 0):
            pt_testing.log("Client acquired shared lock")
            time.sleep(0.05)
            pt_testing.log("Client releasing shared lock")

def test_server_shared_lock_prohibition():
    """Test that server raises exception when trying to use shared locks"""
    pt.use("multiprocessing-3")
    pt_testing.initialise_log()
    pt.launch(_test_server_shared_lock_prohibition)
    log = pt_testing.get_log()
    
    # Check that server raised the expected exception
    server_error = any("Server shared lock correctly raised: Server process cannot participate in shared locks" in line for line in log)
    client_acquired = any("Client acquired shared lock" in line for line in log)
    
    assert server_error, f"Server should have raised RuntimeError for shared lock. Log: {log}"
    assert client_acquired, f"Client should have acquired shared lock. Log: {log}"


def test_iteration_state_closes_tasks():
    from tangos.parallel_tasks.jobs import IterationState

    for backend_size in (2,3):
        iteration_state = IterationState.from_context(3, backend_size=backend_size)
        assert iteration_state.next_job(0)==0
        assert iteration_state.next_job(1)==1

        assert iteration_state.count_complete()==0
        assert iteration_state.next_job(0)==2
        assert iteration_state.count_complete()==1
        assert iteration_state.next_job(0) is None
        assert iteration_state.count_complete()==2
        assert iteration_state.next_job(0) is None
        assert iteration_state.count_complete()==2

        assert not iteration_state.finished() # no more jobs, but not finished until all tasks are closed
        assert iteration_state.next_job(1) is None
        assert iteration_state.count_complete()==3

        if backend_size==3:
            # STILL not finished -- all the tasks are closed, but not all ranks have seen that
            assert not iteration_state.finished()

            assert iteration_state.next_job(2) is None

    assert iteration_state.finished()

def test_iteration_state_from_string():
    from tangos.parallel_tasks.jobs import IterationState

    iteration_state = IterationState.from_context(20, backend_size=2)
    iteration_state.next_job(0) # job 0
    iteration_state.next_job(1) # job 1 (never completed)
    iteration_state.next_job(0) # job 2 (completes)
    iteration_state.next_job(0) # job 3 (never completed)
    string = iteration_state.to_string()
    iteration_state2 = IterationState.from_string(string, backend_size=2)
    assert iteration_state == iteration_state2
    assert iteration_state2.next_job(0) == 1
    assert iteration_state2.next_job(0) == 3
    assert iteration_state2.next_job(0) == 4
