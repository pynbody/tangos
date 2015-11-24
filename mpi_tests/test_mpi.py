import halo_db as db
import halo_db.parallel_tasks as pt
import time

pt.use('multiprocessing')

def test_function():
    lock = pt.RLock("hello")
    for i in pt.distributed(xrange(10)):
        with lock:
            print "Task",i
            time.sleep(0.5)


pt.launch(test_function, 8)