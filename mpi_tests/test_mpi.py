#!/usr/bin/env python

import sys
import tangos.parallel_tasks as pt
import time


def test_function():
    lock = pt.RLock("hello")

    print "Hello from rank",pt.backend.rank()
    for i in pt.distributed(xrange(10)):
        with lock:
            print "Task",i
            time.sleep(0.5)

if len(sys.argv)!=2:
    print "Syntax: test_mpi.py [backend name]"
else:
    pt.use(sys.argv[1])
    pt.launch(test_function, 8)



