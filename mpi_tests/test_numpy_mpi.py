#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import print_function
import sys
import tangos.parallel_tasks as pt
import time
import numpy as np
from six.moves import zip


def test_function():
    lock = pt.ExclusiveLock("hello")

    rank = pt.backend.rank()

    for test_len, test_type in zip((10, 10000, 10000), (np.int32, np.float64, np.float32)) :
        data_to_send = np.arange(test_len, dtype=test_type)

        if rank == 1:
            pt.backend.send_numpy_array(data_to_send, 2)

        elif rank == 2:
            received = pt.backend.receive_numpy_array(1)
            assert(received.dtype==data_to_send.dtype)
            assert(received.shape==data_to_send.shape)
            assert all(received==data_to_send)

    if rank==2:
        print()
        print("OK")


if len(sys.argv)!=2:
    print("Syntax: test_numpy_mpi.py [backend name]")
else:
    pt.use(sys.argv[1])
    pt.launch(test_function, 8)