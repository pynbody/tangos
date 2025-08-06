"""An implementation of a multiprocessing backend for parallel tasks in Tangos.

This implements an MPI-like interface using a 'manager' process which relays messages between worker processes.
Messages are thus sent and received using a star-like topology, but this should be transparent to the user.
"""

import multiprocessing
import multiprocessing.resource_tracker
import os
import pickle
import select
import signal
import struct
import sys
import threading
import time
from typing import Optional

import tblib.pickling_support

from ...log import logger

_slave = False
_rank = None
_size = None
_pipe = None
_recv_lock = None
_recv_buffer = []

_print_exceptions = True

send_lock = threading.Lock() # a lock to make sure if multiple threads are running, only one can send/receive at a time
receive_lock = threading.Lock()

# Compatibility fix for python >=3.8 on MacOS, where the default process start
# method changed:
if sys.version_info[:2]>=(3,3):
    mp_context = multiprocessing.get_context('fork')
else :
    mp_context = multiprocessing

class NoMatchingItem(Exception):
    pass


def send(data, destination, tag=0):
    with send_lock:
        payload = pickle.dumps(data)
        header = struct.pack("ii", destination, tag)
        _pipe.send_bytes(header+payload)

def receive_any(source=None):
    return receive(source,None,True)


def receive(source=None, tag=0, return_tag=False):
    with receive_lock:
        while True:
            try:
                item = _pop_first_match_from_reception_buffer(source, tag)
                if return_tag:
                    return item
                else:
                    return item[0]
            except NoMatchingItem:
                _receive_item_into_buffer()


NUMPY_SPECIAL_TAG = 1515

def send_numpy_array(data, destination):
    send(data,destination,tag=NUMPY_SPECIAL_TAG)

def receive_numpy_array(source):
    return receive(source,tag=NUMPY_SPECIAL_TAG)

def _pop_first_match_from_reception_buffer(source, tag):
    for item in _recv_buffer:
        if ((item[2] == tag or tag is None) and (item[1] == source or source is None)):
            # consume item
            _recv_buffer.remove(item)
            return item

    raise NoMatchingItem()

def _receive_item_into_buffer():
    if _recv_lock.acquire(False):
        try:
            message = _pipe.recv_bytes()
            source, tag = struct.unpack("ii", message[:8])
            payload = pickle.loads(message[8:])
            _recv_buffer.append((payload, source, tag))
        finally:
            _recv_lock.release()
    else:
        # block until a data item has been received by another thread
        _recv_lock.acquire()
        _recv_lock.release()



def rank():
    return _rank

def size():
    return _size

def barrier():
    pass

def finalize():
    pass

def launch_wrapper(target_fn, rank_in, size_in, pipe_in, args_in, capture_log):
    tblib.pickling_support.install()

    global _slave, _rank, _size, _pipe, _recv_lock
    _rank = rank_in
    _size = size_in
    _pipe = pipe_in
    _recv_lock = threading.Lock()

    result = None

    try:
        if capture_log:
            from .. import log
            try:
                with log.LogCapturer() as lc:
                    target_fn(*args_in)
            finally:
                result = lc.get_output()
        else:
            target_fn(*args_in)
        if result is not None:
            send(("log", result), -1, -1)
        send("exit", -1, -1)
    except Exception as e:
        import sys
        import traceback
        exc_type, exc_value, exc_traceback = sys.exc_info()
        global _print_exceptions
        if _print_exceptions:
            print("Error on a sub-process:", file=sys.stderr)
            traceback.print_exception(exc_type, exc_value, exc_traceback,
                                      file=sys.stderr)
        if result is not None:
            send(("log", result), -1, -1)
        send(("error", exc_value, exc_traceback), -1, -1)


    _pipe.close()

class RemoteException(Exception):
    pass

def launch_functions(functions, args, capture_log=False):
    global _slave
    if _slave:
        raise RuntimeError("Multiprocessing session is already underway")

    # the resource tracker must be running before we start any processes,
    # otherwise they'll start their own resource trackers and all sorts
    # of confusion will ensue
    multiprocessing.resource_tracker.ensure_running()

    num_procs = len(functions)


    child_connections, parent_connections = list(zip(*[mp_context.Pipe() for rank in range(num_procs)]))
    processes = [mp_context.Process(target=launch_wrapper, args=(function, rank, num_procs, pipe, args_i, capture_log))
                 for rank, (pipe, function, args_i) in
                 enumerate(zip(child_connections, functions, args))]

    for proc_i in processes:
        proc_i.start()

    running = [True for rank in range(num_procs)]
    error: Optional[Exception] = None

    log = "" if capture_log else None

    while any(running):
        readable_pipes, _, _ = select.select(parent_connections, [], [])
        for pipe_i in readable_pipes:
            source = parent_connections.index(pipe_i)
            if pipe_i.poll():
                message = pipe_i.recv_bytes()
                destination, tag = struct.unpack("ii", message[:8])
                if destination == -1:
                    # special message, not for a specific process
                    message = pickle.loads(message[8:])
                    if message=='exit':
                        running[source]=False
                    elif isinstance(message[0], str) and message[0]=='error':
                        error = message[1]
                        traceback = message[2]
                        running = [False]
                        break
                    elif isinstance(message[0], str) and message[0]=='log':
                        log+=message[1]
                else:
                    new_header = struct.pack("ii", source, tag)
                    payload = message[8:]
                    parent_connections[destination].send_bytes(new_header + payload)

    for pipe_i in parent_connections:
        pipe_i.close()

    for proc_i in processes:
        if error:
            os.kill(proc_i.pid, signal.SIGTERM)
        proc_i.join(timeout=1.0)
        if proc_i.is_alive():
            logger.warn("Process %d did not terminate in a timely way; sending SIGKILL", proc_i.pid)
            os.kill(proc_i.pid, signal.SIGKILL)
            proc_i.join()

    if error is not None:
        raise error.with_traceback(traceback)

    return _sort_log(log)

def _sort_log(log):
    """Sort the log by time.

    The input log is of the format, for example:
      [  3] 2023-12-12 13:55:46,004 Message

    This routine extracts the times, parses them and returns a reordered log in time order.
    It is stable, i.e. messages with the same timestep are returned in the input order
    """
    if log is None:
        return None
    lines = log.split("\n")
    times = []
    for line in lines:
        if line.strip() == "":
            continue
        times.append(line[6:28])
    times = [time.strptime(t, "%Y-%m-%d %H:%M:%S,%f") for t in times]
    lines = [line for _, line in sorted(zip(times, lines), key=lambda item: item[0])]
    return "\n".join(lines)





def launch(function, args, **kwargs):
    from .. import _num_procs
    if _num_procs is None:
        raise RuntimeError("To launch a parallel session using multiprocessing backend, you need to specify the number "
                           "of processors. You can do this by calling the backend multiprocessing-<n> where <n> is the"
                           "number of processors you want to use.")

    return launch_functions([function]*_num_procs, [args]*_num_procs, **kwargs)
