import multiprocessing
import threading
import warnings
import time
import os
import signal

_slave = False
_rank = None
_size = None
_pipe = None
_recv_lock = None
_recv_buffer = []

class NoMatchingItem(Exception):
    pass


def send(data, destination, tag=0):
    _pipe.send((data, destination, tag))

def receive_any(source=None):
    return receive(source,None,True)


def receive(source=None, tag=0, return_tag=False):
    while True:
        try:
            item = _pop_first_match_from_reception_buffer(source, tag)
            if return_tag:
                return item
            else:
                return item[0]
        except NoMatchingItem:
            _receive_item_into_buffer()


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
            _recv_buffer.append(_pipe.recv())
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
    _pipe.send("finalize")

def launch_wrapper(target_fn, rank_in, size_in, pipe_in, args_in):
    global _slave, _rank, _size, _pipe, _recv_lock
    _rank = rank_in
    _size = size_in
    _pipe = pipe_in
    _recv_lock = threading.Lock()
    try:
        target_fn(*args_in)
        finalize()
    except Exception as e:
        _pipe.send(("error", e))

    _pipe.close()


def launch_functions(functions, args):
    global _slave
    if _slave:
        raise RuntimeError("Multiprocessing session is already underway")

    num_procs = len(functions)

    child_connections, parent_connections = zip(*[multiprocessing.Pipe() for rank in range(num_procs)])
    processes = [multiprocessing.Process(target=launch_wrapper, args=(function, rank, num_procs, pipe, args_i))
                 for rank, (pipe, function, args_i) in
                 enumerate(zip(child_connections, functions, args))]

    for proc_i in processes:
        proc_i.start()

    running = [True for rank in range(num_procs)]
    error = False

    while any(running):
        for i, pipe_i in enumerate(parent_connections):
            if pipe_i.poll():
                message = pipe_i.recv()
                if message=='finalize':
                    #print "  ---> multiprocessing backend: finalize node ",i,running
                    running[i]=False
                elif message[0]=='error':
                    error = message[1]
                    running = [False]
                    break
                else:
                    #print "multiprocessing backend: pass message ",i,"->",message[1]
                    parent_connections[message[1]].send((message[0],i,message[2]))

    #print "multiprocessing backend: all finished"

    for pipe_i in parent_connections:
        pipe_i.close()

    for proc_i in processes:
        if error:
            #print "multiprocessing backend: send signal to",proc_i.pid
            os.kill(proc_i.pid, signal.SIGTERM)
        proc_i.join()

    if error:
        raise error



def launch(function, num_procs, args):
    if num_procs is None:
        raise RuntimeError("To launch a parallel session using multiprocessing backend, you need to specify the number of processors")

    launch_functions([function]*num_procs, [args]*num_procs)


