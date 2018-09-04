from __future__ import absolute_import
import time
import warnings
import importlib
import sys
import re

import tangos.core.creator
from .. import core, config
import traceback
from six.moves import range

backend = None
_backend_name = config.default_backend
from .. import log
from . import message, jobs, backends


from ..log import logger

def _process_command_line():
    command_line = " ".join(sys.argv)
    match = re.match("^(.*)--backend *=? *([^ ]*)(.*)$", command_line)
    if match is not None:
        global _backend_name
        _backend_name = match.group(2)
        new_command_line = match.group(1)+" "+match.group(3)
        sys.argv = new_command_line.split()

_process_command_line()

def use(name):
    global backend, _backend_name
    if backend is not None:
        warnings.warn("Attempt to specify backend but parallelism is already initialised. This call may have no effect, unless you know exactly what you're doing.", RuntimeWarning)
    _backend_name = name

def init_backend():
    global backend
    if backend is None:
        backend = importlib.import_module("."+_backend_name, package='tangos.parallel_tasks.backends')

def deinit_backend():
    global backend
    backend = None

def parallel_backend_loaded():
    global _backend_name
    return _backend_name!='null'

def launch(function, num_procs=None, args=[]):
    init_backend()
    if _backend_name!='null':
        backend.launch(_exec_function_or_server, num_procs, [function, args])
    else:
        function(*args)

    deinit_backend()

def distributed(file_list, proc=None, of=None):
    """Distribute a list of tasks between all nodes"""

    if type(file_list) == set:
        file_list = list(file_list)

    if _backend_name=='null':
        if proc is None:
            proc = 1
            of = 1
        i = (len(file_list) * (proc - 1)) // of
        j = (len(file_list) * proc) // of - 1
        assert proc <= of and proc > 0
        if proc == of:
            j += 1
        return file_list[i:j + 1]
    else:
        from . import jobs
        return jobs.parallel_iterate(file_list)


def _exec_function_or_server(function, args):
    log.set_identity_string("[%3d] " % backend.rank())
    if backend.rank()==0:
        _server_thread()
    else:
        function(*args)
        MessageExit().send(0)
    _shutdown_parallelism()
    log.set_identity_string("")

class MessageExit(message.Message):
    pass


def _server_thread():
    # Sit idle until request for a job comes in, then assign first
    # available job and move on. Jobs are labelled through the
    # provided iterator

    j = -1
    num_jobs = None
    current_job = None
    alive = [True for i in range(backend.size())]
    awaiting_barrier = [False for i in range(backend.size())]

    while any(alive[1:]):
        obj = message.Message.receive()
        if isinstance(obj, MessageExit):
            alive[obj.source]=False
        else:
            obj.process()


    log.logger.info("Terminating manager")


def _shutdown_parallelism():
    global backend
    log.logger.info("Shutting down parallel_tasks")
    backend.barrier()
    backend.finalize()
    backend = None
    _bankend_name = 'null'




from .lock import ExclusiveLock
from .barrier import barrier
from . import remote_import