import importlib
import re
import sys
import time
import traceback
import warnings

import tangos.core.creator

from .. import config, core

backend = None
_backend_name = config.default_backend
_num_procs = None # only for multiprocessing backend

from .. import log
from ..log import logger
from . import backends, jobs, message


def use(name):
    global backend, _backend_name, _num_procs
    if backend is not None:
        warnings.warn("Attempt to specify backend but parallelism is already initialised. This call may have no effect, unless you know exactly what you're doing.", RuntimeWarning)
    if "-" in name:
        _backend_name, num_procs = name.split("-")
        _num_procs = int(num_procs)
    else:
        _backend_name = name

def _process_command_line():
    command_line = " ".join(sys.argv)
    match = re.match("^(.*)--backend *=? *([^ ]*)(.*)$", command_line)
    if match is not None:
        use(match.group(2))
        new_command_line = match.group(1)+" "+match.group(3)
        sys.argv = new_command_line.split()

_process_command_line()

def init_backend():
    global backend
    if backend is None:
        backend = importlib.import_module("."+_backend_name, package='tangos.parallel_tasks.backends')

def deinit_backend():
    global backend
    backend = None

def parallelism_is_active():
    global _backend_name
    return _backend_name != 'null' and backend is not None

def launch(function, args=None):
    if args is None:
        args = []

    # we need to close any existing connections because we may fork, which leads to
    # buggy/unreliable behaviour. This should invalidate the session attached to
    # any existing objects, which is intended behaviour. If you are using parallel
    # tasks, you need to re-query for any objects you are using once inside the
    # parallel jobs.
    if core._engine is not None:
        connection_info = core._internal_session_args
    else:
        connection_info = None

    try:
        init_backend()

        try:
            core.close_db()
            if _backend_name != 'null':
                backend.launch(_exec_function_or_server, [function, connection_info, args])
            else:
                function(*args)
        finally:
            if connection_info is not None:
                core.init_db(*connection_info)
    finally:
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


def _exec_function_or_server(function, connection_info, args):
    log.set_identity_string("[%3d] " % backend.rank())
    if connection_info is not None:
        log.logger.debug("Reinitialising database, args "+str(connection_info))
        core.init_db(*connection_info)

    if backend.rank()==0:
        _server_thread()
    else:
        function(*args)
        MessageExit().send(0)
    core.close_db()
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




from . import remote_import
from .barrier import barrier
from .lock import ExclusiveLock
