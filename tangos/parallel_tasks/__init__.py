import importlib
import re
import sys
import warnings

from .. import config, core

backend = None
_backend_name = config.default_backend
_num_procs = None # only for multiprocessing backend

_on_exit = [] # list of functions to call when parallelism is shutting down

from .. import log
from . import accumulative_statistics, jobs, message


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

def launch(function, args=None, backend_kwargs=None):
    if args is None:
        args = []

    if backend_kwargs is None:
        backend_kwargs = {}

    result = None

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
                result = backend.launch(_exec_function_or_server, [function, connection_info, args], **backend_kwargs)
            else:
                result = function(*args)
        finally:
            if connection_info is not None:
                core.init_db(*connection_info)
    finally:
        deinit_backend()

    return result

def distributed(items, allow_resume=False, resumption_id=None):
    """Return an iterator that consumes the items, distributed across all processors
    (i.e. each item is consumed by only one processor, in a dynamic way).

    Optionally, if allow_resume is True, then the iterator will resume from the last point it reached
    provided argv and the stack trace are unchanged. If resumption_id is not None, then
    the stack trace is ignored and only resumption_id needs to match."""

    if type(items) == set:
        items = list(items)

    if _backend_name=='null':
        return items
    else:
        from . import jobs
        return jobs.distributed_iterate(items, allow_resume, resumption_id)

def synchronized(items, allow_resume=False, resumption_id=None):
    """Return an iterator that consumes all items on all processors.

    Optionally, if allow_resume is True, then the iterator will resume from the last point it reached
    provided argv and the stack trace are unchanged. If resumption_id is not None, then
    the stack trace is ignored and only resumption_id needs to match."""
    if _backend_name=='null':
        return items
    else:
        from . import jobs
        return jobs.synchronized_iterate(items, allow_resume, resumption_id)




def _exec_function_or_server(function, connection_info, args):
    log.set_identity_string("[%3d] " % backend.rank())
    if connection_info is not None:
        log.logger.debug("Reinitialising database, args "+str(connection_info))
        core.init_db(*connection_info)

    from .message import _setup_message_reception_timing_monitor
    _setup_message_reception_timing_monitor()

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

    from .async_message import init_async_processing_thread
    init_async_processing_thread() # uses on_exit_parallelism to ensure thread is cleared up

    alive = [True for i in range(backend.size())]

    while any(alive[1:]):
        obj = message.Message.receive()
        if isinstance(obj, MessageExit):
            alive[obj.source]=False
        else:
            obj.process()

def on_exit_parallelism(function):
    global _on_exit
    _on_exit.append(function)

def _shutdown_parallelism():
    global backend, _on_exit
    log.logger.debug("Clearing up process")

    for fn in _on_exit:
        fn()
    _on_exit = []

    backend.barrier()
    backend.finalize()
    backend = None
    _bankend_name = 'null'




from . import async_message, remote_import, shared_set
from .barrier import barrier
from .lock import ExclusiveLock
