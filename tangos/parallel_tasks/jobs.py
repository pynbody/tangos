import base64
import getpass
import hashlib
import os
import pickle
import pipes
import sys
import traceback
import zlib

from .. import log
from . import message


class InconsistentJobList(RuntimeError):
    pass

class InconsistentContext(RuntimeError):
    pass

class IterationState:
    _stored_iteration_states = {}
    def __init__(self, context, jobs_complete, /, backend_size=None):
        from . import backend
        self._context = context
        self._jobs_complete = jobs_complete
        self._rank_running_job = {i: None for i in range(1,backend_size or backend.size())}

    def __len__(self):
        return len(self._jobs_complete)

    def to_string(self):
        return base64.a85encode(
            zlib.compress(
                pickle.dumps(self._jobs_complete)
            )
        ).decode('ascii')

    @classmethod
    def from_string(cls, string, context=None, backend_size=None):
        jobs_complete = pickle.loads(
            zlib.decompress(
                base64.a85decode(string.encode('ascii'))
            )
        )
        return cls(context, jobs_complete, backend_size=backend_size)

    @classmethod
    def from_context(cls, num_jobs, argv=None, stack_hash=None, allow_resume=None, backend_size=None):
        context = (argv, stack_hash, num_jobs)
        if allow_resume:
            cmap = cls._get_stored_completion_map_from_context(context)
            if cmap is not None:
                r = cls.from_string(cmap, context)
                log.logger.info(
                    f"Resuming from previous run. {r.count_complete()} of {len(r)} jobs are already complete.")
                log.logger.info(
                    f"To prevent tangos from doing this, you can delete the file {cls._resume_state_filename()}")
                return r

        return cls(context, [False]*num_jobs, backend_size=backend_size)


    @classmethod
    def _resume_state_filename(cls):
        return f"tangos_resume_state_{getpass.getuser()}.pickle"

    @classmethod
    def _get_stored_completion_maps(cls):
        maps = {}
        try:
            with open(cls._resume_state_filename(), "rb") as f:
                maps = pickle.load(f)
        except FileNotFoundError:
            pass
        return maps

    @classmethod
    def _store_completion_maps(cls, maps):
        with open(cls._resume_state_filename(), "wb") as f:
            pickle.dump(maps, f)
    @classmethod
    def _get_stored_completion_map_from_context(cls, context):
        maps = cls._get_stored_completion_maps()
        return maps.get(context, None)

    @classmethod
    def clear_resume_state(cls):
        try:
            os.unlink(cls._resume_state_filename())
        except FileNotFoundError:
            pass

    def _store_completion_map(self):
        # In principle, there could be a race condition if more than one tangos process
        # is ongoing on the same filesystem. However, this is very unlikely to happen
        # since updating the completion map is very quick and happens quite rarely,
        # so we don't worry about it.
        maps = self._get_stored_completion_maps()
        maps[self._context] = self.to_string()
        self._store_completion_maps(maps)

    def mark_complete(self, job):
        if job is None:
            return
        self._jobs_complete[job] = True
        self._store_completion_map()

    def next_job(self, for_rank):
        if for_rank in self._rank_running_job:
            self.mark_complete(self._rank_running_job[for_rank])
            del self._rank_running_job[for_rank]

        for i in range(len(self._jobs_complete)):
            if not self._jobs_complete[i] and i not in self._rank_running_job.values():
                self._rank_running_job[for_rank] = i
                return i
        return None

    def finished(self):
        # not enough for all jobs to be complete, must also have notified all ranks (this matters
        # if some ranks never did any work at all)
        return all(self._jobs_complete) and len(self._rank_running_job)==0

    def count_complete(self):
        return sum(self._jobs_complete)

    def __eq__(self, other):
        return self._jobs_complete == other._jobs_complete


class SynchronizedIterationState(IterationState):
    def _first_incomplete_job_after(self, job):
        if job is None:
            job = -1 # so that we scan over all possible jobs when the loop first enters
        for i in range(job+1, len(self._jobs_complete)):
            if not self._jobs_complete[i]:
                return i
        return None

    def _is_still_running_somewhere(self, job):
        for v in self._rank_running_job.values():
            if v == job:
                return True
        return False

    def next_job(self, for_rank):
        previous_job = self._rank_running_job[for_rank]
        my_next_job = self._first_incomplete_job_after(previous_job)
        if my_next_job is None:
            del self._rank_running_job[for_rank]
        else:
            self._rank_running_job[for_rank] = my_next_job

        if not self._is_still_running_somewhere(previous_job):
            self.mark_complete(previous_job)

        return my_next_job

current_num_jobs = None
current_iteration_state = None
current_stack_hash = None
current_is_resumable = False

class MessageStartIteration(message.Message):
    def process(self):
        global current_iteration_state, current_stack_hash, current_is_resumable
        req_jobs, req_hash, allow_resume, synchronized = self.contents

        if current_iteration_state is None:
            current_num_jobs = req_jobs
            current_stack_hash = req_hash
            current_is_resumable = allow_resume
            # convert sys.argv to a string which is quoted/escaped as needed
            argv_string = " ".join([pipes.quote(arg) for arg in sys.argv])

            IteratorClass = SynchronizedIterationState if synchronized else IterationState

            current_iteration_state = IteratorClass.from_context(req_jobs, argv=" ".join(sys.argv),
                                                                  stack_hash=req_hash,
                                                                  allow_resume=allow_resume)
            # note: if running a 'synchronized' loop, only rank 1 will be requesting jobs, and the other ranks
            # will be synchronized just via a simple barrier. This allows for a much simpler implementation.


        else:
            if len(current_iteration_state) != req_jobs:
                raise InconsistentJobList(f"Number of jobs ({req_jobs}) expected by rank {self.source} "
                                          f"is inconsistent with {len(current_iteration_state)}")
            if current_stack_hash != req_hash:
                raise InconsistentContext(f"Inconsistent stack from rank {self.source} when entering parallel loop")
            if current_is_resumable != allow_resume:
                raise InconsistentContext(f"Inconsistent allow_resume flag from rank {self.source} when entering parallel loop")

class MessageDeliverJob(message.Message):
    pass

class MessageDistributeJobList(message.Message):
    def process(self):
        # server should send this back out to all the other ranks
        from . import backend
        for rank in range(1, backend.size()):
            if rank != self.source:
                MessageDistributeJobList(self.contents).send(rank)
class MessageRequestJob(message.Message):
    def process(self):
        global current_iteration_state
        source = self.source
        assert current_iteration_state is not None # should not be requesting jobs if we are not in a loop

        job = current_iteration_state.next_job(source)

        if job is not None:
            log.logger.debug("Send job %d of %d to node %d", job, len(current_iteration_state), source)
        else:
            log.logger.debug("Finished jobs; notify node %d", source)

        MessageDeliverJob(job).send(source)

        if current_iteration_state.finished():
            current_iteration_state = None

def distributed_iterate(task_list, allow_resume=False, resumption_id=None):
    """Sets up an iterator returning items of task_list.

    If allow_resume is True, then the iterator will resume from the last point it reached
    provided argv and the stack trace are unchanged. If resumption_id is not None, then
    the stack trace is ignored and only resumption_id needs to match.
    """
    from . import backend, barrier

    resumption_id = resumption_id or _autogenerate_resume_id()

    assert backend is not None, "Parallelism is not initialised"
    MessageStartIteration((len(task_list), resumption_id, allow_resume, False)).send(0)
    barrier()

    while True:
        MessageRequestJob().send(0)
        job = MessageDeliverJob.receive(0).contents

        if job is None:
            barrier()
            return
        else:
            yield task_list[job]


def _autogenerate_resume_id():
    stack_string = "\n".join(traceback.format_stack())
    # we need a hash of stack_string that is stable across runs.
    resumption_id = hashlib.sha256(stack_string.encode('utf-8')).hexdigest()
    return resumption_id


def synchronized_iterate(task_list, allow_resume=False, resumption_id=None):
    """Like distributed_iterate, but all processes see all tasks.

    The main advantage is the ability to resume if allow_resume is True"""
    from . import backend, barrier

    resumption_id = resumption_id or _autogenerate_resume_id()

    assert backend is not None, "Parallelism is not initialised"

    MessageStartIteration((len(task_list), resumption_id, allow_resume, True)).send(0)
    barrier()

    while True:
        MessageRequestJob().send(0)
        job = MessageDeliverJob.receive(0).contents

        if job is None:
            barrier()
            return

        yield task_list[job]






def generate_task_list_and_parallel_iterate(task_list_function, allow_resume=False):
    """Call task_list_function on only one rank, and then parallel iterate with all ranks"""
    from . import backend

    assert backend is not None, "Parallelism is not initialised"

    if backend.rank()==1:
        task_list = task_list_function()
        MessageDistributeJobList(task_list).send(0)
        log.logger.debug("generated task list = %r",task_list)
    else:
        log.logger.debug("awaiting rank 1 generating task list")
        task_list = MessageDistributeJobList.receive(0).contents
        log.logger.debug("task_list = %r",task_list)
    return distributed_iterate(task_list, allow_resume=allow_resume)
