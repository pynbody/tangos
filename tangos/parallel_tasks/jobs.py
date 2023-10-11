from .. import log
from . import message

j = -1
num_jobs = None
current_job = None

class MessageStartIteration(message.Message):
    def process(self):
        global num_jobs, current_job
        if num_jobs is None:
            num_jobs = self.contents
            current_job = 0
        else:
            if num_jobs != self.contents:
                raise RuntimeError("Number of jobs (%d) expected by rank %d is inconsistent with %d" % (
                self.contents, self.source, num_jobs))


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
        global j, num_jobs, current_job
        source = self.source
        if current_job is not None and num_jobs>0:
            log.logger.info("Send job %d of %d to node %d", current_job, num_jobs, source)
        else:
            num_jobs = None
            current_job = None # in case num_jobs=0, still want to send 'end of loop' signal to client
            log.logger.info("Finished jobs; notify node %d", source)

        MessageDeliverJob(current_job).send(source)

        if current_job is not None:
            current_job += 1
            if current_job == num_jobs:
                num_jobs = None
                current_job = None


def parallel_iterate(task_list):
    """Sets up an iterator returning items of task_list. """
    from . import backend, barrier

    assert backend is not None, "Parallelism is not initialised"
    MessageStartIteration(len(task_list)).send(0)
    barrier()

    while True:
        MessageRequestJob().send(0)
        job = MessageDeliverJob.receive(0).contents

        if job is None:
            barrier()
            return
        else:
            yield task_list[job]

def generate_task_list_and_parallel_iterate(task_list_function):
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
    return parallel_iterate(task_list)
