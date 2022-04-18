from __future__ import absolute_import
from . import message
from .. import log

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