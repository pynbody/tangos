import copy
import threading
import time
from typing import Optional

from ..config import PROPERTY_WRITER_PARALLEL_STATISTICS_TIME_BETWEEN_UPDATES
from ..log import logger
from .message import BarrierMessageWithResponse, Message

_existing_accumulators = []
_accumulator_reporting_thread: Optional[threading.Thread] = None
_accumulator_reporting_semaphore: Optional[threading.Semaphore] = None

def _accumulator_reporting():
    global _existing_accumulators
    while True:
        if _accumulator_reporting_semaphore.acquire(timeout=PROPERTY_WRITER_PARALLEL_STATISTICS_TIME_BETWEEN_UPDATES):
            break # semaphore has been released, meaning the thread should stop

        # otherwise, semaphore has timed out, so report
        for a in _existing_accumulators:
            a.report_to_log_if_needed(logger)
def _start_accumulator_reporting_thread():
    from . import on_exit_parallelism
    global _accumulator_reporting_thread, _accumulator_reporting_semaphore
    if _accumulator_reporting_thread is None:
        _accumulator_reporting_semaphore = threading.Semaphore(0)
        _accumulator_reporting_thread = threading.Thread(target=_accumulator_reporting)
        _accumulator_reporting_thread.start()
        on_exit_parallelism(_stop_accumulator_reporting_thread)

def _stop_accumulator_reporting_thread():
    global _accumulator_reporting_thread, _accumulator_reporting_semaphore
    if _accumulator_reporting_thread is not None:
        _accumulator_reporting_semaphore.release()
        _accumulator_reporting_thread.join()
        _accumulator_reporting_thread = None
        _accumulator_reporting_semaphore = None

class CreateNewAccumulatorMessage(BarrierMessageWithResponse):

    def process_global(self):
        global _existing_accumulators

        _start_accumulator_reporting_thread()

        from . import on_exit_parallelism

        if self.contents[1]:
            # has kwargs
            new_accumulator = self.contents[0](**self.contents[1])
        else:
            new_accumulator = self.contents[0]()

        accumulator_id = len(_existing_accumulators)
        _existing_accumulators.append(new_accumulator)

        locally_bound_accumulator = new_accumulator
        logger.debug("Created new accumulator of type %s with id %d" % (
                     locally_bound_accumulator.__class__.__name__, accumulator_id))
        on_exit_parallelism(lambda: locally_bound_accumulator.report_to_log_if_needed(logger))

        self.respond(accumulator_id)

class AccumulateStatisticsMessage(Message):
    def process(self):
        global _existing_accumulators
        _existing_accumulators[self.contents.id].add(self.contents)

class StatisticsAccumulatorBase:
    REPORT_AFTER = PROPERTY_WRITER_PARALLEL_STATISTICS_TIME_BETWEEN_UPDATES
    def __init__(self, allow_parallel=False, accumulator_init_kwargs=None):
        """This is a base class for accumulating statistics, possibly in parallel across multiple processes.

        Note that if allow_parallel is True, then all processes must create an instance of this class
        (in effect creating the class will act like a barrier). If only some processes create an instance,
        they will block, possibly creating a deadlock. This is why allow_parallel defaults to False.
        """
        from . import backend, parallelism_is_active
        self._last_reported = time.time()
        self._parallel = allow_parallel and parallelism_is_active() and backend.rank() != 0
        if self._parallel:
            logger.debug(f"Registering {self.__class__}")
            self.id = CreateNewAccumulatorMessage((self.__class__, accumulator_init_kwargs)).send_and_get_response(0)
            logger.debug(f"Received accumulator id={ self.id}")
        self._state_at_last_report = copy.deepcopy(self)

    def report_to_server(self):
        if self._parallel:
            AccumulateStatisticsMessage(self).send(0)
            self.reset()

    def reset(self):
        raise NotImplementedError("This method should be overriden to reset the statistics")

    def add(self, other):
        raise NotImplementedError("This method should be overriden to add two accumulations together")

    def report_to_log(self, logger):
        raise NotImplementedError("This method should be overriden to log a statistics report")

    def report_to_log_or_server(self, logger):
        if self._parallel:
            self.report_to_server()
        else:
            self.report_to_log(logger)

    def report_to_log_if_needed(self, logger):
        if self != self._state_at_last_report:
            self.report_to_log(logger)
            self._state_at_last_report = None # avoid limitless depth in copy!
            self._state_at_last_report = copy.deepcopy(self)

    def __eq__(self, other):
        raise NotImplementedError("This method should be overriden to compare two accumulations")
