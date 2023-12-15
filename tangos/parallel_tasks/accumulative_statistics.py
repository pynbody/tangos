import logging
import time

from ..config import PROPERTY_WRITER_PARALLEL_STATISTICS_TIME_BETWEEN_UPDATES
from ..log import logger
from .message import BarrierMessageWithResponse, Message

_new_accumulator_requested_for_ranks = []
_new_accumulator = None
_existing_accumulators = []
class CreateNewAccumulatorMessage(BarrierMessageWithResponse):

    def process_global(self):
        from . import backend, on_exit_parallelism

        new_accumulator = self.contents()
        accumulator_id = len(_existing_accumulators)
        _existing_accumulators.append(new_accumulator)

        locally_bound_accumulator = new_accumulator
        logger.debug("Created new accumulator of type %s with id %d" % (
        locally_bound_accumulator.__class__.__name__, accumulator_id))
        on_exit_parallelism(lambda: locally_bound_accumulator.report_to_log_if_needed(logger, 0.05))

        self.respond(accumulator_id)

class AccumulateStatisticsMessage(Message):
    def process(self):
        global _existing_accumulators
        _existing_accumulators[self.contents.id].add(self.contents)

class StatisticsAccumulatorBase:
    REPORT_AFTER = PROPERTY_WRITER_PARALLEL_STATISTICS_TIME_BETWEEN_UPDATES
    def __init__(self, allow_parallel=False):
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
            self.id = CreateNewAccumulatorMessage(self.__class__).send_and_get_response(0)
            logger.debug(f"Received accumulator id={ self.id}")

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

    def report_to_log_if_needed(self, logger, after_time=None):
        if after_time is None:
            after_time = self.REPORT_AFTER
        if time.time() - self._last_reported > after_time:
            self.report_to_log(logger)
            self._last_reported = time.time()
