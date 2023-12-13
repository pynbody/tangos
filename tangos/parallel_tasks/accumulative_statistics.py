import logging
import time

from ..config import PROPERTY_WRITER_PARALLEL_STATISTICS_TIME_BETWEEN_UPDATES
from ..log import logger
from .message import Message

_new_accumulator_requested_for_ranks = []
_new_accumulator = None
_existing_accumulators = []
class CreateNewAccumulatorMessage(Message):

    def process(self):
        from . import backend
        global _new_accumulator, _new_accumulator_requested_for_ranks, _existing_accumulators
        assert issubclass(self.contents, StatisticsAccumulatorBase)
        if _new_accumulator is None:
            _new_accumulator = self.contents()
            _new_accumulator_requested_for_ranks = [self.source]
        else:
            assert self.source not in _new_accumulator_requested_for_ranks
            assert isinstance(_new_accumulator, self.contents)
            _new_accumulator_requested_for_ranks.append(self.source)

            from . import backend

        if len(_new_accumulator_requested_for_ranks) == backend.size()-1:
            self._confirm_new_accumulator()

    def _confirm_new_accumulator(self):
        global _new_accumulator, _new_accumulator_requested_for_ranks, _existing_accumulators
        from . import backend, on_exit_parallelism
        accumulator_id = len(_existing_accumulators)
        _existing_accumulators.append(_new_accumulator)

        locally_bound_accumulator = _new_accumulator
        logger.debug("Created new accumulator of type %s with id %d" % (locally_bound_accumulator.__class__.__name__, accumulator_id))
        on_exit_parallelism(lambda: locally_bound_accumulator.report_to_log_if_needed(logger, 0.05))

        _new_accumulator = None
        _new_accumulator_requested_for_ranks = []

        for destination in range(1, backend.size()):
            AccumulatorIdMessage(accumulator_id).send(destination)
class AccumulatorIdMessage(Message):
    pass
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
            CreateNewAccumulatorMessage(self.__class__).send(0)
            logger.debug(f"Awaiting accumulator id for {self.__class__}")
            self.id = AccumulatorIdMessage.receive(0).contents
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
