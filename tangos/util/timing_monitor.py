import contextlib
import inspect
import time

import numpy as np

from ..parallel_tasks import accumulative_statistics


class TimingMonitor(accumulative_statistics.StatisticsAccumulatorBase):
    """This class keeps track of how long a Property is taking to evaluate, and (if the Property is implemented
    to take advantage of this), the time spent on sub-tasks. It provides formatting to place this information
    into the log."""
    def __init__(self, allow_parallel=False, label="running times", num_chars=20, show_percentages=True):
        self._label = label
        self._num_chars = num_chars
        self._show_percentages = show_percentages
        self.reset()
        super().__init__(allow_parallel=allow_parallel,
                         accumulator_init_kwargs={'label': label, 'num_chars': num_chars,
                                                  'show_percentages': show_percentages})
        self._monitoring = None

    @contextlib.contextmanager
    def __call__(self, object):
        self._start(object)
        yield
        self._end()

    def reset(self):
        self.timings_by_class = {}
        self.labels_by_class = {}

    def check_compatible_object(self, object):
        if not hasattr(object, 'timing_monitor'):
            raise TypeError("TimingMonitor requires a compatible object to monitor")

    def _set_as_monitor_for(self, object):
        self.check_compatible_object(object)
        if self._monitoring is not None:
            raise RuntimeError(
                f"TimingMonitor is already monitoring {self._monitoring!r} and cannot also monitor {object!r}")
        self._monitoring = object
        self._old_timing_monitor = object.timing_monitor
        object.timing_monitor = self

    def _unset_as_monitor_for(self, object):
        if self._monitoring is not object:
            raise RuntimeError("Consistency error - trying to stop monitoring an object that is not being monitored")
        object.timing_monitor = self._old_timing_monitor
        del self._old_timing_monitor
        self._monitoring = None

    def _start(self, object):
        """Start a timer for the specified object"""
        self._set_as_monitor_for(object)
        self._time_marks_info = ["start"]
        self._time_marks = [time.time()]

    def _end(self):
        """End a timer for the specified object."""
        if isinstance(self._monitoring, type):
            cl = self._monitoring
        else:
            cl = type(self._monitoring)
        self._unset_as_monitor_for(self._monitoring)
        self._time_marks_info.append("end")
        self._time_marks.append(time.time())

        self._add_run_to_running_totals(cl, self._time_marks, self._time_marks_info)

    def _add_run_to_running_totals(self, cl, latest_run_time_marks, latest_run_time_marks_labels):
        previous_timings = self.timings_by_class.get(cl, None)
        if previous_timings is None or len(previous_timings) == len(latest_run_time_marks) - 1:
            cumulative_timings = np.diff(latest_run_time_marks)
            if previous_timings is not None:
                cumulative_timings += previous_timings
            self.timings_by_class[cl] = cumulative_timings
            self.labels_by_class[cl] = latest_run_time_marks_labels
        else:
            # Incompatibility between this and previous timings from the same procedure. Can only track total time spent.
            start_time = latest_run_time_marks[0]
            end_time = latest_run_time_marks[-1]
            time_elapsed = end_time - start_time + sum(previous_timings)
            self.labels_by_class[cl] = ['start', 'end']
            self.timings_by_class[cl] = [time_elapsed]

    def mark(self, label=None):
        """Mark a named event so that more detailed timing can be given"""
        self._time_marks.append(time.time())
        if label is None:
            self._time_marks_info.append(
                inspect.currentframe().f_back.f_lineno)
        else:
            self._time_marks_info.append(label)

    def add(self, other):
        """Add the time taken by another TimingMonitor to this one"""
        if self._monitoring is not None:
            raise RuntimeError("Cannot add timings to a TimingMonitor that is currently monitoring a procedure")

        for c in other.labels_by_class.keys():
            labels = other.labels_by_class[c]
            timings = other.timings_by_class[c]
            self._add_run_to_running_totals(c, np.cumsum(np.concatenate(([0.0],timings))), labels)

    def report_to_log(self, logger):
        if len(self.timings_by_class) == 0:
            return
        logger.info("")
        logger.info(f"CUMULATIVE {self._label.upper()}, summed over all processes, if applicable")
        v_tot = 1e-10
        for k, v in self.timings_by_class.items():
            v_tot += sum(v)

        for k, v in self.timings_by_class.items():
            name = str(k)[:-2]
            name = name.split(".")[-1]
            name = ("%"+str(self._num_chars)+"s ") % (name[-self._num_chars:])
            if self._show_percentages:
                logger.info(" " + name + f"{self.format_time(sum(v)):>12} | {100 * sum(v) / v_tot:4.1f}%")
            else:
                logger.info(" " + name + f"{self.format_time(sum(v)):>12}")
            if len(v)>1:
                marks_info = self.labels_by_class[k]
                logger.info("  ------------ INTERNAL BREAKDOWN ------------" )
                for i, this_v in enumerate(v):
                    logger.info(("    {:>8s} {:<8s} {:>12} | {:4.1f}% | {:4.1f}%").format(
                                 marks_info[i], marks_info[i + 1],
                                 self.format_time(this_v), 100 * this_v / sum(v), 100 * this_v / v_tot))
                logger.info("  --------------------------------------------")

        logger.info("")

    @classmethod
    def format_time(cls, time_in_seconds):
        """Returns a formatted time with sensible accuracy, e.g.

        0.124 -> 0.12s
        12.51 -> 12.5s
        123.4 -> 2m 3s
        3715.22 -> 1h 1m 55s
        """

        if time_in_seconds < 1:
            return "%.2fs" % time_in_seconds
        elif time_in_seconds < 60:
            return "%.1fs" % time_in_seconds
        elif time_in_seconds < 3600:
            return "%dm %.0fs" % (time_in_seconds // 60, time_in_seconds % 60)
        else:
            return "%dh %dm %.0fs" % (time_in_seconds // 3600, (time_in_seconds % 3600) // 60, time_in_seconds % 60)

    def __eq__(self, other):
        if type(other) != type(self):
            return False

        if self.timings_by_class.keys() != other.timings_by_class.keys():
            return False

        for k in self.timings_by_class.keys():
            if not np.all(self.timings_by_class[k] == other.timings_by_class[k]):
                return False

        return True
