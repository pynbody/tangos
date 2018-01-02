from __future__ import absolute_import
import contextlib
import time
import inspect
import numpy as np
import six

class TimingMonitor(object):
    """This class keeps track of how long a Property is taking to evaluate, and (if the Property is implemented
    to take advantage of this), the time spent on sub-tasks. It provides formatting to place this information
    into the log."""
    def __init__(self):
        self.timings_by_class = {}
        self.labels_by_class = {}
        self._monitoring = None

    @contextlib.contextmanager
    def __call__(self, object):
        self._start(object)
        yield
        self._end()

    def check_compatible_object(self, object):
        if not hasattr(object, 'timing_monitor'):
            raise TypeError("TimingMonitor requires a compatible object to monitor")

    def _set_as_monitor_for(self, object):
        self.check_compatible_object(object)
        if self._monitoring is not None:
            raise RuntimeError(
                "TimingMonitor is already monitoring %r and cannot also monitor %r" % (self._monitoring, object))
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
        cl = type(self._monitoring)
        self._unset_as_monitor_for(self._monitoring)
        self._time_marks_info.append("end")
        self._time_marks.append(time.time())
        previous_timings = self.timings_by_class.get(cl, 0)
        cumulative_timings = np.diff(self._time_marks)+previous_timings
        self.timings_by_class[cl] = cumulative_timings
        self.labels_by_class[cl] = self._time_marks_info


    def mark(self, label=None):
        """Mark a named event so that more detailed timing can be given"""
        self._time_marks.append(time.time())
        if label is None:
            self._time_marks_info.append(
                inspect.currentframe().f_back.f_lineno)
        else:
            self._time_marks_info.append(label)

    def summarise_timing(self, logger):
        logger.info("CUMULATIVE RUNNING TIMES (just this node)")
        v_tot = 1e-10
        for k, v in six.iteritems(self.timings_by_class):
            v_tot += sum(v)

        for k, v in six.iteritems(self.timings_by_class):
            name = str(k)[:-2]
            name = name.split(".")[-1]
            name = "%20s " % (name[-20:])
            if len(v)>1:
                marks_info = self.labels_by_class[k]
                logger.info(" " + name + "%.1fs | %.1f%%" % (sum(v), 100 * sum(v) / v_tot))
                logger.info("  ------ INTERNAL BREAKDOWN ------" )
                for i, this_v in enumerate(v):
                    logger.info((" %8s %8s %.1fs | %.1f%% | %.1f%%") %
                                (marks_info[i], marks_info[i + 1],
                                 this_v, 100 * this_v / sum(v), 100 * this_v / v_tot))
                logger.info("  --------------------------------")
            else:
                logger.info(name + "%.1fs | %.1f%%" % (v[0], 100 * v[0] / v_tot))

