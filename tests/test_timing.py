import logging
import time

import pytest

import tangos.util.timing_monitor as tm
from tangos.log import LogCapturer, logger


class Dummy():
    def __init__(self):
        self.timing_monitor = None

def test_timing_graceful_fail():
    """Issue #135: TimingMonitor could crash when monitoring procedures which themselves terminated early"""
    x=Dummy()
    TM = tm.TimingMonitor()


    lc = LogCapturer()

    for i in range(5):
        with TM(x):
            TM.mark("hello")
            TM.mark("goodbye")

    with lc:
        TM.report_to_log(logger)

    assert "INTERNAL BREAKDOWN" in lc.get_output()
    assert "hello" in lc.get_output()
    assert "goodbye" in lc.get_output()

    # Now intentionally break the time measurements by failing to mark 'hello' and 'goodbye'
    with TM(x):
        pass

    lc = LogCapturer()

    with lc:
        TM.report_to_log(logger)

    assert "hello" not in lc.get_output()
    assert "goodbye" not in lc.get_output()
    assert "INTERNAL BREAKDOWN" not in lc.get_output()
    assert "Dummy" in lc.get_output()


    # Now do a 'normal' run again - timing summary should continue to fail gracefully since it has no meaningful
    # accumulation of timing stats.
    with TM(x):
        TM.mark("hello")
        TM.mark("goodbye")

    with lc:
        TM.report_to_log(logger)

    assert "hello" not in lc.get_output()
    assert "goodbye" not in lc.get_output()
    assert "INTERNAL BREAKDOWN" not in lc.get_output()
    assert "Dummy" in lc.get_output()

def test_timing_add():
    TM = tm.TimingMonitor()
    TM2 = tm.TimingMonitor()
    x = Dummy()

    with TM(x):
        time.sleep(0.1)
        TM.mark("hello")
        time.sleep(0.2)

    with TM2(x):
        time.sleep(0.1)
        TM2.mark("hello")
        time.sleep(0.1)

    lc = LogCapturer()

    with lc:
        TM.report_to_log(logger)
        TM2.report_to_log(logger)

    assert "Dummy        0.3" in lc.get_output()
    assert "start hello           0.1" in lc.get_output()

    TM.add(TM2)
    lc = LogCapturer()

    with lc:
        TM.report_to_log(logger)

    output = lc.get_output_without_timestamps()

    assert "Dummy        0.5" in output
    assert "start hello           0.2" in output
    assert "hello end             0.3" in output

@pytest.fixture
def sample_timing_monitor():
    sample_timing_monitor = tm.TimingMonitor()
    x = Dummy()

    with sample_timing_monitor(x):
        time.sleep(0.1)
        sample_timing_monitor.mark("hello")
        time.sleep(0.2)

    return sample_timing_monitor
def test_picklable(sample_timing_monitor):
    lc = LogCapturer()

    with lc:
        sample_timing_monitor.report_to_log(logger)

    correct_results = lc.get_output_without_timestamps()

    import pickle
    TM2 = pickle.loads(pickle.dumps(sample_timing_monitor))

    lc = LogCapturer()
    with lc:
        TM2.report_to_log(logger)

    assert lc.get_output_without_timestamps() == correct_results

def test_time_formatting():
    results = [(0.151, "0.15s"),
               (12.46, "12.5s"),
               (61.3, "1m 1s"),
               (3702.51, "1h 1m 43s")
               ]

    for t, f in results:
        assert tm.TimingMonitor.format_time(t) == f



def test_report_if_needed(sample_timing_monitor):
    lc = LogCapturer()
    with lc:
        sample_timing_monitor.report_to_log_if_needed(logger)

    assert len(lc.get_output())>0

    lc = LogCapturer()
    with lc:
        sample_timing_monitor.report_to_log_if_needed(logger)

    assert len(lc.get_output()) == 0

    x = Dummy()
    with sample_timing_monitor(x):
        sample_timing_monitor.mark("hello")
        time.sleep(0.2)

    with lc:
        sample_timing_monitor.report_to_log_if_needed(logger)

    assert len(lc.get_output())>0

def test_report_if_needed_no_limitless_recursion(sample_timing_monitor):
    sample_timing_monitor.report_to_log_if_needed(logger)
    assert sample_timing_monitor._state_at_last_report is not None
    assert sample_timing_monitor._state_at_last_report._state_at_last_report is None
