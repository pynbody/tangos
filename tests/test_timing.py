import logging
import time

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

    assert "Dummy 0.3s" in lc.get_output()
    assert "0.1s" in lc.get_output()

    TM.add(TM2)
    lc = LogCapturer()

    with lc:
        TM.report_to_log(logger)

    output = lc.get_output_without_timestamps()

    assert "Dummy 0.5s" in output
    assert "hello 0.2s" in output
    assert "end 0.3s" in output

def test_picklable():
    TM = tm.TimingMonitor()
    x = Dummy()

    with TM(x):
        time.sleep(0.1)
        TM.mark("hello")
        time.sleep(0.2)

    lc = LogCapturer()

    with lc:
        TM.report_to_log(logger)

    correct_results = lc.get_output_without_timestamps()

    import pickle
    TM2 = pickle.loads(pickle.dumps(TM))

    lc = LogCapturer()
    with lc:
        TM2.report_to_log(logger)

    assert lc.get_output_without_timestamps() == correct_results
