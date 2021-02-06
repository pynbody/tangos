import tangos.util.timing_monitor as tm
import logging
from tangos.log import LogCapturer, logger

class Dummy():
    pass

def test_timing_graceful_fail():
    """Issue #135: TimingMonitor could crash when monitoring procedures which themselves terminated early"""
    x=Dummy()
    x.timing_monitor=None
    TM = tm.TimingMonitor()


    lc = LogCapturer()

    for i in range(5):
        with TM(x):
            TM.mark("hello")
            TM.mark("goodbye")

    with lc:
        TM.summarise_timing(logger)

    assert "INTERNAL BREAKDOWN" in lc.get_output()
    assert "hello" in lc.get_output()
    assert "goodbye" in lc.get_output()

    # Now intentionally break the time measurements by failing to mark 'hello' and 'goodbye'
    with TM(x):
        pass

    lc = LogCapturer()

    with lc:
        TM.summarise_timing(logger)

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
        TM.summarise_timing(logger)

    assert "hello" not in lc.get_output()
    assert "goodbye" not in lc.get_output()
    assert "INTERNAL BREAKDOWN" not in lc.get_output()
    assert "Dummy" in lc.get_output()
