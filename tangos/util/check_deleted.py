from __future__ import absolute_import
import contextlib, weakref, sys, gc
from ..log import logger

@contextlib.contextmanager
def check_deleted(a):
    if a is None:
        yield
        return
    else:
        a_s = weakref.ref(a)
        #sys.exc_clear()
        del a
        yield
        gc.collect()
        if a_s() is not None:
            logger.error("check_deleted failed")
            logger.error("gc reports hanging references: %s", gc.get_referrers(a_s()))