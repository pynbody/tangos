from __future__ import absolute_import
import contextlib, weakref, sys, gc
from ..log import logger

class CheckDeleted(object):
    def __init__(self, obj):
        if obj is not None:
            self._obj_weakref = weakref.ref(obj)
        else:
            self._obj_weakref = None

    def __enter__(self):
        pass

    def __exit__(self, *_):
        gc.collect()
        if self._obj_weakref is not None and self._obj_weakref() is not None:
            logger.error("check_deleted failed")
            referrers = gc.get_referrers(self._obj_weakref())
            formatted_referrers = ", ".join(["<%s id=%d>" % (type(o), id(o)) for o in referrers])
            logger.error("gc reports hanging references: %s", formatted_referrers)
        self._obj_weakref = None

check_deleted = CheckDeleted


