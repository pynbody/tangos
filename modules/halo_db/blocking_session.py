import time
import sqlalchemy
from sqlalchemy.orm import Session

__author__ = 'app'


class BlockingSession(Session):
    def _execute_with_retries(self, fn, *params, **kwparams):
        retries = 20
        while retries > 0:
            try:
                return fn(*params, **kwparams)
            except sqlalchemy.exc.OperationalError:
                super(BlockingSession,self).rollback()
                retries -= 1
                if retries > 0:
                    print "DB is locked (%d attempts remain)..." % retries
                    time.sleep(1)
                else:
                    raise

    def flush(self, *params, **kwparams):
        return self._execute_with_retries(super(BlockingSession, self).flush, *params, **kwparams)

    def execute(self, *params, **kwparams):
        return self._execute_with_retries(super(BlockingSession, self).execute, *params, **kwparams)