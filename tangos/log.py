from __future__ import absolute_import
import logging
from six import StringIO
import copy

logger = logging.getLogger(__name__)

logger.setLevel(logging.INFO)
handler_stderr = logging.StreamHandler()
handler_stderr.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s : %(message)s")
handler_stderr.setFormatter(formatter)
logger.addHandler(handler_stderr)


class LogCapturer(object):
    def __init__(self):
        self.buffer = StringIO()
        self.handler_buffer = logging.StreamHandler(self.buffer)
        self.handler_buffer.setLevel(logging.INFO)
        self._suspended_handlers = []

    def __enter__(self):
        self._suspended_handlers = copy.copy(logger.handlers)
        for x_handler in self._suspended_handlers:
            logger.removeHandler(x_handler)
        logger.addHandler(self.handler_buffer)

    def __exit__(self, *exc_info):
        for x_handler in self._suspended_handlers:
            logger.addHandler(x_handler)
        self._suspended_handlers = []
        logger.removeHandler(self.handler_buffer)

    def get_output(self):
        return self.buffer.getvalue()


def set_identity_string(identifier):
    global handler_stderr
    formatter = logging.Formatter(identifier+"%(asctime)s : %(message)s")
    handler_stderr.setFormatter(formatter)