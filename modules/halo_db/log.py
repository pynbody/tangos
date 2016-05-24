import logging
import cStringIO

logger = logging.getLogger(__name__)

logger.setLevel(logging.INFO)
handler_stderr = logging.StreamHandler()
handler_stderr.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s : %(message)s")
handler_stderr.setFormatter(formatter)
logger.addHandler(handler_stderr)


class LogCapturer(object):
    def __init__(self):
        self.buffer = cStringIO.StringIO()
        self.handler_buffer = logging.StreamHandler(self.buffer)
        self.handler_buffer.setLevel(logging.INFO)
        self._suspended_handlers = []

    def __enter__(self):
        logger.addHandler(self.handler_buffer)
        self._suspended_handlers = logger.handlers
        for x_handler in self._suspended_handlers:
            logger.removeHandler(x_handler)

    def __exit__(self, *exc_info):
        for x_handler in self._suspended_handlers:
            logger.addHandler(x_handler)
        self._suspended_handlers = []
        logger.removeHandler(handler_stderr)

    def get_output(self):
        return self.buffer.getvalue()


def set_identity_string(identifier):
    global handler_stderr
    formatter = logging.Formatter(identifier+"%(asctime)s : %(message)s")
    handler_stderr.setFormatter(formatter)