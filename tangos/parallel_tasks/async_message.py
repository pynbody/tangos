import queue
from threading import Thread

from .. import config
from ..log import logger
from . import on_exit_parallelism
from .message import Message


class AsyncProcessedMessage(Message):
    _async_task_queue = queue.Queue()
    def process_async(self):
        """Override to provide the processing/response mechanism, that will be performed in a separate thread"""
        raise NotImplementedError()

    def process(self):
        if config.enable_async_message_processing:
            self._async_task_queue.put(self)
        else:
            self.process_async()

def init_async_processing_thread():
    if not config.enable_async_message_processing:
        return
    def async_processing_thread():
        while True:
            msg = AsyncProcessedMessage._async_task_queue.get()
            if msg is None:
                break
            try:
                msg.process_async()
            except Exception as e:
                print(f"Error processing async message {msg}: {e}")
                logger.error(f"Error processing async message {msg}: {e}")

    t = Thread(target=async_processing_thread)
    t.daemon = True
    t.start()

    def exit_async_processing_thread():
        AsyncProcessedMessage._async_task_queue.empty()
        AsyncProcessedMessage._async_task_queue.put(None)
        t.join()

    on_exit_parallelism(exit_async_processing_thread)
