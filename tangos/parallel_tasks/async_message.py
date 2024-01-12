import queue
from threading import Thread

from . import on_exit_parallelism
from .message import Message


class AsyncProcessedMessage(Message):
    _async_task_queue = queue.Queue()
    def process_async(self):
        """Override to provide the processing/response mechanism, that will be performed in a separate thread"""
        raise NotImplementedError()

    def process(self):
        self._async_task_queue.put(self)

def init_async_processing_thread():
    def async_processing_thread():
        while True:
            msg = AsyncProcessedMessage._async_task_queue.get()
            if msg is None:
                break
            msg.process_async()

    t = Thread(target=async_processing_thread)
    t.daemon = True
    t.start()

    def exit_async_processing_thread():
        AsyncProcessedMessage._async_task_queue.empty()
        AsyncProcessedMessage._async_task_queue.put(None)
        t.join()

    on_exit_parallelism(exit_async_processing_thread)
