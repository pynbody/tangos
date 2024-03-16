import time

import pytest

from tangos import parallel_tasks as pt
from tangos.parallel_tasks import async_message, message, testing


class Response(message.Message):
    pass


class SlowProcessingMessage(async_message.AsyncProcessedMessage):
    def process_async(self):
        time.sleep(0.1)
        Response("slow").send(self.source)

class FastProcessingMessage(message.Message):
    def process(self):
        Response("fast").send(self.source)

def _test_async_message():
    SlowProcessingMessage().send(0)
    FastProcessingMessage().send(0)
    msg = Response.receive(0)
    # fast message response should overtake slow message response
    assert msg.contents == "fast"

    msg = Response.receive(0)
    assert msg.contents == "slow"


@pytest.mark.skip("Async processing is currently switched off")
def test_async_message():
    pt.use('multiprocessing-2')
    pt.launch(_test_async_message)
