from __future__ import absolute_import
from . import message
from six.moves import range

awaiting_barrier = None


class MessageBarrierPass(message.Message):
    pass

class MessageBarrier(message.Message):
    def process(self):
        from . import backend
        global awaiting_barrier
        if awaiting_barrier is None:
            awaiting_barrier = [False for i in range(backend.size())]

        awaiting_barrier[self.source] = True
        if all(awaiting_barrier[1:]):
            for i in range(1, backend.size()):
                MessageBarrierPass().send(i)
                awaiting_barrier = [False for i in range(backend.size())]


def barrier():
    from . import backend
    assert backend.rank()!=0, "The server process cannot take part in a barrier"
    MessageBarrier().send(0)
    MessageBarrierPass.receive(0)
