from . import message


class SimpleBarrierMessage(message.BarrierMessageWithResponse):
    def process_global(self):
        self.respond(None)

def barrier():
    from . import backend, parallelism_is_active
    if not parallelism_is_active():
        return
    assert backend.rank()!=0, "The server process cannot take part in a barrier"
    SimpleBarrierMessage().send_and_get_response(0) # awaits response which only comes when all processes reach barrier
