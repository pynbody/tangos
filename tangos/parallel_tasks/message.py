reception_timing_monitor = None

def _setup_message_reception_timing_monitor():
    global reception_timing_monitor

    from ..util import timing_monitor
    from . import backend
    if backend is None or backend.rank() == 0:
        # server can't gather its own timing information
        reception_timing_monitor = timing_monitor.TimingMonitor(allow_parallel=False, label='idle')
    else:
        reception_timing_monitor = timing_monitor.TimingMonitor(allow_parallel=True, label='response wait',
                                                                num_chars=40, show_percentages=False)
    return reception_timing_monitor

def update_performance_stats():
    from . import backend
    if reception_timing_monitor is not None and backend is not None:
        assert backend.rank() != 0
        reception_timing_monitor.report_to_log_or_server(None)

class MessageMetaClass(type):
    _message_classes = {}
    _next_tag = 100

    timing_monitor = None

    def __new__(meta, name, bases, dct):
        return super().__new__(meta, name, bases, dct)

    def __init__(cls, name, bases, dct):
        super().__init__(name, bases, dct)
        MessageMetaClass.register_class(cls, MessageMetaClass._next_tag)
        MessageMetaClass._next_tag+=1

    @staticmethod
    def tag_to_class(tag):
        if tag not in MessageMetaClass._message_classes:
            raise RuntimeError("Unknown message receieved (tag %d)" % tag)

        return MessageMetaClass._message_classes[tag]

    @staticmethod
    def class_is_known(cls):
        return cls in MessageMetaClass._message_classes.values()

    @staticmethod
    def register_class(cls, tag):
        if MessageMetaClass.class_is_known(cls):
            raise AttributeError("Attempting to register duplicate message class %r"%cls)
        MessageMetaClass._message_classes[tag] = cls
        cls._tag = tag

class MessageWithResponseMetaClass(MessageMetaClass):
    def __init__(cls, name, bases, dct):
        super().__init__(name, bases, dct)
        response_class = type(cls.__name__+"Response", (ServerResponseMessage,), {})
        assert response_class.__name__ not in globals()
        globals()[response_class.__name__] = response_class # for Pickle
        cls._response_class = response_class


class Message(metaclass=MessageMetaClass):
    _handler = None

    def __init__(self, contents=None):
        self.contents = contents
        self.source = None

    @classmethod
    def deserialize(cls, source, message):
        obj = cls(message)
        obj.source = source
        return obj

    def serialize(self):
        return self.contents

    @staticmethod
    def interpret_and_deserialize(tag, source, message):
        obj = MessageMetaClass.tag_to_class(tag).deserialize(source, message)
        return obj

    @staticmethod
    def process_incoming_message(tag, source, message):
        obj = Message.interpret_and_deserialize(tag, source, message)
        obj.process()

    def send(self, destination):
        from . import backend
        backend.send(self.serialize(), destination=destination, tag=self._tag)

    @classmethod
    def receive(cls, source=None):
        from . import backend
        global reception_timing_monitor

        if reception_timing_monitor is not None:
            with reception_timing_monitor(cls):
                msg, source, tag = backend.receive_any(source=None)
        else:
            msg, source, tag = backend.receive_any(source=None)

        obj = Message.interpret_and_deserialize(tag, source, msg)

        if not isinstance(obj, cls):
            if hasattr(obj, "_is_exception"):
                raise obj.contents
            else:
                raise RuntimeError("Unexpected message of type %r received"%type(obj))
        return obj

    def process(self):
        raise NotImplementedError(f"No process implemented for this message of type {type(self)}")

class ServerResponseMessage(Message):
    pass

class MessageWithResponse(Message, metaclass=MessageWithResponseMetaClass):
    """An extension of the message class where the server can return a response to each process"""
    def respond(self, response):
        return self._response_class(response).send(self.source)

    def send_and_get_response(self, destination):
        self.send(destination)
        return self.get_response(destination)

    def get_response(self, receiving_from):
        return self._response_class.receive(receiving_from).contents

class BarrierMessageWithResponse(MessageWithResponse):
    """An extension of the message class where the client blocks until all processes have made the request, and then the server responds"""
    _current_barrier_message = None
    def process(self):
        from . import backend
        if BarrierMessageWithResponse._current_barrier_message is None:
            BarrierMessageWithResponse._current_barrier_message = self
            BarrierMessageWithResponse._current_barrier_message._all_sources = [self.source]
        else:
            self.assert_consistent(BarrierMessageWithResponse._current_barrier_message)
            assert self.source not in BarrierMessageWithResponse._current_barrier_message._all_sources
            BarrierMessageWithResponse._current_barrier_message._all_sources.append(self.source)

        if len(BarrierMessageWithResponse._current_barrier_message._all_sources) == backend.size()-1:
            BarrierMessageWithResponse._current_barrier_message = None
            self.process_global()

    def process_global(self):
        raise NotImplementedError("No process implemented for this message")

    def assert_consistent(self, original_message):
        assert type(self) == type(original_message)
        assert self.contents == original_message.contents

    def respond(self, response):
        from . import backend
        response = self._response_class(response)
        for i in range(1, backend.size()):
            response.send(i)


class ExceptionMessage(Message):
    _is_exception = True

    def process(self):
        raise self.contents
