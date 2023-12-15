class MessageMetaClass(type):
    _message_classes = {}
    _next_tag = 100

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

        msg, source, tag = backend.receive_any(source=None)
        obj = Message.interpret_and_deserialize(tag, source, msg)

        if not isinstance(obj, cls):
            if hasattr(obj, "_is_exception"):
                raise obj.contents
            else:
                raise RuntimeError("Unexpected message of type %r received"%type(obj))
        return obj

    def process(self):
        raise NotImplementedError("No process implemented for this message")

class ServerResponseMessage(Message):
    pass

class MessageWithResponse(Message):
    """An extension of the message class where the server can return a response to each process"""
    def respond(self, response):
        return ServerResponseMessage(response).send(self.source)

    def send_and_get_response(self, destination):
        self.send(destination)
        return self.get_response(destination)

    def get_response(self, receiving_from):
        return ServerResponseMessage.receive(receiving_from).contents

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
        response = ServerResponseMessage(response)
        for i in range(1, backend.size()):
            response.send(i)


class ExceptionMessage(Message):
    _is_exception = True

    def process(self):
        raise self.contents
