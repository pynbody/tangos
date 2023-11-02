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


class ExceptionMessage(Message):
    _is_exception = True

    def process(self):
        raise self.contents
