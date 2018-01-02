from __future__ import absolute_import
import contextlib
import six
import struct
import hashlib


def _stable_hash(string):
    return struct.unpack('<L', hashlib.md5(string.encode()).digest()[:4])[0]

class MessageMetaClass(type):
    _message_classes = {}

    def __new__(meta, name, bases, dct):
        return super(MessageMetaClass, meta).__new__(meta, name, bases, dct)

    def __init__(cls, name, bases, dct):
        super(MessageMetaClass, cls).__init__(name, bases, dct)
        MessageMetaClass.register_class(cls)


    @staticmethod
    def class_to_hash(cls):
        result = _stable_hash(cls.__name__) & 0xfffffff
        return result

    @staticmethod
    def hash_to_class(hash):
        if hash not in MessageMetaClass._message_classes:
            raise RuntimeError("Unknown message receieved")

        return MessageMetaClass._message_classes[hash]

    @staticmethod
    def class_is_known(cls):
        return MessageMetaClass.class_to_hash(cls) in MessageMetaClass._message_classes

    @staticmethod
    def register_class(cls):
        if MessageMetaClass.class_is_known(cls):
            raise AttributeError("Attempting to register duplicate message class")
        MessageMetaClass._message_classes[MessageMetaClass.class_to_hash(cls)] = cls
        cls._tag = MessageMetaClass.class_to_hash(cls)


class Message(six.with_metaclass(MessageMetaClass, object)):
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
        obj = MessageMetaClass.hash_to_class(tag).deserialize(source, message)
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