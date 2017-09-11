# Remote import message allows a client to ask the server to import a module.
# Currently used by the pynbody server (which cannot be loaded by default as it
# in turn imports pynbody, which cannot be a dependency)

from __future__ import absolute_import
from .message import Message
import importlib


class ImportRequestMessage(Message):
    def process(self):
        importlib.import_module(self.contents)
