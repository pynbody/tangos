# Remote import message allows a client to ask the server to import a module.
# Currently used by the pynbody server (which cannot be loaded by default as it
# in turn imports pynbody, which cannot be a dependency)

import importlib

from .message import Message


class ImportRequestMessage(Message):
    def process(self):
        importlib.import_module(self.contents)
