from .message import Message

_remote_sets = {}
class RemoteSetOperation(Message):
    def process(self):
        set_id, operation, value = self.contents
        global _remote_sets
        if operation=="add-if-not-exists":
            result = LocalSet(set_id).add_if_not_exists(value)
            self.reply(result)
        else:
            raise ValueError("Unknown operation %s" % operation)

    def reply(self, result):
        RemoteSetResult(result).send(self.source)

class RemoteSetResult(Message):
    pass

class SharedSet:
    def __new__(cls, set_id, allow_parallel=False):
        if cls is SharedSet:
            from . import parallelism_is_active
            parallel = allow_parallel and parallelism_is_active()
            if parallel:
                cls = RemoteSet
            else:
                cls = LocalSet
        return object.__new__(cls)

    def __init__(self, set_id):
        self.set_id = set_id

    def __getnewargs__(self):
        return self.set_id,

    def add_if_not_exists(self, value):
        """Adds the value to the set, and returns True if it was already present, as an atomic operation"""
        raise NotImplementedError("Constructing a SharedSet should automatically return a RemoteSet or LocalSet as appropriate")

class RemoteSet(SharedSet):
    def __init__(self, set_id, allow_parallel=False):
        assert allow_parallel
        super().__init__(set_id)
        self._underlying_set = set()

    def add_if_not_exists(self, value):
        """Adds to the set, and returns a boolean indicating whether the value was already present"""
        RemoteSetOperation((self.set_id, "add-if-not-exists", value)).send(0)
        result = RemoteSetResult.receive(0).contents
        return result

class LocalSet(SharedSet):
    def __init__(self, set_id, allow_parallel=False):
        super().__init__(set_id)
        self._underlying_set = _remote_sets.get(set_id, set())
        _remote_sets[set_id] = self._underlying_set
    def add_if_not_exists(self, value):
        result = value in self._underlying_set
        if not result:
            self._underlying_set.add(value)
        return result
