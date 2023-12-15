from .message import Message

FILENAME = "parallel_tasks_test_log.txt"

def initialise_log():
    with open(FILENAME, "w") as f:
        f.write("")

def get_log(remove_process_ids=False):
    if remove_process_ids:
        processor = lambda s: s.strip()[4:]
    else:
        processor = lambda s: s.strip()

    with open(FILENAME) as f:
        return [processor(s) for s in f.readlines()]

def log(message):
    ServerLogMessage(message).send(0)

class ServerLogMessage(Message):
    def process(self):
        with open(FILENAME, "a") as f:
            f.write(f"[{self.source:d}] {self.contents:s}\r\n")
