from .message import Message

FILENAME = "parallel_tasks_test_log.txt"

def initialise_log():
    with open(FILENAME, "w") as f:
        f.write("")

def get_log():
    with open(FILENAME) as f:
        return f.readlines()

def log(message):
    ServerLogMessage(message).send(0)

class ServerLogMessage(Message):
    def process(self):
        with open(FILENAME, "a") as f:
            f.write(f"[{self.source:d}] {self.contents:s}\r\n")
