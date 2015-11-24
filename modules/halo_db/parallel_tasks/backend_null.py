def send(data, destination, tag=0):
    raise RuntimeError, "Cannot send data to another CPU: parallelism is disabled"

def receive(source=None, tag=0):
    raise RuntimeError, "Cannot receive data from another CPU: parallelism is disabled"

def rank():
    return 0

def size():
    return 1

def barrier():
    pass

def finalize():
    pass

def launch_new_task():
    raise RuntimeError, "Cannot launch new task: parallelism is disabled"