import os


def read_datasets(basedir,filename):
    if os.path.exists(os.path.join(basedir, "datasets.txt")):
        with open(os.path.join(basedir, "datasets.txt")) as f:
            for l in f:
                if l.split()[0].endswith(filename):
                    return int(l.split()[1])
    else:
        raise AssertionError("Unable to open datasets.txt")
