import os


def read_datasets(basedir,filename):
    if os.path.exists(os.path.join(basedir, "datasets.txt")):
        with open(os.path.join(basedir, "datasets.txt")) as f:
            for l in f:
                if filename in l.split()[0]:
                    return int(l.split()[1])
    raise AssertionError("Unable to open datasets.txt")
