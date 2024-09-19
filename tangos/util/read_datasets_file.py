import os


def read_datasets(basedir, filename):
    dataset_file = os.path.join(basedir, "datasets.txt")
    if not os.path.exists(dataset_file):
        raise FileNotFoundError(dataset_file)

    with open(dataset_file) as f:
        for line in f:
            if filename in line.split()[0]:
                return int(line.split()[1])

    raise Exception(
        f"Dataset list {dataset_file} found, but requested "
        f"filename={filename} not found in it."
    )
