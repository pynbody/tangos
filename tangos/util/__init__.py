import numpy as np

is_none = np.vectorize(lambda x: x is None, "?")
is_not_none = np.vectorize(lambda x: x is not None, "?")