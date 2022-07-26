import sys

import numpy as np

is_none = np.vectorize(lambda x: x is None, "?")
is_not_none = np.vectorize(lambda x: x is not None, "?")
is_false = np.vectorize(lambda x: x is False, "?")
is_not_false = np.vectorize(lambda x: x is not False, "?")
