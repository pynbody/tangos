import numpy as np
import sys

is_none = np.vectorize(lambda x: x is None, "?")
is_not_none = np.vectorize(lambda x: x is not None, "?")
is_false = np.vectorize(lambda x: x is False, "?")
is_not_false = np.vectorize(lambda x: x is not False, "?")

if sys.version_info[0]<3:
    from ._lru_cache import lru_cache
else:
    from functools import lru_cache
