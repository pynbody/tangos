"""
Least-recently-used dictionary for caching

From https://gist.github.com/davesteele/44793cd0348f59f8fadd49d7799bd306

>>> import cache_dict
>>> c = cache_dict.CacheDict(cache_len=2)
>>> c[1] = 1
>>> c[2] = 2
>>> c[3] = 3
>>> c
CacheDict([(2, 2), (3, 3)])
>>> c[2]
2
>>> c[4] = 4
>>> c
CacheDict([(2, 2), (4, 4)])
>>>
"""

from collections import OrderedDict


class CacheDict(OrderedDict):
    """Dict with a limited length, ejecting LRUs as needed."""

    def __init__(self, *args, cache_len: int = 10, **kwargs):
        assert cache_len > 0
        self.cache_len = cache_len

        super().__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        super().move_to_end(key)

        while len(self) > self.cache_len:
            oldkey = next(iter(self))
            super().__delitem__(oldkey)

    def __getitem__(self, key):
        val = super().__getitem__(key)
        super().move_to_end(key)

        return val
