from __future__ import absolute_import
import numpy as np
from ..util import is_not_none


class QueryMask(object):
    """A helper class to remove rows that do not need to be queried, representing them by "None" in the final results"""
    def __init__(self):
        self.N = None

    def mark_nones_as_masked(self, input):
        """Mark any rows in the input that are None as masked, excluding them from future queries."""
        if self.N is None:
            self.N = len(input)
            to_mask = input
        else:
            to_mask = self.unmask(input)

        if len(to_mask.shape)==2:
            mask = np.any(is_not_none(to_mask),axis=0).reshape(self.N)
        elif len(to_mask.shape)==1:
            mask = is_not_none(to_mask)
        else:
            raise ValueError("Not able to use an input of this shape to determine a query mask")

        assert mask.shape==(self.N,)
        self.results_target = np.where(mask)

    def mask(self, input):
        """Mask an input array so that it only includes the rows that are to be queried"""
        self._check_ready()
        return input[self.results_target]

    def unmask(self, input, insert_value=None):
        """Unmask an output array so that it includes None for the rows that were not queried"""
        self._check_ready()
        rval_shape = list(input.shape)
        rval_shape[-1] = self.N
        rval = np.empty(rval_shape,dtype=object)
        rval[:] = insert_value
        maximal_slice = slice(None,None)
        address_tuple = tuple([maximal_slice]*(len(input.shape)-1) + [self.results_target[0]])
        rval[address_tuple] = input
        return rval

    def _check_ready(self):
        if self.N is None:
            raise RuntimeError("The query mask has not yet been configured")