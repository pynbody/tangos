from __future__ import absolute_import
from .. import BuiltinFunction, FixedNumericInput
import scipy, scipy.stats
import math
import numpy as np

def single_array_smooth(ar_in, npix=5, one_tailed=False) :
    kern = scipy.stats.norm.pdf(np.linspace(-3,3,npix))

    if one_tailed :
        kern[:math.ceil(float(npix)/2)-1]=0
    kern/=kern.sum()
    # repeat edge values
    ar_in_extended = np.concatenate(([ar_in[0]]*npix,ar_in,[ar_in[-1]]*npix))
    return np.convolve(ar_in_extended, kern, 'same')[npix:-npix]

@BuiltinFunction.register
def array_smooth(halos, vals, smooth_npix):
    return [single_array_smooth(vals_i, smooth_npix) if vals_i is not None else None for vals_i in vals]
array_smooth.set_input_options(1, provide_proxy=True, assert_class = FixedNumericInput)

@BuiltinFunction.register
def element(halos, arrays, index):
    return [ar_i[index] if ar_i is not None else None for ar_i in arrays]
element.set_input_options(1, provide_proxy=True, assert_class = FixedNumericInput)