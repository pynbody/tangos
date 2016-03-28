from .. import BuiltinFunction, FixedNumericInput
import numpy as np
import scipy, scipy.stats
import math

@BuiltinFunction.register
def abs(halos, vals):
    return [np.linalg.norm(v, axis=-1) for v in vals]

def arithmetic_op(vals1, vals2, op):
    return [op(v1,v2) if v1 is not None and v2 is not None else None for v1,v2 in zip(vals1,vals2)]

@BuiltinFunction.register
def subtract(halos, vals1, vals2):
    return arithmetic_op(vals1, vals2, np.subtract)

@BuiltinFunction.register
def add(halos, vals1, vals2):
    return arithmetic_op(vals1, vals2, np.add)

@BuiltinFunction.register
def divide(halos, vals1, vals2):
    return arithmetic_op(vals1, vals2, np.divide)

@BuiltinFunction.register
def multiply(halos, vals1, vals2):
    return arithmetic_op(vals1, vals2, np.multiply)


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