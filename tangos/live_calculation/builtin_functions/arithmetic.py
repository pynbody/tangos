from __future__ import absolute_import
from .. import BuiltinFunction, FixedNumericInput
import numpy as np
import functools
from six.moves import zip

@BuiltinFunction.register
def abs(halos, vals):
    return arithmetic_unary_op(vals, functools.partial(np.linalg.norm, axis=-1))

@BuiltinFunction.register
def sqrt(halos, vals):
    return arithmetic_unary_op(vals, np.sqrt)

@BuiltinFunction.register
def log(halos, vals):
    return arithmetic_unary_op(vals, np.log)

@BuiltinFunction.register
def log10(halos, vals):
    return arithmetic_unary_op(vals, np.log10)

@BuiltinFunction.register
def subtract(halos, vals1, vals2):
    return arithmetic_binary_op(vals1, vals2, np.subtract)

@BuiltinFunction.register
def add(halos, vals1, vals2):
    return arithmetic_binary_op(vals1, vals2, np.add)

@BuiltinFunction.register
def divide(halos, vals1, vals2):
    return arithmetic_binary_op(vals1, vals2, np.divide)

@BuiltinFunction.register
def multiply(halos, vals1, vals2):
    return arithmetic_binary_op(vals1, vals2, np.multiply)

@BuiltinFunction.register
def greater(halos, vals1, vals2):
    return arithmetic_binary_op(vals1, vals2, np.greater)

@BuiltinFunction.register
def less(halos, vals1, vals2):
    return arithmetic_binary_op(vals1, vals2, np.less)

@BuiltinFunction.register
def logical_and(halos, vals1, vals2):
    return arithmetic_binary_op(vals1, vals2, np.logical_and)

@BuiltinFunction.register
def logical_or(halos, vals1, vals2):
    return arithmetic_binary_op(vals1, vals2, np.logical_or)

@BuiltinFunction.register
def logical_not(halos, vals):
    return arithmetic_unary_op(vals, np.logical_not)

@BuiltinFunction.register
def power(halos, vals1, vals2):
    return arithmetic_binary_op(vals1, vals2, np.power)

def arithmetic_binary_op(vals1, vals2, op):
    results = []
    for v1,v2 in zip(vals1, vals2):
        if v1 is not None and v2 is not None:
            v1 = np.asarray(v1, dtype=float)
            v2 = np.asarray(v2, dtype=float)
            result = op(v1,v2)
        else:
            result = None
        results.append(result)
    return results

def arithmetic_unary_op(vals1, op):
    results = []
    for v1 in vals1:
        if v1 is not None:
            v1 = np.asarray(v1, dtype=float)
            result = op(v1)
        else:
            result = None
        results.append(result)
    return results