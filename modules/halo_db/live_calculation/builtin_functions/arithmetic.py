from .. import BuiltinFunction, FixedNumericInput
import numpy as np

@BuiltinFunction.register
def abs(halos, vals):
    return [np.linalg.norm(v, axis=-1) for v in vals]

@BuiltinFunction.register
def sqrt(halos, vals):
    return [np.sqrt(v) if v is not None else v for v in vals]

def arithmetic_op(vals1, vals2, op):
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

