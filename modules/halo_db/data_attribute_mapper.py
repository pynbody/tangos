import numpy as np
import pickle
import zlib
import pynbody
import time
import datetime

_data_attribute_names = "data_time", "data_string", "data_float", "data_int", "data_array"

def _unpack_array(packed):
    if len(packed) > 0:
        if packed[:2] == "ZX":
            return pickle.loads(zlib.decompress(packed[2:]))
        elif packed[:2] == "PX":
            try:
                x = pickle.loads(packed[2:])
            except KeyError:
                x = np.frombuffer(packed)
        else:
            # old style stored array
            x = np.frombuffer(packed)
        try:
            return float(x)
        except:
            return x
    else:
        return None

def get_data_of_unknown_type(obj):
    data_type = None
    for name in _data_attribute_names:
        data=getattr(obj, name, None)
        if data is not None:
            data_type = name
            break

    if data_type=="data_array":
        data=_unpack_array(data)

    return data


def set_data_of_unknown_type(obj, data):
    try:
        data = float(data)
    except:
        pass

    if type(data) is np.ndarray or type(data) is pynbody.array.SimArray:
        dumped_st = pickle.dumps(data)
        if len(dumped_st) > 1000:
            data = "ZX" + zlib.compress(dumped_st)
        else:
            data = "PX" + dumped_st
        data_type="array"
    elif type(data) is list:
        data = str(np.array(data, dtype=float).data)
        data_type="array"
    elif type(data) is float or type(data) is np.float64:
        data = data
        data_type="float"
    elif type(data) is str or type(data) is unicode:
        data = data
        data_type="string"
    elif type(data) is int:
        data = data
        data_type="int"
    elif type(data) is time.struct_time or type(data) is datetime.datetime:
        if type(data) is time.struct_time:
            data = datetime.datetime(*data[:6])
        data_type="time"
    else:
        raise TypeError("Not sure how to store type " + str(type(data)))

    for name in _data_attribute_names:
        if name.endswith(data_type):
            if hasattr(obj,name):
                setattr(obj, name, data)
            else:
                raise TypeError("Underlying ORM object does not have a slot for "+name)
        elif hasattr(obj,name):
            setattr(obj,name, None)