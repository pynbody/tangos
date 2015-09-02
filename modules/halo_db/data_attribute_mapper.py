import numpy as np
import pickle
import zlib
import pynbody
import time
import datetime



_THRESHOLD_FOR_COMPRESSION = 1000

def get_data_of_unknown_type(obj):
    mapper = DataAttributeMapper(db_object=obj)
    return mapper.get(obj)

def set_data_of_unknown_type(obj, data):
    mapper = DataAttributeMapper(data=data)
    mapper.set(obj,data)



class DataAttributeMapper(object):
    def __new__(cls, db_object=None, data=None):
        if db_object is None and data is None:
            raise ValueError, "Either db_object or data must be specified"
        if data is None:
            subclass=cls._subclass_from_db_object(db_object)
        else:
            subclass=cls._subclass_from_data(data)

        return object.__new__(subclass)

    @classmethod
    def _subclass_from_db_object(cls, db_object):
        for subclass in cls.__subclasses__():
            if subclass._handles_db_object(db_object):
                return subclass
        raise TypeError, "No data found in object %r"%db_object

    @classmethod
    def _subclass_from_data(cls, data):
        for subclass in cls.__subclasses__():
            if subclass._handles_data(data):
                return subclass
        raise TypeError, "Don't know how to store data of type %r"%type(data)

    @classmethod
    def _handles_db_object(cls, db_object):
        return getattr(db_object, cls._attribute_name, None) is not None

    @classmethod
    def _handles_data(cls, data):
        return any([isinstance(data, t) for t in cls._handled_types])

    def pack(self, data):
        return data

    def unpack(self,data):
        return data

    def set(self, db_object, data):
        if hasattr(db_object,self._attribute_name):
            setattr(db_object, self._attribute_name, self.pack(data))
        else:
            raise TypeError("%r object does not have a slot for %r"%(type(db_object),self._attribute_name))

        for cls in DataAttributeMapper.__subclasses__():
            if not isinstance(self, cls):
                setattr(db_object, cls._attribute_name, None)

    def get(self, db_object):
        return self.unpack(getattr(db_object, self._attribute_name))

class TimeAttributeMapper(DataAttributeMapper):
    _attribute_name = "data_time"
    _handled_types = [datetime.datetime]

class StringAttributeMapper(DataAttributeMapper):
    _attribute_name = "data_string"
    _handled_types = [str, unicode]

class FloatAttributeMapper(DataAttributeMapper):
    _attribute_name = "data_float"
    _handled_types = [float, np.float32, np.float64, np.float128]

    def pack(self, data):
        return float(data)

class IntAttributeMapper(DataAttributeMapper):
    _attribute_name = "data_int"
    _handled_types = [int, np.int32, np.int64]

    def pack(self, data):
        return int(data)

class ArrayAttributeMapper(DataAttributeMapper):
    _attribute_name = "data_array"
    _handled_types = [list, np.ndarray, pynbody.array.SimArray]

    def _unpack_compressed(self, packed):
        return pickle.loads(zlib.decompress(packed[2:]))

    def _unpack_uncompressed(self, packed):
        return pickle.loads(packed[2:])

    def _unpack_old_format(self, packed):
        return np.frombuffer(packed)

    def unpack(self, packed):
        if len(packed)==0:
            return None
        elif packed.startswith("ZX"):
            return self._unpack_compressed(packed)
        elif packed.startswith("PX"):
            return self._unpack_uncompressed(packed)
        else:
            return self._unpack_old_format(packed)

    def pack(self, data):
        dumped_st = pickle.dumps(data)
        if len(dumped_st) > _THRESHOLD_FOR_COMPRESSION:
            dumped_st = "ZX" + zlib.compress(dumped_st)
        else:
            dumped_st = "PX" + dumped_st
        return dumped_st


__all__ = ['get_data_of_unknown_type', 'set_data_of_unknown_type']
