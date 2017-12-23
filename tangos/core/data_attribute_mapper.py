"""Logic to help flexibly get/set data in ORM objects (specifically HaloProperty and SimulationProperty) where data
of different types has to be stored in different attributes.
"""

from __future__ import absolute_import
import numpy as np
import zlib
import time
import datetime
import six
import sys
import functools
from six.moves import cPickle as pickle

pickle_loads = pickle.loads
if int(sys.version[0])==3:
    pickle_loads = functools.partial(pickle.loads, encoding='latin1')


_THRESHOLD_FOR_COMPRESSION = 1000

def get_data_of_unknown_type(obj):
    """Starting from the ORM object, extract data which may be stored in a variety of attributes depending on its type"""
    mapper = DataAttributeMapper(db_object=obj)
    return mapper.get(obj)

def set_data_of_unknown_type(obj, data):
    """Store the given data in the ORM object, using an appropriate attribute for the given data type"""
    mapper = DataAttributeMapper(data=data)
    mapper.set(obj,data)


class DataAttributeMapper(object):
    _order = 0
    # this can be used to force a subclass to be 'found' last
    # see __all_subclasses below, and use in NullAttributeMapper
    #
    # Except for a particular branch of py 3.5 it seems that __subclasses__ always
    # returns in order that classes were defined anyway, so it is not clear why this
    # is necessary.

    def __new__(cls, db_object=None, data=None):
        if db_object is None and data is None:
            subclass=NullAttributeMapper
        if data is None:
            subclass=cls._subclass_from_db_object(db_object)
        else:
            subclass=cls._subclass_from_data(data)

        return object.__new__(subclass)

    @classmethod
    def __all_subclasses(cls):
        subclasses = cls.__subclasses__()
        subclasses.sort(key = lambda x: x._order)
        for X in subclasses:
            yield X
            for Y in X.__all_subclasses():
                yield Y

    @classmethod
    def __all_nonabstract_subclasses(cls):
        X= [sub for sub in cls.__all_subclasses() if hasattr(sub,"_attribute_name")]
        return X

    @classmethod
    def _subclass_from_db_object(cls, db_object):
        for subclass in cls.__all_nonabstract_subclasses():
            if subclass._handles_db_object(db_object):
                return subclass

        # NullAttributeMapper should prevent the following line from being reached
        assert False, "Internal error in data_attribute_mapper: no subclass claimed responsibility for object"

    @classmethod
    def _subclass_from_data(cls, data):
        for subclass in cls.__all_nonabstract_subclasses():
            if subclass._handles_data(data):
                return subclass
        raise TypeError("Don't know how to store data of type %r"%type(data))

    @classmethod
    def _handles_db_object(cls, db_object):
        if cls._attribute_name is None:
            return True
        return getattr(db_object, cls._attribute_name, None) is not None

    @classmethod
    def _handles_data(cls, data):
        return any([isinstance(data, t) for t in cls._handled_types])

    def pack(self, data):
        return data

    def unpack(self,data):
        return data

    def _clear_other_attributes(self, db_object):
        for cls in DataAttributeMapper.__all_nonabstract_subclasses():
            if not isinstance(self, cls) and cls._attribute_name is not None:
                setattr(db_object, cls._attribute_name, None)

    def set(self, db_object, data):
        if hasattr(db_object,self._attribute_name):
            setattr(db_object, self._attribute_name, self.pack(data))
        else:
            raise TypeError("%r object does not have a slot for %r"%(type(db_object),self._attribute_name))

        self._clear_other_attributes(db_object)

    def get(self, db_object):
        return self.unpack(getattr(db_object, self._attribute_name))

class TimeAttributeMapper(DataAttributeMapper):
    _attribute_name = "data_time"
    _handled_types = [datetime.datetime, time.struct_time]

    def pack(self, data):
        if isinstance(data, time.struct_time):
            data = datetime.datetime(*data[:6])
        return data

class StringAttributeMapper(DataAttributeMapper):
    _attribute_name = "data_string"
    _handled_types = [str, six.text_type]

class ArrayDowncastingAttributeMapper(DataAttributeMapper):
    @classmethod
    def _handles_data(cls, data):
        if any([isinstance(data, t) for t in cls._handled_types]):
            return True
        if isinstance(data, list) and len(data)==1 and cls._handles_data(data[0]):
            return True
        if isinstance(data, np.ndarray) and len(data.shape)==0 and \
            any([data.dtype == t for t in cls._handled_types]):
            return True
        return False

    def pack(self, data):
        if isinstance(data,list):
            return data[0]
        else:
            return self._handled_types[0](data)

class FloatAttributeMapper(ArrayDowncastingAttributeMapper):
    _attribute_name = "data_float"
    _handled_types = [float, np.float32, np.float64, np.float128]


class IntAttributeMapper(ArrayDowncastingAttributeMapper):
    _attribute_name = "data_int"
    _handled_types = [int, np.int32, np.int64]


class ArrayAttributeMapper(DataAttributeMapper):
    _attribute_name = "data_array"
    _handled_types = [list, np.ndarray]
    _order = 1 # must be used only when downcasting mappers have failed

    def _unpack_compressed(self, packed):
        return pickle_loads(zlib.decompress(packed[2:]))

    def _unpack_uncompressed(self, packed):
        return pickle_loads(packed[2:])

    def _unpack_old_format(self, packed):
        return np.frombuffer(packed)

    def unpack(self, packed):
        if len(packed)==0:
            return None
        elif packed.startswith(b"ZX"):
            return self._unpack_compressed(packed)
        elif packed.startswith(b"PX"):
            return self._unpack_uncompressed(packed)
        else:
            return self._unpack_old_format(packed)

    def pack(self, data):
        dumped_st = pickle.dumps(data)
        if len(dumped_st) > _THRESHOLD_FOR_COMPRESSION:
            dumped_st = b"ZX" + zlib.compress(dumped_st)
        else:
            dumped_st = b"PX" + dumped_st
        return dumped_st


# Following must be defined last to act as a fall-through:
class NullAttributeMapper(DataAttributeMapper):
    _handled_types = [type(None)]
    _attribute_name = None
    _order = 2 # see discussion in the DataAttributeMapper

    def set(self, db_object, data):
        assert data is None
        self._clear_other_attributes(db_object)

    def get(self, db_object):
        return None

__all__ = ['get_data_of_unknown_type', 'set_data_of_unknown_type']
