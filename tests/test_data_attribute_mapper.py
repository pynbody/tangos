import halo_db.data_attribute_mapper as dam
import numpy as np
import datetime
import pickle
import zlib
import pynbody
import time
from nose.tools import assert_raises

class TestTarget(object):
    all_types = "time","string","float","int","array"
    def __init__(self):
        self.create_placeholders()

    def create_placeholders(self):
        for t in self.all_types:
            setattr(self,"data_"+t,None)

    def assert_datatype(self, typename):
        for t in self.all_types:
            attr_name_t = "data_"+t
            data_thistype = getattr(self,attr_name_t)
            if t==typename:
                assert data_thistype is not None, "For type %r, attribute %r should contain data but is None"%(typename, attr_name_t)
            else:
                assert data_thistype is None, "For type %r, attribtue %r should be None but contains %r"%(typename, attr_name_t, data_thistype)

    @property
    def data(self):
        return dam.get_data_of_unknown_type(self)

    @data.setter
    def data(self, data):
        return dam.set_data_of_unknown_type(self, data)


class TestTargetNoStrings(TestTarget):
    all_types = "time","float","int","array"

test_values = {"string":"hello",
               "float":32.3,
               #"int":42,
               "time":datetime.datetime.now(),
               "array":np.array([2,3,6])}

def assert_data_value(data, testval):
    if hasattr(data,"shape"):
        assert all(data==testval)
    else:
        assert data==testval

def test_set_retrieve():

    for typename,testval in test_values.iteritems():
        target = TestTarget()
        target.data=testval
        target.assert_datatype(typename)
        assert_data_value(target.data,testval)

def test_set_retrieve_no_strings():
    for typename,testval in test_values.iteritems():
        target = TestTargetNoStrings()

        if typename=="string":
            assert_raises(TypeError,
                          setattr,target,'data',testval)
        else:
            target.data=testval
            target.assert_datatype(typename)
            assert_data_value(target.data,testval)

def test_set_reset_retrieve():

    for typename,testval in test_values.iteritems():
        target = TestTarget()
        target.data=testval
        target.assert_datatype(typename)
        assert_data_value(target.data,testval)
        for typename2,testval2 in test_values.iteritems():
            target.data=testval2
            target.assert_datatype(typename2)
            assert_data_value(target.data,testval2)

def test_auto_array_from_list():
    target = TestTarget()
    target.data=[1,2,3]
    target.assert_datatype("array")
    assert_data_value(target.data,[1,2,3])

def test_simarray():
    target = TestTarget()
    target.data=pynbody.array.SimArray([1,2,3],"kpc")
    target.assert_datatype("array")
    assert_data_value(target.data,[1,2,3])
    assert target.data.units=="kpc"

def test_struct_time():
    target = TestTarget()
    test_time = time.localtime()
    target.data=test_time
    target.assert_datatype("time")
    assert_data_value(target.data,  datetime.datetime(*test_time[:6]))

def test_array_pack_format():
    target = TestTarget()
    test_data=np.array([1,2,3])
    target.data=test_data
    assert target.data_array.startswith("PX")
    assert target.data_array.endswith(pickle.dumps(test_data))

    test_data=np.arange(2000)
    target.data=test_data
    assert target.data_array.startswith("ZX")
    assert target.data_array.endswith(zlib.compress(pickle.dumps(test_data)))

def test_none():
    target = TestTarget()
    assert target.data is None
    target.data=23
    assert target.data==23
    target.data=None
    assert target.data is None


def test_float_downcast_from_ndarray():
    data = np.array(3.0)
    target = TestTarget()
    target.data = data
    assert target.data==3.0
    target.assert_datatype("float")

def test_float_downcast_from_list():
    data = [3.0]
    target = TestTarget()
    target.data = data
    assert target.data==3.0
    target.assert_datatype("float")


def test_int_downcast_from_ndarray():
    data = np.array(3)
    target = TestTarget()
    target.data = data
    assert target.data==3
    target.assert_datatype("int")

def test_int_downcast_from_list():
    data = [3]
    target = TestTarget()
    target.data = data
    assert target.data==3
    target.assert_datatype("int")
