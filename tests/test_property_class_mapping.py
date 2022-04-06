import tangos
import tangos.input_handlers as soh
import tangos.properties as prop
from pytest import raises as assert_raises

class _TestOutputHandler1(soh.HandlerBase):
    pass

class _TestOutputHandler1Child(_TestOutputHandler1):
    pass

class _TestOutputHandler2(soh.HandlerBase):
    pass

class PropertyForHandler1(prop.PropertyCalculation):
    works_with_handler = _TestOutputHandler1
    requires_particle_data = True
    names = "widget", "hedgehog"

class PropertyForHandler2(prop.PropertyCalculation):
    works_with_handler = _TestOutputHandler2
    requires_particle_data = True
    names = "widget", "robin"

class PropertyForHandler1Child(prop.PropertyCalculation):
    works_with_handler = _TestOutputHandler1Child
    requires_particle_data = True
    names = "hedgehog", "satsuma"

class PropertyForLiveUse(prop.LivePropertyCalculation):
    names = "livewidget"

def test_setup():
    assert issubclass(_TestOutputHandler1Child, _TestOutputHandler1)
    assert not issubclass(_TestOutputHandler1, _TestOutputHandler1Child)

def test_map_unique_name():
    assert prop.providing_class("robin", _TestOutputHandler2) is PropertyForHandler2
    assert prop.providing_class("hedgehog", _TestOutputHandler1) is PropertyForHandler1

def test_map_overloaded_name():
    assert prop.providing_class("widget", _TestOutputHandler1) is PropertyForHandler1
    assert prop.providing_class("widget", _TestOutputHandler2) is PropertyForHandler2

def test_map_precedence():
    # The most specific version of the property should be used, not the "more generic" version (PropertyForHandler1):
    assert prop.providing_class("hedgehog", _TestOutputHandler1Child) is PropertyForHandler1Child


def test_map_nonexistent_name():
    with assert_raises(NameError):
        prop.providing_class("widget", soh.HandlerBase)
    with assert_raises(NameError):
        prop.providing_class("robin", _TestOutputHandler1)

def test_map_liveproperty():
    assert prop.providing_class("livewidget") is PropertyForLiveUse

    with assert_raises(NameError):
        prop.providing_class("widget") # unavailable as a live property
