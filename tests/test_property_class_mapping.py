import tangos
import tangos.input_handlers as soh
import tangos.properties as prop
from nose.tools import assert_raises

class TestOutputHandler1(soh.SimulationOutputSetHandler):
    pass

class TestOutputHandler1Child(TestOutputHandler1):
    pass

class TestOutputHandler2(soh.SimulationOutputSetHandler):
    pass

class PropertyForHandler1(prop.HaloProperties):
    works_with_handler = TestOutputHandler1
    requires_particle_data = True
    names = "widget", "hedgehog"

class PropertyForHandler2(prop.HaloProperties):
    works_with_handler = TestOutputHandler2
    requires_particle_data = True
    names = "widget", "robin"

class PropertyForHandler1Child(prop.HaloProperties):
    works_with_handler = TestOutputHandler1Child
    requires_particle_data = True
    names = "hedgehog", "satsuma"

class PropertyForLiveUse(prop.LiveHaloProperties):
    names = "livewidget"

def test_setup():
    assert issubclass(TestOutputHandler1Child, TestOutputHandler1)
    assert not issubclass(TestOutputHandler1, TestOutputHandler1Child)

def test_map_unique_name():
    assert prop.providing_class("robin", TestOutputHandler2) is PropertyForHandler2
    assert prop.providing_class("hedgehog", TestOutputHandler1) is PropertyForHandler1

def test_map_overloaded_name():
    assert prop.providing_class("widget", TestOutputHandler1) is PropertyForHandler1
    assert prop.providing_class("widget", TestOutputHandler2) is PropertyForHandler2

def test_map_precedence():
    # The most specific version of the property should be used, not the "more generic" version (PropertyForHandler1):
    assert prop.providing_class("hedgehog", TestOutputHandler1Child) is PropertyForHandler1Child


def test_map_nonexistent_name():
    with assert_raises(NameError):
        prop.providing_class("widget", soh.SimulationOutputSetHandler)
    with assert_raises(NameError):
        prop.providing_class("robin", TestOutputHandler1)

def test_map_liveproperty():
    assert prop.providing_class("livewidget") is PropertyForLiveUse

    with assert_raises(NameError):
        prop.providing_class("widget") # unavailable as a live property
