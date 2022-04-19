from tangos.log import LogCapturer
from tangos.util.check_deleted import check_deleted


class DerivedList(list):
    pass

def test_check_delete_none():
    a = None
    with check_deleted(a):
        del a


def test_check_delete():
    a = DerivedList([1,2,3])
    lc = LogCapturer()
    with lc:
        with check_deleted(a):
            del a
    assert "check_deleted" not in lc.get_output()

def test_hanging_ref_check_delete():
    a = DerivedList([1,2,3])
    b = a
    lc = LogCapturer()
    with lc:
        with check_deleted(a):
            del a
    assert "check_deleted" in lc.get_output()
