__author__ = 'app'

import halo_db as db

def setup():
    db.init_db("sqlite://")


def test_set():
    bh_obj = db.get_or_create_dictionary_item(db.core.internal_session, "BH")
    assert bh_obj is not None
    bh_obj2 = db.get_or_create_dictionary_item(db.core.internal_session, "BH")
    assert bh_obj2 is bh_obj