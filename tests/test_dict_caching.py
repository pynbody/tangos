import halo_db.core.dictionary

__author__ = 'app'

import halo_db as db

def setup():
    db.init_db("sqlite://")


def test_set():
    bh_obj = halo_db.core.dictionary.get_or_create_dictionary_item(db.core.get_default_session(), "BH")
    assert bh_obj is not None
    bh_obj2 = halo_db.core.dictionary.get_or_create_dictionary_item(db.core.get_default_session(), "BH")
    assert bh_obj2 is bh_obj