import halo_db.core.dictionary
from halo_db import testing

__author__ = 'app'

import halo_db as db

def setup():
    testing.init_blank_db_for_testing()


def test_set():
    bh_obj = halo_db.core.dictionary.get_or_create_dictionary_item(db.core.get_default_session(), "BH")
    assert bh_obj is not None
    bh_obj2 = halo_db.core.dictionary.get_or_create_dictionary_item(db.core.get_default_session(), "BH")
    assert bh_obj2 is bh_obj