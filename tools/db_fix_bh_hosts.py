import halo_db as db
import sqlalchemy

import halo_db.core.dictionary
import halo_db.core.halo
import halo_db.core.halo_data


def fix_reverse_links(session, name_forwards, name_reverse):
    """Remove any existing links with given name_reverse, and regenerate them from the links called name_forwards
    """
    dict_forwards = halo_db.core.dictionary.get_or_create_dictionary_item(db.core.get_default_session(), name_forwards)
    dict_reverse = halo_db.core.dictionary.get_or_create_dictionary_item(db.core.get_default_session(), name_reverse)
    session.query(halo_db.core.halo_data.HaloLink).filter_by(relation_id=dict_reverse.id).delete()
    session.commit()
    connection = session.connection()

    query = session.query(halo_db.core.halo_data.HaloLink.halo_to_id, halo_db.core.halo_data.HaloLink.halo_from_id,
                          sqlalchemy.literal(dict_reverse.id), halo_db.core.halo_data.HaloLink.weight).filter_by(relation_id=dict_forwards.id)
    insert = halo_db.core.halo_data.HaloLink.__table__.insert().\
                       from_select(['halo_from_id','halo_to_id','relation_id','weight'],
                                   query)
    print insert
    connection.execute(insert)
    session.commit()

fix_reverse_links(db.core.get_default_session(),"BH_central","host_halo")
