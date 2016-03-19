import halo_db as db
import sqlalchemy

def fix_reverse_links(session, name_forwards, name_reverse):
    """Remove any existing links with given name_reverse, and regenerate them from the links called name_forwards
    """
    dict_forwards = db.core.get_or_create_dictionary_item(db.core.internal_session, name_forwards)
    dict_reverse = db.core.get_or_create_dictionary_item(db.core.internal_session, name_reverse)
    session.query(db.core.HaloLink).filter_by(relation_id=dict_reverse.id).delete()
    session.commit()
    connection = session.connection()

    query = session.query(db.core.HaloLink.halo_to_id, db.core.HaloLink.halo_from_id,
                          sqlalchemy.literal(dict_reverse.id),db.core.HaloLink.weight).filter_by(relation_id=dict_forwards.id)
    insert = db.core.HaloLink.__table__.insert().\
                       from_select(['halo_from_id','halo_to_id','relation_id','weight'],
                                   query)
    print insert
    connection.execute(insert)
    session.commit()

fix_reverse_links(db.core.internal_session,"BH_central","host_halo")
