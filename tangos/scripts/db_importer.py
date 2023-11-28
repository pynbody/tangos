import sys

import sqlalchemy
import tqdm
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .. import Base, Creator, DictionaryItem, core
from ..config import DB_IMPORT_CHUNK_SIZE, DB_IMPORT_COMMIT_AFTER_CHUNKS
from ..core import (HaloLink, HaloProperty, Simulation, SimulationObjectBase,
                    SimulationProperty, TimeStep)


def db_import(options):

    remote_db = options.file

    if "://" not in remote_db:
        remote_db = "sqlite:///"+remote_db

    engine2 = create_engine(remote_db, echo=False)
    ext_session = sessionmaker(bind=engine2)()

    _db_import_export(core.get_default_session(), ext_session)


def _db_import_export(target_session, from_session):
    """Copy all database entries from one session into another

    *args*:
    target_session: the session to copy into
    from_session: the session to copy from

    This is a non-trivial operation. The following steps are taken:

    1) For efficiency, all foreign key constraints and indexes are dropped from the target
    2) A copy of the target dictionary is made, which does not have a unique constraint, so that temporary
       duplicates of dictionary entries can be made
    3) All tables are copied from the source to the destination, with the following caveats:
        * The id column of all tables is offset by the existing maximum, to prevent collisions
        * Foreign keys are updated to point to the new ids
        * The dictionary table is copied to the temporary dictionary table, not the permanent one
    4) The temporary dictionary table is de-duplicated, and the result copied back to the permanent dictionary table
    5) Indexes and foreign keys are recreated on the target
    """

    from sqlalchemy.schema import DropTable

    target_connection = target_session.connection()
    from_connection = from_session.connection()

    copy_classes = [Creator, Simulation, TimeStep, SimulationObjectBase, DictionaryItem, SimulationProperty,
                    HaloLink, HaloProperty]

    print("Dropping foreign key constraints...")
    _drop_foreign_keys(target_session)

    print("Dropping indexes...")
    _drop_or_create_indexes(target_connection, mode='drop')

    print("Creating temporary dictionary...")
    # this is necessary because the dictionary table has a unique constraint on the text column
    # which we temporarily need to violate
    temp_dict = _create_temporary_dictionary(target_connection)


    print("Copying tables...")
    try:
        id_offsets = {}
        for target in copy_classes:
            if target == DictionaryItem:
                # special treatment to avoid unique constraint violation - insert into a temporary
                # table and then tidy everything up later
                target_table = temp_dict
            else:
                target_table = None
            id_offsets = _copy_table(from_connection, target_connection, target, id_offsets, target_table)

        _dedup_temp_dictionary_items(target_connection, temp_dict)
        _temporary_to_permanent_dictionary(target_connection, temp_dict)
        target_connection.commit()

    finally:
        target_connection.rollback()

        print("Recreating indexes...")
        _drop_or_create_indexes(target_connection, mode='create')

        print("Recreating foreign keys...")
        _create_foreign_keys(target_session)

        print("Dropping temporary dictionary table...")
        target_connection.execute(DropTable(temp_dict))
def _copy_table(from_connection, target_connection, orm_class, offsets, destination_table=None):
    import tqdm
    from sqlalchemy import func, insert, select

    table = orm_class.__table__

    if destination_table is None:
        destination_table = table

    num_rows = from_connection.execute(select(func.count(table.c.id))).scalar()

    id_offset = target_connection.execute(select(func.max(table.c.id))).scalar()
    if id_offset is None:
        id_offset = 0

    offsets[table.c.id] = id_offset
    cols_select = _get_import_columns_with_required_offsets(table, offsets)


    num_done = 0

    source_result = from_connection.execute(select(*cols_select))

    retries = 0

    with tqdm.tqdm(total=num_rows, desc = f"Copying {orm_class.__name__}", unit="row", smoothing=0.1) as pbar:
        while num_done < num_rows:
            all_rows = source_result.fetchmany(DB_IMPORT_CHUNK_SIZE)
            all_rows = [tuple(r) for r in all_rows]

            try:
                target_connection.execute(insert(destination_table).values(all_rows))

            except sqlalchemy.exc.OperationalError as e:
                if retries>=1:
                    raise # if this line is hit, it may reflect a data limit in the server, e.g. max_allowed_packet in MySQL
                    # Such limits result in the connection being dropped. In PostgreSQL an error is written in the
                    # server log, but in MySQL it does not seem to be. Reducing CHUNK_SIZE may help, or increasing
                    # the limit on the server.

                num_committed = num_done - (num_done % (DB_IMPORT_CHUNK_SIZE * DB_IMPORT_COMMIT_AFTER_CHUNKS))
                pbar.update(num_committed-num_done) # negative correction
                print(f"Note: lost connection to database after {num_done} rows. Resetting to {num_committed}.")
                # reset to point of last commit
                num_done = num_committed
                target_connection.rollback()
                # create a new connection from the target connection's engine
                target_connection = target_connection.engine.connect()
                source_result = from_connection.execute(select(table).offset(num_committed))
                retries+=1
                continue


            num_done += len(all_rows)
            pbar.update(len(all_rows))

            if num_done % (DB_IMPORT_CHUNK_SIZE * DB_IMPORT_COMMIT_AFTER_CHUNKS) == 0:
                target_connection.commit()
                retries = 0



    target_connection.commit()

    return offsets


def _get_import_columns_with_required_offsets(table, offsets):
    cols_select = []
    fk_map = {fk.parent: fk.column for fk in table.foreign_keys}
    for c in table.c:
        if c in fk_map.keys():
            foreign_column = fk_map[c]
        else:
            foreign_column = c

        if foreign_column in offsets.keys() and offsets[foreign_column] != 0:
            cols_select.append(c + offsets[foreign_column])
        else:
            cols_select.append(c)
    return cols_select


def _drop_foreign_keys(session):
    from sqlalchemy import ForeignKeyConstraint, MetaData, Table
    from sqlalchemy.engine import reflection
    from sqlalchemy.schema import AddConstraint, DropConstraint


    engine = session.get_bind()

    inspector = reflection.Inspector.from_engine(engine)
    fake_metadata = MetaData()

    fake_tables = []
    all_fks = []

    for table_name in Base.metadata.tables:
        # if the metadata has multihop tables left, we aren't interested in those (they're not really in the database)
        if 'multihop' not in table_name:
            fks = []
            for fk in inspector.get_foreign_keys(table_name):
                if fk['name']:
                    fks.append(ForeignKeyConstraint((), (), name=fk['name']))
            t = Table(table_name, fake_metadata, *fks)
            fake_tables.append(t)
            all_fks.extend(fks)

    with engine.begin() as conn:
        for fkc in all_fks:
            print(str(DropConstraint(fkc)))
            conn.execute(DropConstraint(fkc))


def _create_foreign_keys(session, verbose=False):
    from sqlalchemy import ForeignKeyConstraint, MetaData, Table
    from sqlalchemy.engine import reflection
    from sqlalchemy.schema import AddConstraint, DropConstraint


    engine = session.get_bind()

    with engine.begin() as conn:
        for table in Base.metadata.tables.values():
            for fk in table.foreign_keys:
                if verbose:
                    print(str(AddConstraint(fk.constraint)), end="")
                try:
                    conn.execute(AddConstraint(fk.constraint))
                except sqlalchemy.exc.OperationalError:
                    if verbose:
                        print("... FAILED")
                else:
                    if verbose:
                        print("... OK")


def _drop_or_create_indexes(connection, mode='drop', verbose=False):
    from sqlalchemy.schema import CreateIndex, DropIndex
    for table in Base.metadata.tables.values():
        for index in table.indexes:

            try:
                if mode=='drop':
                    if verbose:
                        print(str(DropIndex(index)), end="")
                        sys.stdout.flush()
                    index.drop(connection)
                elif mode=='create':
                    if verbose:
                        print(str(CreateIndex(index)), end="")
                        sys.stdout.flush()
                    index.create(connection)
                else:
                    raise ValueError("mode must be 'drop' or 'create'")
            except (sqlalchemy.exc.OperationalError, sqlalchemy.exc.ProgrammingError):
                if verbose:
                    print("... FAILED")
                connection.rollback()
            else:
                if verbose:
                    print("... OK")
                connection.commit()


def _dedup_temp_dictionary_items(connection, dict_table):
    from sqlalchemy import (Column, bindparam, delete, func, select, text,
                            update)

    updater_expressions = _updates_for_all_columns_referencing_dictionary_id()

    results = connection.execute(select(dict_table.c.text, dict_table.c.id)).fetchall()
    text_to_id = {r[0]: [] for r in results}
    for t, id in results:
        text_to_id[t].append(id)

    text_to_id = {text: id for text, id in text_to_id.items() if len(id)>1 }

    to_delete = []
    for text, id in tqdm.tqdm(text_to_id.items(), desc="De-dup dictionary", unit="items"):
        if len(id)>1:
            update_binds = [{'good_id': id[0], 'bad_id': bad_id} for bad_id in id[1:]]
            for sql in updater_expressions:
                connection.execute(sql, update_binds)
            to_delete+=id[1:]

    connection.execute(delete(dict_table).where(dict_table.c.id.in_(to_delete)))

    connection.commit()


def _updates_for_all_columns_referencing_dictionary_id():
    from sqlalchemy import bindparam, update

    # find all foreign keys pointing to dictionary.id:
    updates = []
    for table in Base.metadata.tables.values():
        for fk in table.foreign_keys:
            if fk.column == DictionaryItem.__table__.c.id:
                col = fk.parent
                tab = col.table
                update_sql = update(tab).where(col == bindparam('bad_id')).values(
                    **{col.description: bindparam('good_id')})
                updates.append(update_sql)


    return updates




def _temporary_to_permanent_dictionary(connection, temp_dict_table):


    dict_table = DictionaryItem.__table__

    print("Copying back dictionary...")

    # copy the dictionary table into the temporary table
    connection.execute(dict_table.delete())
    connection.execute(dict_table.insert().from_select([dict_table.c.id, dict_table.c.text],
                                                            temp_dict_table.select()))

    connection.commit()


def _create_temporary_dictionary(connection):
    from sqlalchemy import Column, Integer, String, Table
    from sqlalchemy.schema import CreateTable

    dict_table = DictionaryItem.__table__

    # create a table like the dictionary table, but without the unique constraint
    temp_dict_table = Table("temporary_dictionary", Base.metadata,
                            Column("id", Integer, primary_key=True),
                            Column("text", String(128)))
    connection.execute(CreateTable(temp_dict_table))

    # copy the dictionary table into the temporary table
    connection.execute(temp_dict_table.insert().from_select([dict_table.c.id, dict_table.c.text],
                                                            dict_table.select()))

    return temp_dict_table
