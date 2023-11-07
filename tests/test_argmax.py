import tangos
from tangos import testing
from tangos.util import sql_argmax


def test_argmax():
    testing.init_blank_db_for_testing()

    engine = tangos.core.get_default_engine()

    # define a sqlalchemy table (non-ORM), with columns "id", "value" and "category"
    from sqlalchemy import Column, Integer, MetaData, Table, insert
    metadata = MetaData()
    table = Table('test_table', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('value', Integer),
                    Column('category', Integer))
    metadata.create_all(engine)

    # populate the table with some data
    with engine.connect() as c:
        c.execute(insert(table),
                       [
                        {'value': 1, 'category': 1},
                        {'value': 2, 'category': 1},
                        {'value': 3, 'category': 1},
                        {'value': 4, 'category': 2},
                        {'value': 5, 'category': 2}
                       ])

        sql_argmax.delete_non_maximal_rows(c, table, table.c.value, [table.c.category])
        query = table.select()

        assert c.execute(query).all() == [(3,3,1),(5,5,2)]
