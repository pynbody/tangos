import sqlalchemy.engine

from ..log import logger


def explain_query(query, engine_or_connection=None):
    """Get the underlying SQL engine to explain how it will execute a given query. For debugging purposes.

    If engine_or_connection is None, use the query's existing engine"""

    if engine_or_connection is None:
        engine_or_connection = query.session.connection()

    if isinstance(engine_or_connection, sqlalchemy.engine.Engine):
        with engine_or_connection.connect() as connection:
            _explain_query_using_connection(query, connection)
    else:
        _explain_query_using_connection(query, engine_or_connection)

def _explain_query_using_connection(query, connection):
    compiled_q = query.statement.compile(compile_kwargs={"literal_binds": True})
    # use self._connection.execute("explain "+str(compiled_q)) to get explanation, then print it:

    from sqlalchemy.sql import text
    dialect = connection.dialect.dialect_description.split("+")[0].lower()
    if dialect == 'sqlite':
        explain_command = "explain query plan "
    else:
        explain_command = "explain analyze "

    explain_result = connection.execute(text(explain_command + str(compiled_q)))

    from prettytable import from_db_cursor


    logger.info("Analysis of query:")
    logger.info(compiled_q)

    pt = from_db_cursor(explain_result.cursor)
    pt.align = "l"


    logger.info(pt)
