from sqlalchemy import func, and_, select

def argmax(query, maximise_column, group_bys):
    """Returns a modified query that emulates an argmax function for SQL

    :param query: The SQLAlchemy query
    :param maximise_column: The table column to maximise
    :param group_bys: The table columns to group by
    :returns: modified SQLAlchemy query that returns the rows which satisfy argmax(maximise_column)
    """

    # we're going to have a subquery that looks very much like the main query,
    # but which serves to pick out the argmax
    argmax_subquery = query

    argmax_subquery = argmax_subquery.add_columns(func.max(maximise_column).label("maximise_column_result")). \
        group_by(*group_bys).subquery()
    # NB tried making the above a materialised CTE to improve performance, but it doesn't help (presumably the SQLite
    # optimizers already spot the subquery is used twice?)

    join_conditions = [group_by == argmax_subquery.corresponding_column(group_by) for group_by in group_bys]
    # the main query will join to the subquery to pick out all details of the selected row
    query = query.join(argmax_subquery,
                       and_( maximise_column == argmax_subquery.c.maximise_column_result,
                            *join_conditions))

    # there may be rare occasions where two links have exactly the same weight, in which case the above query
    # currently generates more than one row. Avoid by grouping. Note in this case SQL will return
    # basically a random choice of which row to return, so this is not a perfect solution - long term TODO.
    # This also ends up necessitating using MySQL without ONLY_FULL_GROUP_BY mode enabled
    query = query.group_by(*group_bys)

    return query

def delete_non_maximal_rows(connection, table, maximise_column, group_bys):
    """Deletes columns from a table that are *not* the argmax.

    This is provided as a performance workaround because of the high cost of the argmax query

    :param connection: connection on which to execute the delete
    :param table: The SQLAlchemy table
    :param maximise_column: The table column to maximise
    :param group_bys: The table columns to group by
    :returns: number of deleted rows
    """
    max_weight = func.max(maximise_column).label("max_weight")

    source_and_max_weight = select(max_weight, *group_bys).\
        group_by(*group_bys).subquery(name="max_from_source_id")

    group_bys_subquery = [source_and_max_weight.corresponding_column(c) for c in group_bys]
    ids_to_eliminate = select(table.primary_key.columns[0]).\
        join(source_and_max_weight,and_(*[a==b for a,b in zip(group_bys, group_bys_subquery)])).\
        filter(maximise_column < source_and_max_weight.c.max_weight)

    # with sqlite it's OK to just do in_(ids_to_eliminate) but MySQL needs a subquery construction, otherwise
    # it complains it can't do the delete
    ids_to_eliminate_cte = ids_to_eliminate.subquery()
    ids_to_eliminate_select = select(ids_to_eliminate_cte.c.id)

    delete_statement = table.delete(table.primary_key.columns[0].in_(ids_to_eliminate_select))
    deleted_count = connection.execute(delete_statement).rowcount

    return deleted_count