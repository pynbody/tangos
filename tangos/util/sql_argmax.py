from sqlalchemy import and_, func, select


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

    argmax_subquery = argmax_subquery.with_entities(func.max(maximise_column).label("maximise_column_result"),
                                                    *group_bys). \
        group_by(*group_bys).subquery()
    # NB tried making the above a materialised CTE to improve performance, but it doesn't help (presumably the SQLite
    # optimizers already spot the subquery is used twice?)

    group_bys_in_argmax_subquery = [argmax_subquery.corresponding_column(group_by) for group_by in group_bys]

    join_conditions = [a == b for a, b in zip(group_bys, group_bys_in_argmax_subquery)]

    # Ideally now the main query will join to the subquery to pick out all details of the selected row, like:
    #
    # query = query.join(argmax_subquery,
    #                    and_(maximise_column == argmax_subquery.c.maximise_column_result,
    #                         *join_conditions))
    #
    # However this ignores the edge case where two rows in a group both have the same value in maximise_column.
    #
    # We need to de-dup these groups, and do it by picking out the first of any duplicates, in order of the
    # primary key

    assert len(group_bys[0].table.primary_key.columns) == 1 # unable to de-dup if there isn't a single PK column

    primary_key = group_bys[0].table.primary_key.columns[0]

    deduped_argmax_subquery \
        = query.with_entities(func.max(primary_key).label('id')).join(argmax_subquery,
                                                    and_( maximise_column == argmax_subquery.c.maximise_column_result,
                                                            *join_conditions) ).\
                                                    group_by(*group_bys_in_argmax_subquery).subquery()


    # the main query will join to the subquery to pick out all details of the selected row
    query = query.join(deduped_argmax_subquery, deduped_argmax_subquery.c.id == primary_key)

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

    delete_statement = table.delete().where(table.primary_key.columns[0].in_(ids_to_eliminate_select))
    deleted_count = connection.execute(delete_statement).rowcount

    return deleted_count
