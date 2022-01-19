from sqlalchemy import func, and_

def sql_argmax(query, maximise_column, group_bys):
    """Returns a modified query that emulates an argmax function for SQL

    :param query: The SQLAlchemy query
    :param maximise_column: The table column to maximise
    :param group_bys: The table columns to group by
    """

    # we're going to have a subquery that looks very much like the main query,
    # but which serves to pick out the argmax
    argmax_subquery = query

    argmax_subquery = argmax_subquery.add_columns(func.max(maximise_column).label("maximise_column_result")). \
        group_by(*group_bys).subquery()

    join_conditions = [group_by == getattr(argmax_subquery.c, group_by.name) for group_by in group_bys]
    # the main query will join to the subquery to pick out all details of the selected row
    query = query.join(argmax_subquery,
                       and_( maximise_column == argmax_subquery.c.maximise_column_result,
                            *join_conditions))

    # there may be rare occasions where two links have exactly the same weight, in which case the above query
    # currently generates more than one row. Avoid by grouping. Note in this case SQL will return
    # basically a random choice of which row to return, so this is not a perfect solution - TODO.
    query = query.group_by(*group_bys)

    return query