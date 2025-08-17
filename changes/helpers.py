from sqlalchemy import Table, func

from app.extensions import db


def groupedChanges(order_by="id", sort_order="asc", limit=500, offset=0):

    view = Table("changed_records", db.metadata, autoload_with=db.engine)

    order_by_clause = getattr(getattr(view.c, order_by), sort_order)()

    skip_columns = [
        "id",
        "start_date",
        "end_date",
        "current_flag",
        "dup_ver",
        "source_filename",
    ]
    select_columns = [view.c.id]
    for column in view.columns:
        if column.name not in skip_columns:
            select_columns.append(func.array_agg(column).label(column.name))

    changed_records = db.session.execute(
        db.select(*select_columns)
        .group_by(view.c.id)
        .order_by(order_by_clause)
        .limit(limit)
        .offset(offset)
    )

    count = db.session.execute(db.select(view)).rowcount

    query = {
        "order_by": order_by,
        "sort_order": sort_order,
        "limit": limit,
        "offset": offset,
    }

    return changed_records, query, count
