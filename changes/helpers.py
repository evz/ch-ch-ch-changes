from changes.database import db_session, engine
from sqlalchemy import Table, MetaData, func
from collections import OrderedDict
import inspect

def groupedChanges(order_by='id', 
                   sort_order='asc',
                   limit=500,
                   offset=0):

    view = Table('changed_records', MetaData(),
                                 autoload=True, autoload_with=engine)
    
    order_by_clause = getattr(getattr(view.c, order_by), sort_order)()
    
    skip_columns = [
        'id', 
        'start_date', 
        'end_date', 
        'current_flag', 
        'dup_ver', 
        'source_filename'
    ]
    select_columns = [view.c.id]
    for column in view.columns:
        if column.name not in skip_columns:
            select_columns.append(func.array_agg(column)\
                                   .label(column.name))

    changed_records = db_session.query(*select_columns)\
                                .group_by(view.c.id)\
                                .order_by(order_by_clause)\
                                .limit(limit)\
                                .offset(offset)
    
    count = db_session.query(view).count()
    
    query = {
        'order_by': order_by,
        'sort_order': sort_order,
        'limit': limit,
        'offset': offset
    }

    return changed_records, query, count
