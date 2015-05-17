from changes.database import db_session, engine
from sqlalchemy import Table, MetaData
from collections import OrderedDict
import inspect

def paginateView(order_by='id', 
                 sort_order='asc',
                 limit=500,
                 offset=0):

    view = Table('changed_records', MetaData(),
                                 autoload=True, autoload_with=engine)
    
    order_by_clause = getattr(getattr(view.c, order_by), sort_order)()
    
    changed_records = db_session.query(view)\
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
