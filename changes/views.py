import os
import json
from flask import request, current_app, Blueprint, render_template
from changes.database import engine
from sqlalchemy import text

views = Blueprint('views', __name__)

@views.route('/')
def index():
    limit = request.args.get('limit', 100)
    offset = request.args.get('offset', 0)
    grouped_records = ''' 
        SELECT 
          COUNT(*) AS "Change Count",
          MAX(case_number) AS "Case Number", 
          MAX(orig_date) AS "Report Date",
          MAX(block) AS "Block",
          MAX(primary_type) AS "Primary Classification",
          MAX(description) AS "Secondary Classification",
          MAX(location_description) AS "Location Description",
          bool_or(arrest) AS "Arrest"
        FROM changed_records
        GROUP BY id
        ORDER BY COUNT(*) DESC
        LIMIT :limit
        OFFSET :offset
    '''
    grouped_records = engine.execute(text(grouped_records), 
                                     limit=limit,
                                     offset=offset)

    return render_template('index.html', 
                           grouped_records=grouped_records)

