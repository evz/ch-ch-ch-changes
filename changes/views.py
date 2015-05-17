import os
import json
from flask import request, current_app, Blueprint, render_template
from changes.database import engine
from sqlalchemy import text
from collections import OrderedDict

views = Blueprint('views', __name__)

@views.route('/')
def index():
    limit = request.args.get('limit', 100)
    offset = request.args.get('offset', 0)
    grouped_records = ''' 
        SELECT 
          COUNT(*) AS "Change Count",
          id AS "Record ID",
          MAX(case_number) AS "Case Number", 
          MAX(orig_date) AS "Report Date",
          MAX(block) AS "Block",
          MAX(primary_type) AS "Primary Classification",
          MAX(description) AS "Secondary Classification",
          MAX(location_description) AS "Location Description",
          bool_or(arrest) AS "Arrest",
          MAX(updated_on) AS "Last Update"
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

@views.route('/detail/<record_id>/')
def detail(record_id):
    record_set = ''' 
        SELECT
          id AS "Record ID",
          case_number AS "Case Number",
          orig_date AS "Report Date",
          block AS "Block",
          iucr AS "IUCR Code",
          primary_type "Primary Classification",
          description AS "Secondary Classification",
          location_description AS "Location Description",
          arrest AS "Arrest",
          domestic AS "Domestic",
          beat AS "Beat",
          district AS "District",
          ward AS "Ward",
          community_area AS "Community Area",
          fbi_code AS "FBI Code",
          x_coordinate AS "X Coordinate",
          y_coordinate AS "Y Coordinate",
          year AS "Year",
          updated_on AS "Last Update",
          latitude AS "Latitude",
          longitude AS "Longitude"
        FROM changed_records WHERE id = :record_id
        ORDER BY updated_on DESC
    '''
    record_set = engine.execute(text(record_set), record_id=record_id)
    
    grouped_by_field = OrderedDict()

    for record in record_set:
        for field in record.keys():
            try:
                grouped_by_field[field].append(getattr(record, field))
            except KeyError:
                grouped_by_field[field] = [getattr(record, field)]

    return render_template('detail.html', grouped_by_fields=grouped_by_field)
