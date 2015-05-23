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
    '''
    
    if request.args.get('page'):
        page = int(request.args['page'])
        offset = (page - 1) * 100
        grouped_records = '{0} OFFSET {1}'\
                .format(grouped_records, offset)
    
    grouped_records = engine.execute(text(grouped_records), 
                                     limit=limit,
                                     offset=offset)
    
    record_count = ''' 
        SELECT COUNT(DISTINCT id) AS record_count
        FROM changed_records
    '''

    record_count = engine.execute(record_count).first().record_count
    
    page_count = int(round(record_count, -2) / 100)
    
    return render_template('index.html', 
                           grouped_records=grouped_records,
                           page_count=page_count)

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
        ORDER BY updated_on
    '''
    record_set = engine.execute(text(record_set), record_id=record_id)
    
    grouped_by_field = OrderedDict()

    for record in record_set:
        for field in record.keys():
            try:
                grouped_by_field[field].append(getattr(record, field))
            except KeyError:
                grouped_by_field[field] = [getattr(record, field)]
    
    diff_fields = []
    
    for field, values in grouped_by_field.items():
        if len(set(values)) > 1:
            diff_fields.append(field)
    
    return render_template('detail.html', 
                           grouped_by_field=grouped_by_field,
                           diff_fields=diff_fields)

@views.route('/compare/')
def compare():
    records = ''' 
        SELECT
          c.id AS "Record ID",
          c.case_number AS "Case Number",
          c.orig_date AS "Report Date",
          c.block AS "Block",
          c.iucr AS "IUCR Code",
          c.primary_type "Primary Classification",
          c.description AS "Secondary Classification",
          c.location_description AS "Location Description",
          c.arrest AS "Arrest",
          c.domestic AS "Domestic",
          c.beat AS "Beat",
          c.district AS "District",
          c.ward AS "Ward",
          c.community_area AS "Community Area",
          c.fbi_code AS "FBI Code",
          c.x_coordinate AS "X Coordinate",
          c.y_coordinate AS "Y Coordinate",
          c.year AS "Year",
          c.updated_on AS "Last Update",
          c.latitude AS "Latitude",
          c.longitude AS "Longitude"
        FROM changed_records AS c
        JOIN (
          SELECT id
          FROM changed_records
          WHERE updated_on BETWEEN :start_date AND :end_date
        ) AS s
          ON c.id = s.id
        ORDER BY updated_on DESC
    '''
    return render_template('compare.html')
