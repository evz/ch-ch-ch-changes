import os
import json
import itertools
from collections import OrderedDict
import math

from flask import request, current_app, Blueprint, render_template
from sqlalchemy import text

from app.views import views
from app.extensions import db


@views.route('/')
def index():
    query = '''
      SELECT 
        MIN(file_date) AS start_date,
        MAX(file_date) AS end_date
      FROM etl_tracker
    '''	
    result = db.session.execute(text(query)).first()
    start_date = result.start_date.strftime('%B %-d, %Y')
    end_date = result.end_date.strftime('%B %-d, %Y')
    return render_template('index.html',
                           start_date=start_date,
                           end_date=end_date)


@views.route('/change-list/')
def change_list():
    limit = request.args.get('limit', 100)
    offset = request.args.get('offset', 0)
    records = '''
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
        FROM changed_records
        ORDER by ID
        LIMIT :limit
    '''
    display_fields = [
        "Record ID",
        "Case Number",
        "Report Date",
        "Block",
        "Primary Classification",
        "Secondary Classification",
        "Location Description",
        "Arrest",
        "Last Update",
    ]
    
    query_params = {
        'limit': limit
    }

    if request.args.get('page'):
        page = int(request.args['page'])
        offset = (page - 1) * 100
        records = f'{records} OFFSET :offset'
        query_params['offset'] = offset
    
    records = db.session.execute(text(records), query_params)

    grouped_records = []
    fields = set()
    
    results = [dict(zip(records.keys(), r)) for r in records]

    for record_id, group in itertools.groupby(results, key=lambda x: x['Record ID']):

        group = list(group)
        grouped_by_field = {}
        for record in group:
            for field in record.keys():
                try:
                    grouped_by_field[field].append(record[field])
                except KeyError:
                    grouped_by_field[field] = [record[field]]

        diff_fields = []
        output_record = OrderedDict()

        for field, values in grouped_by_field.items():
            if len(set(values)) > 1:
                diff_fields.append(field)

            if field in display_fields:
                fields.add(field)
                output_record[field] = max(t for t in grouped_by_field[field] if t is not None)

        output_record['diff_fields'] = diff_fields
        output_record['Change Count'] = len(group)
        grouped_records.append(output_record)


    record_count = 'SELECT COUNT(*) FROM changed_records'
    record_count = db.session.execute(text(record_count)).first()[0]
    page_count = math.ceil((record_count / 100))

    return render_template('change_list.html',
                           grouped_records=grouped_records,
                           page_count=page_count,
                           fields=fields)


@views.route('/detail/<record_id>/')
def detail(record_id):
    record_set = '''
        SELECT
          id AS "Record ID",
          case_number AS "Case Number",
          orig_date AS "Report Date",
          block AS "Block",
          c.iucr AS "IUCR Code",
          CASE WHEN index_code = 'I'
          THEN 'Yes'
          ELSE 'No'
          END AS "Index crime?",
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
        FROM changed_records AS c
        JOIN iucr AS i
          ON c.iucr = i.iucr
          OR c.iucr = lpad(i.iucr, 4, '0')
        WHERE id = :record_id
        ORDER BY updated_on
    '''
    record_set = db.session.execute(text(record_set), 
                                    dict(record_id=record_id))
    
    results = [dict(zip(record_set.keys(), r)) for r in record_set]
    
    grouped_by_field = OrderedDict()

    for record in results:
        for field in record.keys():
            try:
                grouped_by_field[field].append(record.get(field))
            except KeyError:
                grouped_by_field[field] = [record.get(field)]

    diff_fields = []

    for field, values in grouped_by_field.items():
        if len(set(values)) > 1:
            diff_fields.append(field)
    return render_template('detail.html',
                           grouped_by_field=grouped_by_field,
                           diff_fields=diff_fields)


@views.route('/deleted/')
def deleted():
    limit = request.args.get('limit', 100)
    offset = request.args.get('offset', 0)
    
    query = '''
      SELECT
        id,
        json_agg(row_to_json(s.*)) AS data
      FROM (
        SELECT 
          id,
          c.iucr,
          c.primary_type || ' - ' || c.description AS description,
          c.fbi_code,
          index_code,
          orig_date,
          deleted_on
        FROM changed_records AS c
        JOIN iucr AS i
          ON c.iucr = i.iucr
          OR c.iucr = lpad(i.iucr, 4, '0')
        WHERE c.deleted_flag = TRUE
        ORDER BY id
      ) AS s
      GROUP BY id
      LIMIT :limit
    '''

    if request.args.get('page'):
        page = int(request.args['page'])
        offset = (page - 1) * 100
        query = '{0} OFFSET {1}'\
                .format(query, offset)

    records = db.session.execute(text(query),
                                 dict(limit=limit,
                                      offset=offset))

    page_count = math.ceil((records.rowcount / 100))
    
    return render_template('deleted.html',
                           records=records,
                           page_count=page_count)


@views.route('/index-code-changes/')
def index_code_change():

    limit = request.args.get('limit', 100)
    offset = request.args.get('offset', 0)

    subquery = '''
      SELECT
        id,
        c.iucr,
        c.primary_type || ' - ' || c.description AS description,
        c.fbi_code,
        index_code,
        updated_on
      FROM changed_records AS c
      JOIN iucr AS i
        ON c.iucr = i.iucr
        OR c.iucr = lpad(i.iucr, 4, '0')
      JOIN (
        SELECT
          id,
          array_agg(index_code)
        FROM changed_records AS c
        JOIN iucr AS i
          ON c.iucr = i.iucr
          OR c.iucr = lpad(i.iucr, 4, '0')
        GROUP BY id
        HAVING(
          array_length(array_agg(DISTINCT index_code), 1) > 1
            AND array_agg(index_code) && ARRAY['I', 'N']::VARCHAR[]
        )
      ) AS s
        USING(id)
      ORDER BY id, updated_on
    '''

    query = '''
      SELECT
        id,
        json_agg(row_to_json(s.*)) AS data
      FROM ({}) AS s
      GROUP BY id
      LIMIT :limit
    '''.format(subquery)

    if request.args.get('page'):
        page = int(request.args['page'])
        offset = (page - 1) * 100
        query = '{0} OFFSET {1}'\
                .format(query, offset)

    records = db.session.execute(text(query),
                                 dict(limit=limit,
                                      offset=offset))

    page_count = math.ceil((records.rowcount / 100))
    
    return render_template('index_code_change.html',
                           records=records,
                           page_count=page_count)
