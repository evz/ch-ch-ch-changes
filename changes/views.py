import os
import json
import itertools
from collections import OrderedDict

from flask import request, current_app, Blueprint, render_template
from sqlalchemy import text

from changes.database import engine

views = Blueprint('views', __name__)

@views.route('/')
def index():
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
        ORDER BY id
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

    if request.args.get('page'):
        page = int(request.args['page'])
        offset = (page - 1) * 100
        records = '{0} OFFSET {1}'\
                .format(records, offset)

    records = engine.execute(text(records),
                             limit=limit,
                             offset=offset)

    grouped_records = []
    fields = set()
    for record_id, group in itertools.groupby(records, key=lambda x: x['Record ID']):

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

    record_count = '''
        SELECT COUNT(DISTINCT id) AS record_count
        FROM changed_records
    '''

    record_count = engine.execute(record_count).first().record_count

    page_count = int(round(record_count, -2) / 100)

    return render_template('index.html',
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
        WHERE id = :record_id
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


@views.route('/deleted/')
def deleted():
    return render_template('index.html',
                           grouped_records=grouped_records,
                           page_count=page_count,
                           fields=fields)


@views.route('/downgraded/')
def downgraded():

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
        USING(iucr)
      JOIN (
        SELECT
          id,
          array_agg(index_code)
        FROM changed_records
        JOIN iucr
          USING(iucr)
        GROUP BY id
        HAVING(
          array_length(array_agg(DISTINCT index_code), 1) > 1
            AND array_agg(index_code) && ARRAY['I', 'N']::VARCHAR[]
        )
      ) AS s
        USING(id)
      ORDER BY id
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

    records = engine.execute(text(query),
                             limit=limit,
                             offset=offset)

    page_count = '''
      SELECT COUNT(DISTINCT id) FROM ({}) AS s
    '''.format(subquery)

    page_count = engine.execute(text(page_count)).first().count

    return render_template('downgraded.html',
                           records=records,
                           page_count=page_count)
