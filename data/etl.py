import psycopg2
import gzip

DB_USER = 'eric'
DB_HOST = '127.0.0.1'
DB_NAME = 'changes'
DB_PORT = '5432'
DB_CONN_STR = 'host={0} dbname={1} user={2} port={3}'\
    .format(DB_HOST, DB_NAME, DB_USER, DB_PORT)

DATA_COLS = ''' 
    id BIGINT,
    case_number VARCHAR(10),
    orig_date TIMESTAMP,
    block VARCHAR(50),
    iucr VARCHAR(10),
    primary_type VARCHAR(100),
    description VARCHAR(100),
    location_description VARCHAR(50),
    arrest BOOLEAN,
    domestic BOOLEAN,
    beat VARCHAR(10),
    district VARCHAR(5),
    ward INTEGER,
    community_area VARCHAR(10),
    fbi_code VARCHAR(10),
    x_coordinate INTEGER,
    y_coordinate INTEGER,
    year INTEGER,
    updated_on TIMESTAMP,
    latitude FLOAT8,
    longitude FLOAT8,
    location VARCHAR(50),
'''

COLS = [
    'id',
    'case_number',
    'orig_date',
    'block',
    'iucr',
    'primary_type',
    'description',
    'location_description',
    'arrest',
    'domestic',
    'beat',
    'district',
    'ward',
    'community_area',
    'fbi_code',
    'x_coordinate',
    'y_coordinate',
    'year',
    'updated_on',
    'latitude',
    'longitude',
    'location',
]

def makeDataTable():
    ''' 
    Step One: Make the data table where the data will eventually live
    '''

    create = '''
        CREATE TABLE IF NOT EXISTS dat_chicago_crime(
          row_id SERIAL,
          start_date TIMESTAMP,
          end_date TIMESTAMP DEFAULT NULL,
          current_flag BOOLEAN DEFAULT TRUE,
          dup_ver INTEGER,
          source_filename VARCHAR,
          {0}
          PRIMARY KEY(row_id),
          UNIQUE(id, start_date)
        )
        '''.format(DATA_COLS)
    with psycopg2.connect(DB_CONN_STR) as conn:
        with conn.cursor() as curs:
            curs.execute(create)

def makeSourceTable():
    ''' 
    Step Two: Make the table where we will store the incoming data
    '''
    drop = 'DROP TABLE IF EXISTS src_chicago_crime'
    create = ''' 
        CREATE TABLE IF NOT EXISTS src_chicago_crime(
          {0}
          line_num SERIAL
        )
        '''.format(DATA_COLS)
    with psycopg2.connect(DB_CONN_STR) as conn:
        with conn.cursor() as curs:
            curs.execute(drop)
            curs.execute(create)

def insertSourceData(fp):
    ''' 
    Step Three: Store the incoming data
    '''
    copy_st = ''' 
        COPY src_chicago_crime({0}) 
        FROM STDIN
        WITH (FORMAT CSV, HEADER TRUE, DELIMITER',')
    '''.format(','.join(COLS))
    f = gzip.GzipFile(fileobj=fp)
    with psycopg2.connect(DB_CONN_STR) as conn:
        with conn.cursor() as curs:
            try:
                curs.copy_expert(copy_st, f)
            except psycopg2.extensions.QueryCanceledError as e:
                 raise e

def makeNewDupTables():
    ''' 
    Step Four: Make tables that we'll use to find records with the same id
    and where we will find records that are not already in the dat table
    '''

    drop = 'DROP TABLE IF EXISTS {0}_chicago_crime'
    create = ''' 
        CREATE TABLE IF NOT EXISTS {0}_chicago_crime(
          id BIGINT,
          line_num INT,
          dup_ver INT,
          PRIMARY KEY(dup_ver, id)
        )'''
    create_index = ''' 
        CREATE INDEX {0}_id_ix ON {0}_chicago_crime (id)
    '''
    for table in ['new', 'dup']:
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as curs:
                curs.execute(drop.format(table))
                curs.execute(create.format(table))
                curs.execute(create_index.format(table))

def findDupRows():
    ''' 
    Step Five: Find records that have the same id
    '''
    insert = ''' 
        INSERT INTO dup_chicago_crime
        SELECT 
          id, 
          line_num, 
          RANK() OVER(
            PARTITION BY id ORDER BY line_num DESC
          ) AS dup_ver
        FROM src_chicago_crime
    '''
    with psycopg2.connect(DB_CONN_STR) as conn:
        with conn.cursor() as curs:
            curs.execute(insert)

def findNewRows():
    ''' 
    Step Six: Find records that don't already exist in the dat table
    '''
    insert = ''' 
        INSERT INTO new_chicago_crime
        SELECT 
          id,
          line_num,
          dup_ver
        FROM src_chicago_crime
        JOIN dup_chicago_crime
          USING (line_num, id)
        LEFT OUTER JOIN dat_chicago_crime
          USING(dup_ver,id)
        WHERE row_id IS NULL
    '''
    with psycopg2.connect(DB_CONN_STR) as conn:
        with conn.cursor() as curs:
            curs.execute(insert)

def insertNewRows(filename):
    ''' 
    Step Seven: Insert new rows into the dat table
    '''
    insert = ''' 
        INSERT INTO dat_chicago_crime (
          start_date, 
          dup_ver,
          source_filename,
          {0}
        )
        SELECT 
          NOW() AS start_date,
          n.dup_ver,
          %(filename)s AS source_filename,
          {0}
        FROM src_chicago_crime AS s
        JOIN new_chicago_crime AS n
          USING(id)
    '''.format(','.join(COLS))
    with psycopg2.connect(DB_CONN_STR) as conn:
        with conn.cursor() as curs:
            curs.execute(insert, {'filename': filename})

def findChangedRows():
    ''' 
    Step Eight: Compare incoming data to data already in dat table to find
    rows that have changed.
    '''
    drop = 'DROP TABLE IF EXISTS chg_chicago_crime'

    create = '''
        CREATE TABLE IF NOT EXISTS chg_chicago_crime(
          id INTEGER,
          PRIMARY KEY (id)
        )'''

    with psycopg2.connect(DB_CONN_STR) as conn:
        with conn.cursor() as curs:
            curs.execute(drop)
            curs.execute(create)

    insert = ''' 
        INSERT INTO chg_chicago_crime
          SELECT d.id
          FROM src_chicago_crime AS s
          JOIN dat_chicago_crime AS d
            USING (id)
          WHERE d.current_flag = TRUE
            AND (((s.id IS NOT NULL OR d.id IS NOT NULL) AND s.id <> d.id)
               OR ((s.case_number IS NOT NULL OR d.case_number IS NOT NULL) AND s.case_number <> d.case_number)
               OR ((s.orig_date IS NOT NULL OR d.orig_date IS NOT NULL) AND s.orig_date <> d.orig_date)
               OR ((s.block IS NOT NULL OR d.block IS NOT NULL) AND s.block <> d.block)
               OR ((s.iucr IS NOT NULL OR d.iucr IS NOT NULL) AND s.iucr <> d.iucr)
               OR ((s.primary_type IS NOT NULL OR d.primary_type IS NOT NULL) AND s.primary_type <> d.primary_type)
               OR ((s.description IS NOT NULL OR d.description IS NOT NULL) AND s.description <> d.description)
               OR ((s.location_description IS NOT NULL OR d.location_description IS NOT NULL) AND s.location_description <> d.location_description)
               OR ((s.arrest IS NOT NULL OR d.arrest IS NOT NULL) AND s.arrest <> d.arrest)
               OR ((s.domestic IS NOT NULL OR d.domestic IS NOT NULL) AND s.domestic <> d.domestic)
               OR ((s.beat IS NOT NULL OR d.beat IS NOT NULL) AND s.beat <> d.beat)
               OR ((s.district IS NOT NULL OR d.district IS NOT NULL) AND s.district <> d.district)
               OR ((s.ward IS NOT NULL OR d.ward IS NOT NULL) AND s.ward <> d.ward)
               OR ((s.community_area IS NOT NULL OR d.community_area IS NOT NULL) AND s.community_area <> d.community_area)
               OR ((s.fbi_code IS NOT NULL OR d.fbi_code IS NOT NULL) AND s.fbi_code <> d.fbi_code)
               OR ((s.year IS NOT NULL OR d.year IS NOT NULL) AND s.year <> d.year)
               OR ((s.updated_on IS NOT NULL OR d.updated_on IS NOT NULL) AND s.updated_on <> d.updated_on)
            )
    '''
    
    with psycopg2.connect(DB_CONN_STR) as conn:
        with conn.cursor() as curs:
            curs.execute(insert)

def flagChanges():
    # Update existing records to no longer be current
    update = ''' 
        UPDATE dat_chicago_crime AS d SET
          end_date = NOW(),
          current_flag = FALSE
        FROM chg_chicago_crime AS c
        WHERE d.id = c.id
          AND d.current_flag = TRUE
    '''
    
    # Insert new version 
    insert = ''' 
        INSERT INTO dat_chicago_crime (
          start_date, 
          {0}
        )
        SELECT 
          NOW() AS start_date,
          {0}
        FROM src_chicago_crime AS s
        JOIN chg_chicago_crime AS c
          USING(id)
    '''.format(','.join(COLS))
    
    with psycopg2.connect(DB_CONN_STR) as conn:
        with conn.cursor() as curs:
            curs.execute(update)
    
    with psycopg2.connect(DB_CONN_STR) as conn:
        with conn.cursor() as curs:
            curs.execute(insert)

def updateView():
    create = ''' 
        CREATE MATERIALIZED VIEW changed_records AS (
            SELECT d.* FROM dat_chicago_crime AS d
            JOIN (
                SELECT id FROM dat_chicago_crime
                GROUP BY id
                HAVING(COUNT(*) > 1)
            ) AS s
                ON d.id = s.id
        )
    '''
    with psycopg2.connect(DB_CONN_STR) as conn:
        with conn.cursor() as curs:
            try:
                curs.execute('REFRESH MATERIALIZED VIEW changed_records')
                conn.commit()
            except psycopg2.ProgrammingError:
                conn.rollback()
                curs.execute(create)
                conn.commit()

def makeMetaTable():
    create = ''' 
        CREATE TABLE IF NOT EXISTS etl_tracker(
            filename VARCHAR,
            date_added TIMESTAMP DEFAULT NOW(),
            etl_status VARCHAR
        )
    '''
    with psycopg2.connect(DB_CONN_STR) as conn:
        with conn.cursor() as curs:
            curs.execute(create)

def updateMetaTable(filename, status):
    from datetime import date
    
    year, month, day = filename.split('T')[0].split('/')[1].split('-')
    file_date = date(int(year), int(month), int(day))
    
    insert = ''' 
        INSERT INTO etl_tracker (filename, etl_status, file_date) 
        VALUES (%(filename)s, %(status)s, %(file_date)s)
    '''
    with psycopg2.connect(DB_CONN_STR) as conn:
        with conn.cursor() as curs:
            curs.execute(insert, 
                         {'filename':filename, 
                          'status': status,
                          'file_date': file_date})

if __name__ == "__main__":
    import os
    import csv
    import codecs
    import lxml.etree
    import requests
    from operator import itemgetter
    import time

    makeDataTable()
    makeMetaTable()

    bucket_url = 'http://s3.amazonaws.com/urbanccd-plenario'

    bucket_listing = requests.get(bucket_url, params={'prefix': 'crimes_2001_to_present'})
    
    tag_prefix = 'http://s3.amazonaws.com/doc/2006-03-01/'
    
    downloads = []

    tree = lxml.etree.fromstring(bucket_listing.content)
    for chunk in tree.findall('{%s}Contents' % tag_prefix):
        for key in chunk.findall('{%s}Key' % tag_prefix):
            if key.text.endswith('gz'):
                downloads.append((bucket_url, key.text))
    
    downloads = sorted(downloads, key=itemgetter(1))

    for bucket_url, filename in downloads:
        print('working on %s' % filename)
        file_date = filename.split('T')[0].split('/')[1]
        conn = psycopg2.connect(DB_CONN_STR)
        curs = conn.cursor()
        curs.execute('SELECT * FROM etl_tracker WHERE file_date = %(file_date)s', 
                     {'file_date': file_date})
        record = curs.fetchone()
        conn.close()

        if record:
            print('found a record for date %s' % file_date)
        else:
            print('downloading %s' % filename)
            if not os.path.exists(filename):
                start = time.time()
                with open(filename, 'wb') as f:
                    r = requests.get('%s/%s' % (bucket_url, filename), stream=True)
                    for chunk in r.iter_content(chunk_size=1024):
                        if chunk:
                            f.write(chunk)
                            f.flush()
                print('download took %s \n' % (time.time() - start))
        
            contents = open(filename, 'rb')
            
            print('making source table')
            
            makeSourceTable()
            
            try:
                print('inserting source data')
                start = time.time()
                insertSourceData(contents)
                print('insert took %s \n' % (time.time() - start))
                contents.close()
                proceed = True
            except Exception as e:
                contents.close()
                print(e)
                updateMetaTable(filename, 'failed - bad data')
                proceed = False
            
            if proceed:
                print('finding duplicates')
                start = time.time()
                makeNewDupTables()
                findDupRows()
                print('dedupe took %s \n' % (time.time() - start))
         
                print('inserting new data')
                start = time.time()
                findNewRows()
                insertNewRows(filename)
                print('new rows took %s \n' % (time.time() - start))

                print('finding changes')
                start = time.time()
                findChangedRows()
                print('changes took %s \n' % (time.time() - start))
         
                print('flagging changes')
                flagChanges()
                updateView()

                updateMetaTable(filename, 'success')
                os.remove(filename)
