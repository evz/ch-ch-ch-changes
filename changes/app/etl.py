import codecs
import csv
import gzip
import logging
import os
import time
from datetime import date, datetime, timedelta
from operator import itemgetter

import psycopg2
import requests
from flask import current_app
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from app.extensions import db

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

DATA_COLS = """
    id BIGINT,
    case_number VARCHAR(10),
    orig_date TIMESTAMP,
    block VARCHAR(50),
    iucr VARCHAR(10),
    primary_type VARCHAR(100),
    description VARCHAR(100),
    location_description VARCHAR(100),
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
"""

COLS = [
    "id",
    "case_number",
    "orig_date",
    "block",
    "iucr",
    "primary_type",
    "description",
    "location_description",
    "arrest",
    "domestic",
    "beat",
    "district",
    "ward",
    "community_area",
    "fbi_code",
    "x_coordinate",
    "y_coordinate",
    "year",
    "updated_on",
    "latitude",
    "longitude",
    "location",
]


class ETL(object):
    def __init__(self, storage_dir, file_date=None):
        self.storage_dir = os.path.abspath(storage_dir)
        self.file_date = file_date

        if not self.file_date:
            self.file_date = datetime.now()

        self.table_setup()

    def table_setup(self):
        self.update_iucr_table()
        self.make_data_table()
        self.make_meta_table()

    def run(self):
        logger.info(
            f"Starting ETL process for date: {self.file_date.strftime('%Y-%m-%d')}"
        )

        try:
            filename = self.download_file("chicago-crime", "ijzp-q8t2")
            logger.info(f"Downloaded file: {filename}")
        except requests.RequestException as e:
            logger.error(f"Network error downloading data file: {e}")
            raise
        except OSError as e:
            logger.error(f"File system error during download: {e}")
            raise

        try:
            contents = open(os.path.join(self.storage_dir, filename), "rb")
        except FileNotFoundError:
            logger.error(f"Downloaded file not found: {filename}")
            raise
        except PermissionError:
            logger.error(f"Permission denied accessing file: {filename}")
            raise

        logger.info("Creating source table")
        self.make_source_table()

        try:
            logger.info("Loading source data")
            start = time.time()
            self.insert_source_data(contents)
            duration = time.time() - start
            logger.info(f"Data insert completed in {duration:.2f} seconds")
            contents.close()
            proceed = True
        except psycopg2.DataError as e:
            contents.close()
            logger.error(f"Data format error during insert: {e}")
            self.update_meta_table(filename, "failed - data format error")
            proceed = False
        except psycopg2.OperationalError as e:
            contents.close()
            logger.error(f"Database connection error: {e}")
            self.update_meta_table(filename, "failed - database error")
            proceed = False
        except UnicodeDecodeError as e:
            contents.close()
            logger.error(f"File encoding error: {e}")
            self.update_meta_table(filename, "failed - encoding error")
            proceed = False

        if proceed:
            logger.info("Starting deduplication process")
            start = time.time()
            self.make_new_dup_tables()
            self.find_dup_rows()
            logger.info(f"Deduplication completed in {time.time() - start:.2f} seconds")

            logger.info("Processing new records")
            start = time.time()
            self.find_new_rows()
            self.insert_new_rows(filename)
            logger.info(
                f"New records processing completed in {time.time() - start:.2f} seconds"
            )

            logger.info("Detecting changes")
            start = time.time()
            self.find_changed_rows()
            logger.info(
                f"Change detection completed in {time.time() - start:.2f} seconds"
            )

            logger.info("Updating change flags")
            self.flag_changes()

            logger.info("Updating deletion flags")
            self.flag_deletions()

            logger.info("Refreshing materialized views")
            self.update_view()

            self.update_meta_table(filename, "success")
            logger.info("ETL process completed successfully")

    def download_file(self, download_type, fourbyfour):
        filedate = self.file_date.strftime("%Y-%m-%d.csv")
        filename = f"{download_type}-{filedate}"
        filepath = os.path.join(self.storage_dir, filename)

        if not os.path.exists(filepath):
            logger.info(f"Downloading {download_type} data from Chicago Data Portal")
            start = time.time()

            params = {
                "fourfour": fourbyfour,
                "accessType": "DOWNLOAD",
            }
            url = f"https://data.cityofchicago.org/api/views/{fourbyfour}/rows.csv"

            try:
                r = requests.get(url, params=params, stream=True, timeout=30)
                r.raise_for_status()

                with open(filepath, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024):
                        if chunk:
                            f.write(chunk)
                            f.flush()

                download_duration = time.time() - start
                logger.info(
                    f"{download_type} download completed in {download_duration:.2f} seconds"
                )

            except requests.Timeout:
                logger.error(f"Download timeout for {download_type}")
                raise
            except requests.HTTPError as e:
                logger.error(f"HTTP error downloading {download_type}: {e}")
                raise
            except requests.ConnectionError as e:
                logger.error(f"Connection error downloading {download_type}: {e}")
                raise
        else:
            logger.info(f"Using existing file: {filename}")

        return filename

    def update_iucr_table(self):
        """
        Step Zero: Make / Update IUCR table
        """
        filename = self.download_file("iucr", "qimd-vs49")

        create_final = """
            CREATE TABLE IF NOT EXISTS iucr(
              iucr VARCHAR(5) PRIMARY KEY,
              primary_description VARCHAR(50),
              secondary_description VARCHAR(100),
              index_code VARCHAR(1),
              active BOOLEAN
            )
        """
        drop_update = """
            DROP TABLE IF EXISTS update_iucr;
        """
        create_update = """
            CREATE TABLE update_iucr(
              iucr VARCHAR(5) PRIMARY KEY,
              primary_description VARCHAR(50),
              secondary_description VARCHAR(100),
              index_code VARCHAR(1),
              active BOOLEAN
            )
        """
        with db.engine.begin() as curs:
            curs.execute(text(create_final))
            curs.execute(text(drop_update))
            curs.execute(text(create_update))

        with open(os.path.join(self.storage_dir, filename)) as fp:
            copy_st = """
                COPY update_iucr(
                  iucr,
                  primary_description,
                  secondary_description,
                  index_code,
                  active
                )
                FROM STDIN
                WITH (FORMAT CSV, HEADER TRUE, DELIMITER',')
            """

            # need the psycopg connection here so that we can use the COPY
            # statement
            with psycopg2.connect(
                current_app.config["SQLALCHEMY_DATABASE_URI"]
            ) as conn:
                with conn.cursor() as curs:
                    try:
                        curs.copy_expert(copy_st, fp)
                    except psycopg2.extensions.QueryCanceledError as e:
                        raise e

        update_final = """
            INSERT INTO iucr
            SELECT * FROM update_iucr
            ON CONFLICT (iucr) 
            DO UPDATE SET
              iucr = EXCLUDED.iucr,
              primary_description = EXCLUDED.primary_description,
              secondary_description = EXCLUDED.secondary_description,
              index_code = EXCLUDED.index_code,
              active = EXCLUDED.active
        """
        with db.engine.begin() as curs:
            curs.execute(text(update_final))

    def make_data_table(self):
        """
        Step One: Make the data table where the data will eventually live
        """

        create = """
            CREATE TABLE IF NOT EXISTS dat_chicago_crime(
              row_id SERIAL,
              start_date TIMESTAMP,
              end_date TIMESTAMP DEFAULT NULL,
              current_flag BOOLEAN DEFAULT TRUE,
              deleted_flag BOOLEAN DEFAULT FALSE,
              deleted_on TIMESTAMP,
              dup_ver INTEGER,
              source_filename VARCHAR,
              {0}
              PRIMARY KEY(row_id),
              UNIQUE(id, start_date)
            )
            """.format(
            DATA_COLS
        )
        flag_index = """
            CREATE INDEX IF NOT EXISTS deleted_flag_index ON dat_chicago_crime(deleted_flag)
        """
        date_index = """
            CREATE INDEX IF NOT EXISTS deleted_on_index ON dat_chicago_crime(deleted_on)
        """
        with db.engine.begin() as curs:
            curs.execute(text(create))
            curs.execute(text(flag_index))
            curs.execute(text(date_index))

    def make_source_table(self):
        """
        Step Two: Make the table where we will store the incoming data
        """
        drop = "DROP TABLE IF EXISTS src_chicago_crime"
        create = """
            CREATE TABLE IF NOT EXISTS src_chicago_crime(
              {0}
              line_num SERIAL
            )
            """.format(
            DATA_COLS
        )
        with db.engine.begin() as curs:
            curs.execute(text(drop))
            curs.execute(text(create))

    def insert_source_data(self, fp):
        """
        Step Three: Store the incoming data
        """
        copy_st = """
            COPY src_chicago_crime({0})
            FROM STDIN
            WITH (FORMAT CSV, HEADER TRUE, DELIMITER',')
        """.format(
            ",".join(COLS)
        )

        # need the psycopg connection here so that we can use the COPY
        # statement
        with psycopg2.connect(current_app.config["SQLALCHEMY_DATABASE_URI"]) as conn:
            with conn.cursor() as curs:
                try:
                    curs.copy_expert(copy_st, fp)
                except psycopg2.extensions.QueryCanceledError as e:
                    raise e

    def make_new_dup_tables(self):
        """
        Step Four: Make tables that we'll use to find records with the same id
        and where we will find records that are not already in the dat table
        """

        drop = "DROP TABLE IF EXISTS {0}_chicago_crime"
        create = """
            CREATE TABLE IF NOT EXISTS {0}_chicago_crime(
              id BIGINT,
              line_num INT,
              dup_ver INT,
              PRIMARY KEY(dup_ver, id)
            )"""
        create_index = """
            CREATE INDEX {0}_id_ix ON {0}_chicago_crime (id)
        """
        for table in ["new", "dup"]:
            with db.engine.begin() as curs:
                curs.execute(text(drop.format(table)))
                curs.execute(text(create.format(table)))
                curs.execute(text(create_index.format(table)))

    def find_dup_rows(self):
        """
        Step Five: Find records that have the same id
        """
        insert = """
            INSERT INTO dup_chicago_crime
            SELECT
              id,
              line_num,
              RANK() OVER(
                PARTITION BY id ORDER BY line_num DESC
              ) AS dup_ver
            FROM src_chicago_crime
        """
        with db.engine.begin() as curs:
            curs.execute(text(insert))

    def find_new_rows(self):
        """
        Step Six: Find records that don't already exist in the dat table
        """
        insert = """
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
        """
        with db.engine.begin() as curs:
            curs.execute(text(insert))

    def insert_new_rows(self, filename):
        """
        Step Seven: Insert new rows into the dat table
        """
        insert = """
            INSERT INTO dat_chicago_crime (
              start_date,
              dup_ver,
              source_filename,
              {0}
            )
            SELECT
              NOW() AS start_date,
              n.dup_ver,
              :filename AS source_filename,
              {0}
            FROM src_chicago_crime AS s
            JOIN new_chicago_crime AS n
              USING(id)
        """.format(
            ",".join(COLS)
        )
        with db.engine.begin() as curs:
            curs.execute(text(insert), {"filename": filename})

    def find_changed_rows(self):
        """
        Step Eight: Compare incoming data to data already in dat table to find
        rows that have changed.
        """
        drop = "DROP TABLE IF EXISTS chg_chicago_crime"

        create = """
            CREATE TABLE IF NOT EXISTS chg_chicago_crime(
              id INTEGER,
              PRIMARY KEY (id)
            )"""

        with db.engine.begin() as curs:
            curs.execute(text(drop))
            curs.execute(text(create))

        insert = """
            INSERT INTO chg_chicago_crime
              SELECT d.id
              FROM src_chicago_crime AS s
              JOIN dat_chicago_crime AS d
                USING (id)
              WHERE d.current_flag = TRUE
                AND (((s.id IS NOT NULL OR d.id IS NOT NULL) AND s.id <> d.id)
                   OR ((s.orig_date IS NOT NULL OR d.orig_date IS NOT NULL) AND s.orig_date <> d.orig_date)
                   OR ((s.iucr IS NOT NULL OR d.iucr IS NOT NULL) AND s.iucr <> d.iucr)
                   OR ((s.primary_type IS NOT NULL OR d.primary_type IS NOT NULL) AND s.primary_type <> d.primary_type)
                   OR ((s.description IS NOT NULL OR d.description IS NOT NULL) AND s.description <> d.description)
                   OR ((s.location_description IS NOT NULL OR d.location_description IS NOT NULL) AND s.location_description <> d.location_description)
                   OR ((s.arrest IS NOT NULL OR d.arrest IS NOT NULL) AND s.arrest <> d.arrest)
                   OR ((s.domestic IS NOT NULL OR d.domestic IS NOT NULL) AND s.domestic <> d.domestic)
                   OR ((s.fbi_code IS NOT NULL OR d.fbi_code IS NOT NULL) AND s.fbi_code <> d.fbi_code)
                )
        """

        with db.engine.begin() as curs:
            curs.execute(text(insert))

    def flag_changes(self):
        # Update existing records to no longer be current
        update = """
            UPDATE dat_chicago_crime AS d SET
              end_date = NOW(),
              current_flag = FALSE
            FROM chg_chicago_crime AS c
            WHERE d.id = c.id
              AND d.current_flag = TRUE
        """

        # Insert new version
        insert = """
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
        """.format(
            ",".join(COLS)
        )

        with db.engine.begin() as curs:
            curs.execute(text(update))

        with db.engine.begin() as curs:
            curs.execute(text(insert))

    def flag_deletions(self):
        update = """
            UPDATE dat_chicago_crime SET
              deleted_flag = TRUE,
              deleted_on = :deleted_on
            FROM (
              SELECT
                d.id,
                d.updated_on AS updated_on
              FROM dat_chicago_crime AS d
              LEFT JOIN src_chicago_crime AS s
                USING(id)
              WHERE s.id IS NULL
            ) AS subq
            WHERE subq.id = dat_chicago_crime.id
        """
        with db.engine.begin() as curs:
            curs.execute(
                text(update), {"deleted_on": self.file_date.strftime("%Y-%m-%d")}
            )

    def update_view(self):
        create = """
            CREATE MATERIALIZED VIEW changed_records AS (
                SELECT d.* FROM dat_chicago_crime AS d
                JOIN (
                    SELECT id FROM dat_chicago_crime
                    GROUP BY id
                    HAVING(COUNT(*) > 1)
                ) AS s
                    ON d.id = s.id
            )
        """
        with db.engine.connect() as curs:
            try:
                curs.execute(text("REFRESH MATERIALIZED VIEW changed_records"))
                curs.commit()
            except ProgrammingError:
                curs.rollback()
                curs.execute(text(create))
                curs.commit()

    def make_meta_table(self):
        create = """
            CREATE TABLE IF NOT EXISTS etl_tracker(
                filename VARCHAR,
                date_added TIMESTAMP DEFAULT NOW(),
                etl_status VARCHAR,
                file_date DATE
            )
        """
        with db.engine.begin() as curs:
            curs.execute(text(create))

    def update_meta_table(self, filename, status):

        insert = """
            INSERT INTO etl_tracker (filename, etl_status, file_date)
            VALUES (:filename, :status, :file_date)
        """
        with db.engine.begin() as curs:
            curs.execute(
                text(insert),
                {
                    "filename": filename,
                    "status": status,
                    "file_date": self.file_date.strftime("%Y-%m-%d"),
                },
            )


if __name__ == "__main__":
    etl = ETL()
    etl.run()
