
--
-- create a SRC table for chicago_crime and
--   load the initial source file
--
DROP TABLE IF EXISTS SRC_chicago_crime;

CREATE TABLE IF NOT EXISTS SRC_chicago_crime(
  line_num INTEGER,
  iD INTEGER,
  case_Number VARCHAR(10),
  orig_Date TIMESTAMP,
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
  location POINT,
  PRIMARY KEY(line_num)
);

INSERT INTO SRC_chicago_crime
  SELECT 
    id,
    case_number,
    orig_date,
    block,
    iucr,
    primary_type,
    description,
    location_description,
    arrest,
    domestic,
    beat,
    district,
    ward,
    community_area,
    fbi_code,
    x_coordinate,
    y_coordinate,
    year,
    updated_on,
    latitude,
    longitude,
    location
  FROM DUP_chicago_crime
  JOIN DEDUP_chicago_crime 
  USING (dup_chicago_crime_row_id);

--
-- create the DAT table for chicago_crime and  
--  populate it with the first source file
-- 

DROP TABLE IF EXISTS DAT_chicago_crime;

CREATE TABLE IF NOT EXISTS DAT_chicago_crime(
  chicago_crime_row_id SERIAL,
  start_date TIMESTAMP,
  end_date TIMESTAMP DEFAULT NULL,
  current_flag BOOLEAN DEFAULT true,
  id BIGINT,
  case_number VARCHAR(10),
  orig_date TIMESTAMP,
  block VARCHAR(50),
  iUCR VARCHAR(10),
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
  location POINT,
  PRIMARY KEY(chicago_crime_row_id),
  UNIQUE(id, start_date)
);

INSERT INTO DAT_chicago_crime(
  start_date,
  id,
  case_number,
  orig_date,
  block,
  iucr,
  primary_type,
  description,
  location_description,
  arrest,
  domestic,
  beat,
  district,
  ward,
  community_area,
  fbi_code,
  x_coordinate,
  y_coordinate,
  year,
  updated_on,
  latitude,
  longitude,
  location)
SELECT
  NOW() AS start_date,
  id,
  case_number,
  orig_date,
  block,
  iucr,
  primary_type,
  description,
  location_description,
  arrest,
  domestic,
  beat,
  district,
  ward,
  community_area,
  fbi_code,
  x_coordinate,
  y_coordinate,
  year,
  updated_on,
  latitude,
  longitude,
  location
FROM SRC_chicago_crime;

