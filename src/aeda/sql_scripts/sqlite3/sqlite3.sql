CREATE TABLE IF NOT EXISTS columns (SERVER_NAME TEXT
      , TABLE_CATALOG TEXT
      , TABLE_SCHEMA TEXT
      , TABLE_NAME TEXT
      , COLUMN_NAME TEXT
      , ORDINAL_POSITION INTEGER
      , DATA_TYPE TEXT
      , PRIMARY KEY (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME));

CREATE TABLE IF NOT EXISTS tables (SERVER_NAME TEXT
      , TABLE_CATALOG TEXT
      , TABLE_SCHEMA TEXT
      , TABLE_NAME TEXT
      , N_COLUMNS INTEGER
      , N_ROWS INTEGER
      , PRIMARY KEY (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME));

CREATE TABLE IF NOT EXISTS uniques (SERVER_NAME TEXT
      , TABLE_CATALOG TEXT
      , TABLE_SCHEMA TEXT
      , TABLE_NAME TEXT
      , COLUMN_NAME TEXT
      , ORDINAL_POSITION INTEGER
      , DATA_TYPE TEXT
      , DISTINCT_VALUES INTEGER
      , NULL_VALUES INTEGER
      , PRIMARY KEY (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME));

CREATE TABLE IF NOT EXISTS data_values (SERVER_NAME TEXT
      , TABLE_CATALOG TEXT
      , TABLE_SCHEMA TEXT
      , TABLE_NAME TEXT
      , COLUMN_NAME TEXT
      , DATA_VALUE TEXT
      , FREQUENCY_NUMBER INTEGER
      , FREQUENCY_PERCENTAGE FLOAT
      , PRIMARY KEY (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_VALUE));

CREATE TABLE IF NOT EXISTS dates (SERVER_NAME TEXT
      , TABLE_CATALOG TEXT
      , TABLE_SCHEMA TEXT
      , TABLE_NAME TEXT
      , COLUMN_NAME TEXT
      , DATA_VALUE TEXT
      , FREQUENCY_NUMBER INTEGER
      , FREQUENCY_PERCENTAGE FLOAT
      , PRIMARY KEY (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_VALUE));

CREATE TABLE IF NOT EXISTS stats (SERVER_NAME TEXT
      , TABLE_CATALOG TEXT
      , TABLE_SCHEMA TEXT
      , TABLE_NAME TEXT
      , COLUMN_NAME TEXT
      , AVG FLOAT
      , STDEV FLOAT
      , VAR FLOAT
      , SUM FLOAT
      , MAX FLOAT
      , MIN FLOAT
      , RANGE FLOAT
      , P01 FLOAT
      , P025 FLOAT
      , P05 FLOAT
      , P10 FLOAT
      , Q1 FLOAT
      , Q2 FLOAT
      , Q3 FLOAT
      , P90 FLOAT
      , P95 FLOAT
      , P975 FLOAT
      , P99 FLOAT
      , IQR FLOAT
      , PRIMARY KEY (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME));

CREATE VIEW IF NOT EXISTS servers AS 
select      server_name
            , table_catalog 
            , table_schema 
            , count(distinct table_name) as n_tables
            , sum(n_columns) as n_columns 
            , sum(n_rows) as n_rows 
from tables
group by server_name, table_catalog , table_schema;

CREATE VIEW IF NOT EXISTS summary_dates AS 
select      server_name
            , table_catalog 
            , table_schema 
            , table_name
            , column_name
            , max(date(data_value)) as max_date
            , min(date(data_value)) as min_date
            , JULIANDAY(max(date(data_value))) - JULIANDAY(min(date(data_value))) as range_days
            , COUNT(*) as records
            , COUNT(*) / (JULIANDAY(max(date(data_value))) - JULIANDAY(min(date(data_value)))) as records_per_day
from dates d 
where DATA_VALUE is not null
group by SERVER_NAME , TABLE_CATALOG , TABLE_SCHEMA , TABLE_NAME , COLUMN_NAME;