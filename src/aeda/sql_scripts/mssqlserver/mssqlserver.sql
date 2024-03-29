DROP DATABASE IF EXISTS metadata;
GO

CREATE DATABASE metadata;
GO
USE metadata;
GO

CREATE TABLE columns (SERVER_NAME NVARCHAR(255)
      , TABLE_CATALOG NVARCHAR(255)
      , TABLE_SCHEMA NVARCHAR(255)
      , TABLE_NAME NVARCHAR(255)
      , COLUMN_NAME NVARCHAR(255)
      , ORDINAL_POSITION INT
      , DATA_TYPE NVARCHAR(255));
      
CREATE TABLE tables (SERVER_NAME NVARCHAR(255)
      , TABLE_CATALOG NVARCHAR(255)
      , TABLE_SCHEMA NVARCHAR(255)
      , TABLE_NAME NVARCHAR(255)
      , N_COLUMNS INT
      , N_ROWS INT);

CREATE TABLE uniques (SERVER_NAME NVARCHAR(255)
      , TABLE_CATALOG NVARCHAR(255)
      , TABLE_SCHEMA NVARCHAR(255)
      , TABLE_NAME NVARCHAR(255)
      , COLUMN_NAME NVARCHAR(255)
      , ORDINAL_POSITION INT
      , DATA_TYPE NVARCHAR(255)
      , DISTINCT_VALUES INT
      , NULL_VALUES INT);

CREATE TABLE data_values (SERVER_NAME NVARCHAR(255)
      , TABLE_CATALOG NVARCHAR(255)
      , TABLE_SCHEMA NVARCHAR(255)
      , TABLE_NAME NVARCHAR(255)
      , COLUMN_NAME NVARCHAR(255)
      , DATA_VALUE NVARCHAR(255)
      , FREQUENCY_NUMBER INT
      , FREQUENCY_PERCENTAGE FLOAT);

CREATE TABLE dates (SERVER_NAME NVARCHAR(255)
      , TABLE_CATALOG NVARCHAR(255)
      , TABLE_SCHEMA NVARCHAR(255)
      , TABLE_NAME NVARCHAR(255)
      , COLUMN_NAME NVARCHAR(255)
      , DATA_VALUE NVARCHAR(255)
      , FREQUENCY_NUMBER INT
      , FREQUENCY_PERCENTAGE FLOAT);

CREATE TABLE stats (SERVER_NAME NVARCHAR(255)
      , TABLE_CATALOG NVARCHAR(255)
      , TABLE_SCHEMA NVARCHAR(255)
      , TABLE_NAME NVARCHAR(255)
      , COLUMN_NAME NVARCHAR(255)
      , AVG FLOAT
      , STDEV FLOAT
      , [VAR] FLOAT
      , SUM FLOAT
      , MAX FLOAT
      , MIN FLOAT
      , [RANGE] FLOAT
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
      , IQR FLOAT);
GO

CREATE VIEW servers AS 
select      server_name
            , table_catalog 
            , table_schema 
            , count(distinct table_name) as n_tables
            , sum(n_columns) as n_columns 
            , sum(n_rows) as n_rows 
from tables
group by server_name, table_catalog , table_schema;
GO

-- TODO - add view of dates summary