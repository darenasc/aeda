DROP DATABASE IF EXISTS metadata;
CREATE DATABASE metadata;

\c metadata;

CREATE TABLE IF NOT EXISTS metadata.public.columns (SERVER_NAME VARCHAR(255)
      , TABLE_CATALOG VARCHAR(255)
      , TABLE_SCHEMA VARCHAR(255)
      , TABLE_NAME VARCHAR(255)
      , COLUMN_NAME VARCHAR(255)
      , ORDINAL_POSITION INTEGER
      , DATA_TYPE VARCHAR(255));

CREATE TABLE IF NOT EXISTS metadata.public.tables (SERVER_NAME VARCHAR(255)
      , TABLE_CATALOG VARCHAR(255)
      , TABLE_SCHEMA VARCHAR(255)
      , TABLE_NAME VARCHAR(255)
      , N_COLUMNS INTEGER
      , N_ROWS INTEGER);
     
CREATE TABLE IF NOT EXISTS metadata.public.uniques (SERVER_NAME VARCHAR(255)
      , TABLE_CATALOG VARCHAR(255)
      , TABLE_SCHEMA VARCHAR(255)
      , TABLE_NAME VARCHAR(255)
      , COLUMN_NAME VARCHAR(255)
      , ORDINAL_POSITION INTEGER
      , DATA_TYPE VARCHAR(255)
      , DISTINCT_VALUES INTEGER
      , NULL_VALUES INTEGER);

CREATE TABLE IF NOT EXISTS metadata.public.data_values (SERVER_NAME VARCHAR(255)
      , TABLE_CATALOG VARCHAR(255)
      , TABLE_SCHEMA VARCHAR(255)
      , TABLE_NAME VARCHAR(255)
      , COLUMN_NAME VARCHAR(255)
      , DATA_VALUE VARCHAR(255)
      , FREQUENCY_NUMBER INTEGER
      , FREQUENCY_PERCENTAGE FLOAT);

CREATE TABLE IF NOT EXISTS metadata.public.dates (SERVER_NAME VARCHAR(255)
      , TABLE_CATALOG VARCHAR(255)
      , TABLE_SCHEMA VARCHAR(255)
      , TABLE_NAME VARCHAR(255)
      , COLUMN_NAME VARCHAR(255)
      , DATA_VALUE VARCHAR(255)
      , FREQUENCY_NUMBER INTEGER
      , FREQUENCY_PERCENTAGE FLOAT);

CREATE TABLE IF NOT EXISTS metadata.public.stats (SERVER_NAME VARCHAR(255)
      , TABLE_CATALOG VARCHAR(255)
      , TABLE_SCHEMA VARCHAR(255)
      , TABLE_NAME VARCHAR(255)
      , COLUMN_NAME VARCHAR(255)
      , AVG FLOAT
      , STDEV FLOAT
      , VAR FLOAT
      , SUM FLOAT
      , MAX FLOAT
      , MIN FLOAT
      , "RANGE" FLOAT
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

CREATE VIEW servers AS 
select      server_name
            , table_catalog
            , table_schema
            , count(distinct table_name) as n_tables
            , sum(n_columns) as n_columns
            , sum(n_rows) as n_rows 
from "tables" 
group by server_name, table_catalog , table_schema;