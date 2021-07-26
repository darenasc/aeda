# SQL Code

## Metadata schema

There are 6 tables in the metadata schema.

| Table name | Description |
| --- | --- |
| `columns` | Each row is a column of a table |
| `tables` | Each row is a table of a database |
| `uniques` | Similar to the `columns` table adding the number of rows and null values |
| `data_values` | Each row is a unique data value of a column |
| `dates` | Similar to `data_values` for date related columns |
| `stats` | Each row presents statistics of a numeric column of a table |

All 6 tables have 4 columns in common used to join the data between them:

| Column name | Data type | Description |
| --- | --- | --- |
| SERVER_NAME | VARCHAR(255) | IP or host of the source database |
| TABLE_CATALOG | VARCHAR(255) | Catalog of the source database |
| TABLE_SCHEMA | VARCHAR(255) | Schema of the source database |
| TABLE_NAME | VARCHAR(255) | Table name |

### `columns` table

| Column name | Data type |
| --- | --- |
| SERVER_NAME | VARCHAR(255) |
| TABLE_CATALOG | VARCHAR(255) |
| TABLE_SCHEMA | VARCHAR(255) |
| TABLE_NAME | VARCHAR(255) |
| COLUMN_NAME | VARCHAR(255) |
| ORDINAL_POSITION | INTEGER |
| DATA_TYPE | VARCHAR(255) |

### `tables` table

| Column name | Data type |
| --- | --- |
| SERVER_NAME | VARCHAR(255) |
| TABLE_CATALOG | VARCHAR(255) |
| TABLE_SCHEMA | VARCHAR(255) |
| TABLE_NAME | VARCHAR(255) |
| N_COLUMNS | INTEGER |
| N_ROWS | INTEGER |

### `uniques` table

| Column name | Data type |
| --- | --- |
| SERVER_NAME | VARCHAR(255) |
| TABLE_CATALOG | VARCHAR(255) |
| TABLE_SCHEMA | VARCHAR(255) |
| TABLE_NAME | VARCHAR(255) |
| COLUMN_NAME | VARCHAR(255) |
| ORDINAL_POSITION | INTEGER |
| DATA_TYPE | VARCHAR(255) |
| DISTINCT_VALUES | INTEGER |
| NULL_VALUES | INTEGER |

### `data_values` and `dates` tables

| Column name | Data type |
| --- | --- |
| SERVER_NAME | VARCHAR(255) |
| TABLE_CATALOG | VARCHAR(255) |
| TABLE_SCHEMA | VARCHAR(255) |
| TABLE_NAME | VARCHAR(255) |
| COLUMN_NAME | VARCHAR(255) |
| ORDINAL_POSITION | INTEGER |
| DATA_TYPE | VARCHAR(255) |
| FREQUENCY_NUMBER | INTEGER |
| FREQUENCY_PERCENTAGE | FLOAT |

### `stats` table

| Column name | Data type |
| --- | --- |
| SERVER_NAME | VARCHAR(255) |
| TABLE_CATALOG | VARCHAR(255) |
| TABLE_SCHEMA | VARCHAR(255) |
| TABLE_NAME | VARCHAR(255) |
| COLUMN_NAME | VARCHAR(255) |
| AVG | FLOAT |
| STDEV | FLOAT |
| VAR | FLOAT |
| SUM | FLOAT |
| MAX | FLOAT |
| MIN | FLOAT |
| RANGE | FLOAT |
| P01 | FLOAT |
| P025 | FLOAT |
| P05 | FLOAT |
| P10 | FLOAT |
| Q1 | FLOAT |
| Q2 | FLOAT |
| Q3 | FLOAT |
| P90 | FLOAT |
| P95 | FLOAT |
| P975 | FLOAT |
| P99 | FLOAT |
| IQR | FLOAT |

## SQL Queries

For each database engine there are predefined queries to be used to extract the 
metadata from the source database. 

These queries are defined in the [`config.py`](../src/aeda/config.py) file per 
database engine. 

### Adding new DB engines

To add new database engines you need to add the SQL scripts to the [`config.py`](../src/aeda/config.py) 
file and modify the `SUPPORTED_DB_ENGINES` variable.

The following are the queries that are required to add to support a database 
engine as `source` or as `metatada`. 

| ENGINE | QUERY | DESCRIPTION |
| --- | --- | --- |
| `source` | `columns` | Gets the column names from the `INFORMATION_SCHEMA` or similar filtering by `catalog` and `schema`.|
| `source` | `number_of_columns` | Gets the number of rows per table from the `INFORMATION_SCHEMA` or similar from the `source` |
| `source` | `number_of_rows` | Gets the number of rows per table from the `source` |
| `source` | `get_unique_count` | |
| `source` | `get_frequency` | |
| `source` | `get_first_day_of_month` | |
| `source` | `get_basic_stats` | |
| `source` | `get_percentiles` | |
| `metadata` | `tables` | |
| `metadata` | `insert_into_columns` | |
| `metadata` | `insert_into_tables` | |
| `metadata` | `insert_into_uniques` | |
| `metadata` | `insert_into_data_values` | |
| `metadata` | `insert_into_dates` | |
| `metadata` | `insert_into_stats` | |
| `metadata` | `check_if_column_exists` | |
| `metadata` | `check_if_table_exists` | |
| `metadata` | `check_if_unique_exists` | |
| `metadata` | `check_if_data_value_exists` | |
| `metadata` | `check_if_dates_exists` | |
| `metadata` | `check_if_stats_exists` | |
| `metadata` | `delete_from_columns` | |
| `metadata` | `delete_from_tables` | |
| `metadata` | `delete_from_uniques` | |
| `metadata` | `delete_from_data_values` | |
| `metadata` | `delete_from_dates` | |
| `metadata` | `delete_from_stats` | |
| `metadata` | `update_tables` | |
| `metadata` | `get_tables` | |
| `metadata` | `get_columns` | |
| `metadata` | `get_distinct_values` | |
| `metadata` | `get_date_columns` | |
| `metadata` | `get_numeric_columns` | |
| `metadata` | `update_percentiles` | |