# aeda (automated-exploratory-data-analysis)

Supported databases:

| DB | SOURCE | METADATA |
| --- |:---:|:---:|
| MySQL | :white_check_mark: | :white_check_mark: |
| MariaDB | :white_check_mark: | :white_check_mark: |
| Postgres | :white_check_mark: | :white_check_mark: |
| MSSQLServer | :white_check_mark: | :white_check_mark: |
| SQLite3 | | :white_check_mark: |
| Snowflake | | :white_check_mark: |

To create test databases.
```
python aeda_.py create_db sqlite3
python aeda_.py create_db mysql --section mysql-metadata
python aeda_.py create_db mysql --section mysql-demo

python aeda_.py explore mysql-demo mysql-metadata
python aeda_.py explore mysql-demo postgres-metadata
python aeda_.py explore postgres-demo mysql-metadata
python aeda_.py explore postgres-demo postgres-metadata

python aeda_.py explore postgres-demo sqlserver-metadata
python aeda_.py explore mysql-demo sqlserver-metadata
python aeda_.py explore mysql-demo sqlserver-metadata
python aeda_.py explore sqlserver-production sqlserver-metadata
python aeda_.py explore sqlserver-production mysql-metadata
python aeda_.py explore sqlserver-production postgres-metadata
python aeda_.py explore sqlserver-sales sqlserver-metadata
python aeda_.py explore sqlserver-sales mysql-metadata
python aeda_.py explore sqlserver-sales postgres-metadata

python aeda_.py explore mariadb-world sqlserver-metadata
python aeda_.py explore sqlserver-production mariadb-metadata
python aeda_.py explore sqlserver-sales mariadb-metadata

python aeda_.py explore sqlserver-production sqlite3-metadata
python aeda_.py explore sqlserver-sales sqlite3-metadata
```

To run the `aeda`:
```
python aeda_.py explore mysql-demo mysql-metadata
```

Where `mysql-demo` and `mysql-metadata` are sections in the `databases.ini` configuration file.

## Connections

Connections are declared in `databases.ini`.

```
[<REFERENCE-NAME>]
db_engine = <DB-ENGINE>
host = <HOST>
schema = <SCHEMA>
catalog = <CATALOG>
user = <USER>
password = <PASSWORD>
port = <PORT>
encoding = <>
```

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