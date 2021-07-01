from pathlib import Path

AEDA_DIR = Path(__file__).parent.absolute()

SQLITE3_DB_DIR = AEDA_DIR / "metadata" / "aeda_metadata.db"
SQL_SCRIPTS_DIR = AEDA_DIR / "sql_scripts"
CONFIG_DB = AEDA_DIR / "connection_strings" / "databases.ini"

SUPPORTED_DB_ENGINES = ["sqlite3", "mysql", "postgres", "mssqlserver"]
EXPLORATION_LEVELS = ["server", "catalog", "schema", "table", "view", "query"]

SQL_CREATE_SCRIPTS = {
    "sqlite3": SQL_SCRIPTS_DIR / "sqlite3" / "sqlite3.sql",
    "mysql": SQL_SCRIPTS_DIR / "mysql" / "mysql.sql",
    "postgres": SQL_SCRIPTS_DIR / "postgres" / "postgres.sql",
    "mssqlserver": SQL_SCRIPTS_DIR / "mssqlserver" / "mssqlserver.sql",
}

DATA_TYPES = {
    "mysql": {
        "date_types": ["date", "datetime", "timestamp"],
        "numeric_types": [
            "int",
            "integer",
            "decimal",
            "numeric",
            "float",
            "money",
            "tinyint",
            "bigint",
            "smallint",
            "real",
        ],
    },
}

SQL_SCRIPTS = {
    "columns": {
        "mysql": """SELECT %s AS SERVER_NAME
                            , C.TABLE_CATALOG
                            , C.TABLE_SCHEMA
                            , C.TABLE_NAME
                            , C.COLUMN_NAME
                            , C.ORDINAL_POSITION
                            , C.DATA_TYPE
                    FROM INFORMATION_SCHEMA.COLUMNS AS C 
                        INNER JOIN INFORMATION_SCHEMA.TABLES AS T
                    ON C.TABLE_CATALOG = T.TABLE_CATALOG
                    AND C.TABLE_SCHEMA = T.TABLE_SCHEMA
                    AND C.TABLE_NAME = T.TABLE_NAME
                    AND T.TABLE_TYPE = 'BASE TABLE'
                    AND T.TABLE_CATALOG = %s
                    AND T.TABLE_SCHEMA = %s;""",
        "postgres": """SELECT %s AS SERVER_NAME
                                , C.TABLE_CATALOG
                                , C.TABLE_SCHEMA
                                , C.TABLE_NAME
                                , C.COLUMN_NAME
                                , C.ORDINAL_POSITION
                                , C.DATA_TYPE
                        FROM INFORMATION_SCHEMA.COLUMNS AS C 
                            INNER JOIN INFORMATION_SCHEMA.TABLES AS T
                        ON C.TABLE_CATALOG = T.TABLE_CATALOG
                        AND C.TABLE_SCHEMA = T.TABLE_SCHEMA
                        AND C.TABLE_NAME = T.TABLE_NAME
                        AND T.TABLE_TYPE = 'BASE TABLE'
                        AND T.TABLE_CATALOG = %s
                        AND T.TABLE_SCHEMA = %s;"""
    },
    "insert_into_columns": {
        "mysql": """insert into columns (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE)
                    values (%s, %s, %s, %s, %s, %s, %s);""",
        "postgres": """insert into columns (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE)
                    values (%s, %s, %s, %s, %s, %s, %s);""",
        "sqlite3": """insert into columns (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE)
                    values (?, ?, ?, ?, ?, ?, ?);""",
        "mssqlserver": """insert into columns (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE)
                    values (?, ?, ?, ?, ?, ?, ?);""",
    },
    "insert_into_tables": {
        "mysql": """insert into tables (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, N_COLUMNS, N_ROWS)
                    values (%s, %s, %s, %s, %s, %s);""",
        "postgres": """insert into tables (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, N_COLUMNS, N_ROWS)
                    values (%s, %s, %s, %s, %s, %s);""",
        "sqlite3": """insert into tables (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, N_COLUMNS, N_ROWS)
                    values (?, ?, ?, ?, ?, ?);""",
        "mssqlserver": """insert into tables (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, N_COLUMNS, N_ROWS)
                    values (?, ?, ?, ?, ?, ?);""",
    },
    "insert_into_uniques": {
        "mysql": """insert into uniques (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, DISTINCT_VALUES, NULL_VALUES)
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        "postgres": """insert into uniques (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, DISTINCT_VALUES, NULL_VALUES)
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        "sqlite3": """insert into uniques (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, DISTINCT_VALUES, NULL_VALUES)
                    values (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        "mssqlserver": """insert into uniques (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, DISTINCT_VALUES, NULL_VALUES)
                    values (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
    },
    "insert_into_data_values": {
        "mysql": """insert into data_values (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_VALUE, FREQUENCY_NUMBER)
                    values (%s, %s, %s, %s, %s, %s, %s);""",
        "postgres": """insert into data_values (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_VALUE, FREQUENCY_NUMBER)
                    values (%s, %s, %s, %s, %s, %s, %s);""",
        "sqlite3": """insert into data_values (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_VALUE, FREQUENCY_NUMBER)
                    values (?, ?, ?, ?, ?, ?, ?);""",
        "mssqlserver": """insert into data_values (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_VALUE, FREQUENCY_NUMBER)
                    values (?, ?, ?, ?, ?, ?, ?);""",
    },
    "insert_into_dates": {
        "mysql": """INSERT INTO dates (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_VALUE, FREQUENCY_NUMBER)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        "postgres": """INSERT INTO dates (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_VALUE, FREQUENCY_NUMBER)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        "sqlite3": """INSERT INTO dates (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_VALUE, FREQUENCY_NUMBER)
                    VALUES (?, ?, ?, ?, ?, ?, ?)""",
        "mssqlserver": """INSERT INTO dates (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_VALUE, FREQUENCY_NUMBER)
                    VALUES (?, ?, ?, ?, ?, ?, ?)""",
    },
    "insert_into_stats": {
        "mysql": """insert into stats (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, AVG, STDEV, VAR, SUM, MAX, MIN, `RANGE`)
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""",
        "postgres": """insert into stats (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, AVG, STDEV, VAR, SUM, MAX, MIN, "RANGE")
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""",
        "sqlite3": """insert into stats (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, AVG, STDEV, VAR, SUM, MAX, MIN, "RANGE")
                    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);""",
        "mssqlserver": """insert into stats (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, AVG, STDEV, VAR, SUM, MAX, MIN, "RANGE")
                    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);""",
    },
    "check_if_column_exists": {
        "mysql": """select * from columns
                WHERE SERVER_NAME = %s
                AND TABLE_CATALOG = %s
                AND TABLE_SCHEMA = %s
                AND TABLE_NAME = %s
                AND COLUMN_NAME = %s;""",
        "postgres": """select * from columns
                WHERE SERVER_NAME = %s
                AND TABLE_CATALOG = %s
                AND TABLE_SCHEMA = %s
                AND TABLE_NAME = %s
                AND COLUMN_NAME = %s;""",
        "sqlite3": """select * from columns
                WHERE SERVER_NAME = ?
                AND TABLE_CATALOG = ?
                AND TABLE_SCHEMA = ?
                AND TABLE_NAME = ?
                AND COLUMN_NAME = ?;""",
        "mssqlserver": """select * from columns
                WHERE SERVER_NAME = ?
                AND TABLE_CATALOG = ?
                AND TABLE_SCHEMA = ?
                AND TABLE_NAME = ?
                AND COLUMN_NAME = ?;""",
    },
    "check_if_table_exists": {
        "mysql": """select * from tables
                WHERE SERVER_NAME = %s
                AND TABLE_CATALOG = %s
                AND TABLE_SCHEMA = %s
                AND TABLE_NAME = %s;""",
        "postgres": """select * from tables
                WHERE SERVER_NAME = %s
                AND TABLE_CATALOG = %s
                AND TABLE_SCHEMA = %s
                AND TABLE_NAME = %s;""",
        "sqlite3": """select * from tables
                WHERE SERVER_NAME = ?
                AND TABLE_CATALOG = ?
                AND TABLE_SCHEMA = ?
                AND TABLE_NAME = ?;""",
        "mssqlserver": """select * from tables
                WHERE SERVER_NAME = ?
                AND TABLE_CATALOG = ?
                AND TABLE_SCHEMA = ?
                AND TABLE_NAME = ?;""",
    },
    "check_if_unique_exists": {
        "mysql": """select * from uniques
            WHERE SERVER_NAME = %s
             AND TABLE_CATALOG = %s
             AND TABLE_SCHEMA = %s
             AND TABLE_NAME = %s;""",
        "postgres": """select * from uniques
            WHERE SERVER_NAME = %s
             AND TABLE_CATALOG = %s
             AND TABLE_SCHEMA = %s
             AND TABLE_NAME = %s;""",
        "sqlite3": """select * from uniques
            WHERE SERVER_NAME = ?
             AND TABLE_CATALOG = ?
             AND TABLE_SCHEMA = ?
             AND TABLE_NAME = ?;""",
        "mssqlserver": """select * from uniques
            WHERE SERVER_NAME = ?
             AND TABLE_CATALOG = ?
             AND TABLE_SCHEMA = ?
             AND TABLE_NAME = ?;""",
    },
    "check_if_data_value_exists": {
        "mysql": """select * from data_values
                    WHERE SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s
                    AND COLUMN_NAME = %s;""",
        "postgres": """select * from data_values
                    WHERE SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s
                    AND COLUMN_NAME = %s;""",
        "sqlite3": """select * from data_values
                    WHERE SERVER_NAME = ?
                    AND TABLE_CATALOG = ?
                    AND TABLE_SCHEMA = ?
                    AND TABLE_NAME = ?
                    AND COLUMN_NAME = ?;""",
        "mssqlserver": """select * from data_values
                    WHERE SERVER_NAME = ?
                    AND TABLE_CATALOG = ?
                    AND TABLE_SCHEMA = ?
                    AND TABLE_NAME = ?
                    AND COLUMN_NAME = ?;""",
    },
    "check_if_dates_exists": {
        "mysql": """select * from dates
                    WHERE SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s
                    AND COLUMN_NAME = %s;""",
        "postgres": """select * from dates
                    WHERE SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s
                    AND COLUMN_NAME = %s;""",
        "sqlite3": """select * from dates
                    WHERE SERVER_NAME = ?
                    AND TABLE_CATALOG = ?
                    AND TABLE_SCHEMA = ?
                    AND TABLE_NAME = ?
                    AND COLUMN_NAME = ?;""",
        "mssqlserver": """select * from dates
                    WHERE SERVER_NAME = ?
                    AND TABLE_CATALOG = ?
                    AND TABLE_SCHEMA = ?
                    AND TABLE_NAME = ?
                    AND COLUMN_NAME = ?;""",
    },
    "check_if_stats_exists": {
        "mysql": """select * from stats
                    WHERE SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s
                    AND COLUMN_NAME = %s;""",
        "postgres": """select * from stats
                    WHERE SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s
                    AND COLUMN_NAME = %s;""",
        "sqlite3": """select * from stats
                    WHERE SERVER_NAME = ?
                    AND TABLE_CATALOG = ?
                    AND TABLE_SCHEMA = ?
                    AND TABLE_NAME = ?
                    AND COLUMN_NAME = ?;""",
        "mssqlserver": """select * from stats
                    WHERE SERVER_NAME = ?
                    AND TABLE_CATALOG = ?
                    AND TABLE_SCHEMA = ?
                    AND TABLE_NAME = ?
                    AND COLUMN_NAME = ?;""",
    },
    "delete_from_columns": {
        "mysql": """delete from columns
                WHERE SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s
                    AND COLUMN_NAME = %s;""",
        "postgres": """delete from columns
                WHERE SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s
                    AND COLUMN_NAME = %s;""",
        "sqlite3": """delete from columns
                WHERE SERVER_NAME = ?
                    AND TABLE_CATALOG = ?
                    AND TABLE_SCHEMA = ?
                    AND TABLE_NAME = ?
                    AND COLUMN_NAME = ?;""",
        "mssqlserver": """delete from columns
                WHERE SERVER_NAME = ?
                    AND TABLE_CATALOG = ?
                    AND TABLE_SCHEMA = ?
                    AND TABLE_NAME = ?
                    AND COLUMN_NAME = ?;""",
    },
    "delete_from_tables": {
        "mysql": """delete from tables
                    WHERE SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s;""",
        "postgres": """delete from tables
                    WHERE SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s;""",
        "sqlite3": """delete from tables
                    WHERE SERVER_NAME = ?
                    AND TABLE_CATALOG = ?
                    AND TABLE_SCHEMA = ?
                    AND TABLE_NAME = ?;""",
        "mssqlserver": """delete from tables
                    WHERE SERVER_NAME = ?
                    AND TABLE_CATALOG = ?
                    AND TABLE_SCHEMA = ?
                    AND TABLE_NAME = ?;""",
    },
    "delete_from_uniques": {
        "mysql": """delete from uniques
                    WHERE SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s;""",
        "postgres": """delete from uniques
                    WHERE SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s;""",
        "sqlite3": """delete from uniques
                    WHERE SERVER_NAME = ?
                    AND TABLE_CATALOG = ?
                    AND TABLE_SCHEMA = ?
                    AND TABLE_NAME = ?;""",
        "mssqlserver": """delete from uniques
                    WHERE SERVER_NAME = ?
                    AND TABLE_CATALOG = ?
                    AND TABLE_SCHEMA = ?
                    AND TABLE_NAME = ?;""",
    },
    "delete_from_data_values": {
        "mysql": """delete from data_values
                    WHERE SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s
                    AND COLUMN_NAME = %s;""",
        "postgres": """delete from data_values
                    WHERE SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s
                    AND COLUMN_NAME = %s;""",
        "sqlite3": """delete from data_values
                    WHERE SERVER_NAME = ?
                    AND TABLE_CATALOG = ?
                    AND TABLE_SCHEMA = ?
                    AND TABLE_NAME = ?
                    AND COLUMN_NAME = ?;""",
        "mssqlserver": """delete from data_values
                    WHERE SERVER_NAME = ?
                    AND TABLE_CATALOG = ?
                    AND TABLE_SCHEMA = ?
                    AND TABLE_NAME = ?
                    AND COLUMN_NAME = ?;""",
    },
    "delete_from_dates": {
        "mysql": """delete from dates
                    WHERE SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s
                    AND COLUMN_NAME = %s;""",
        "postgres": """delete from dates
                    WHERE SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s
                    AND COLUMN_NAME = %s;""",
        "sqlite3": """delete from dates
                    WHERE SERVER_NAME = ?
                    AND TABLE_CATALOG = ?
                    AND TABLE_SCHEMA = ?
                    AND TABLE_NAME = ?
                    AND COLUMN_NAME = ?;""",
        "mssqlserver": """delete from dates
                    WHERE SERVER_NAME = ?
                    AND TABLE_CATALOG = ?
                    AND TABLE_SCHEMA = ?
                    AND TABLE_NAME = ?
                    AND COLUMN_NAME = ?;""",
    },
    "delete_from_stats": {
        "mysql": """delete from stats
                    WHERE SERVER_NAME = %s
                     AND TABLE_CATALOG = %s
                     AND TABLE_SCHEMA = %s
                     AND TABLE_NAME = %s
                     AND COLUMN_NAME = %s;""",
        "postgres": """delete from stats
                    WHERE SERVER_NAME = %s
                     AND TABLE_CATALOG = %s
                     AND TABLE_SCHEMA = %s
                     AND TABLE_NAME = %s
                     AND COLUMN_NAME = %s;""",
        "sqlite3": """delete from stats
                    WHERE SERVER_NAME = ?
                     AND TABLE_CATALOG = ?
                     AND TABLE_SCHEMA = ?
                     AND TABLE_NAME = ?
                     AND COLUMN_NAME = ?;""",
        "mssqlserver": """delete from stats
                    WHERE SERVER_NAME = ?
                     AND TABLE_CATALOG = ?
                     AND TABLE_SCHEMA = ?
                     AND TABLE_NAME = ?
                     AND COLUMN_NAME = ?;""",
    },
    "tables": {
        "mysql": """select distinct SERVER_NAME 
                    , TABLE_CATALOG 
                    , TABLE_SCHEMA 
                    , TABLE_NAME
                    from columns
                    where SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s;""",
        "postgres": """select distinct SERVER_NAME 
                    , TABLE_CATALOG 
                    , TABLE_SCHEMA 
                    , TABLE_NAME
                    from columns
                    where SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s;""",
        "sqlite3": """select distinct SERVER_NAME 
                    , TABLE_CATALOG 
                    , TABLE_SCHEMA 
                    , TABLE_NAME
                    from columns
                    where SERVER_NAME = ?
                    AND TABLE_CATALOG = ?
                    AND TABLE_SCHEMA = ?;""",
        "mssqlserver": """select distinct SERVER_NAME 
                    , TABLE_CATALOG 
                    , TABLE_SCHEMA 
                    , TABLE_NAME
                    from columns
                    where SERVER_NAME = ?
                    AND TABLE_CATALOG = ?
                    AND TABLE_SCHEMA = ?;""",
    },
    "number_of_columns": {
        "mysql": """SELECT %s AS SERVER_NAME
                    , TABLE_CATALOG
                    , TABLE_SCHEMA
                    , TABLE_NAME
                    , COUNT(*) AS N_COLUMNS
                    , NULL AS N_ROWS
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s
                    GROUP BY TABLE_CATALOG
                        , TABLE_SCHEMA
                        , TABLE_NAME
                    ORDER BY 1,2,3,4;""",
        "postgres": """SELECT %s AS SERVER_NAME
                    , TABLE_CATALOG
                    , TABLE_SCHEMA
                    , TABLE_NAME
                    , COUNT(*) AS N_COLUMNS
                    , NULL AS N_ROWS
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s
                    GROUP BY TABLE_CATALOG
                        , TABLE_SCHEMA
                        , TABLE_NAME
                    ORDER BY 1,2,3,4;""",
    },
    "number_of_rows": {
        "mysql": """select count(*) as n from {}.{}""",
        "postgres": """select count(*) as n from {}.{}""",
    },
    "update_tables": {
        "mysql": """UPDATE tables 
                    SET N_ROWS = %s
                    WHERE SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s;""",
        "postgres": """UPDATE tables 
                    SET N_ROWS = %s
                    WHERE SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s;""",
        "sqlite3": """UPDATE tables 
                    SET N_ROWS = ?
                    WHERE SERVER_NAME = ?
                    AND TABLE_CATALOG = ?
                    AND TABLE_SCHEMA = ?
                    AND TABLE_NAME = ?;""",
        "mssqlserver": """UPDATE tables 
                    SET N_ROWS = ?
                    WHERE SERVER_NAME = ?
                    AND TABLE_CATALOG = ?
                    AND TABLE_SCHEMA = ?
                    AND TABLE_NAME = ?;""",
    },
    "get_tables": {
        "mysql": """select distinct SERVER_NAME 
                    , TABLE_CATALOG 
                    , TABLE_SCHEMA 
                    , TABLE_NAME
                    , N_ROWS
                    from tables
                    where SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s
                        and N_ROWS > {}
                    order by N_ROWS;""",
        "postgres": """select distinct SERVER_NAME 
                    , TABLE_CATALOG 
                    , TABLE_SCHEMA 
                    , TABLE_NAME
                    , N_ROWS
                    from tables
                    where SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s
                        and N_ROWS > {}
                    order by N_ROWS;""",
        "sqlite3": """select distinct SERVER_NAME 
                    , TABLE_CATALOG 
                    , TABLE_SCHEMA 
                    , TABLE_NAME
                    , N_ROWS
                    from tables
                    where SERVER_NAME = ?
                    AND TABLE_CATALOG = ?
                    AND TABLE_SCHEMA = ?
                        and N_ROWS > {}
                    order by N_ROWS;""",
        "mssqlserver": """select distinct SERVER_NAME 
                    , TABLE_CATALOG 
                    , TABLE_SCHEMA 
                    , TABLE_NAME
                    , N_ROWS
                    from tables
                    where SERVER_NAME = ?
                    AND TABLE_CATALOG = ?
                    AND TABLE_SCHEMA = ?
                        and N_ROWS > {}
                    order by N_ROWS;""",
    },
    "get_columns": {
        "mysql": """select column_name
                        , ORDINAL_POSITION
                        , DATA_TYPE 
                    from columns
                    WHERE SERVER_NAME = %s
                        AND TABLE_CATALOG = %s
                        AND TABLE_SCHEMA = %s
                        AND TABLE_NAME = %s;""",
        "postgres": """select column_name
                        , ORDINAL_POSITION
                        , DATA_TYPE 
                    from columns
                    WHERE SERVER_NAME = %s
                        AND TABLE_CATALOG = %s
                        AND TABLE_SCHEMA = %s
                        AND TABLE_NAME = %s;""",
        "sqlite3": """select column_name
                        , ORDINAL_POSITION
                        , DATA_TYPE 
                    from columns
                    WHERE SERVER_NAME = ?
                        AND TABLE_CATALOG = ?
                        AND TABLE_SCHEMA = ?
                        AND TABLE_NAME = ?;""",
        "mssqlserver": """select column_name
                        , ORDINAL_POSITION
                        , DATA_TYPE 
                    from columns
                    WHERE SERVER_NAME = ?
                        AND TABLE_CATALOG = ?
                        AND TABLE_SCHEMA = ?
                        AND TABLE_NAME = ?;""",
    },
    "get_unique_count": {
        "mysql": """select count(distinct `{0}`) as count_distinct
                            , sum(case when `{0}` is null then 1 else 0 end) as count_null
                    FROM    {1}.{2}""",
        "postgres": """select count(distinct "{0}") as count_distinct
                            , sum(case when "{0}" is null then 1 else 0 end) as count_null
                    FROM    {1}.{2}""",
    },
    "get_distinct_values": {
        "mysql": """select DISTINCT_VALUES 
                    from uniques 
                    where SERVER_NAME = %s
                        AND TABLE_CATALOG = %s
                        AND TABLE_SCHEMA = %s
                        AND TABLE_NAME = %s
                        AND COLUMN_NAME = %s;""",
        "postgres": """select DISTINCT_VALUES 
                    from uniques 
                    where SERVER_NAME = %s
                        AND TABLE_CATALOG = %s
                        AND TABLE_SCHEMA = %s
                        AND TABLE_NAME = %s
                        AND COLUMN_NAME = %s;""",
        "sqlite3": """select DISTINCT_VALUES 
                    from uniques 
                    where SERVER_NAME = ?
                        AND TABLE_CATALOG = ?
                        AND TABLE_SCHEMA = ?
                        AND TABLE_NAME = ?
                        AND COLUMN_NAME = ?;""",
        "mssqlserver": """select DISTINCT_VALUES 
                    from uniques 
                    where SERVER_NAME = ?
                        AND TABLE_CATALOG = ?
                        AND TABLE_SCHEMA = ?
                        AND TABLE_NAME = ?
                        AND COLUMN_NAME = ?;""",
    },
    "get_frequency": {
        "mysql": """SELECT `{0}` AS `{0}`
                        , COUNT(*) AS N 
                    FROM {1}.{2} 
                    GROUP BY `{0}`;""",
        "postgres": """SELECT "{0}" AS "{0}"
                        , COUNT(*) AS N 
                    FROM {1}.{2} 
                    GROUP BY "{0}";""",
    },
    "get_date_columns": {
        "mysql": """select server_name
                            , table_catalog
                            , table_schema
                            , table_name
                            , column_name
                    from columns 
                    WHERE SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s
                    AND lower(DATA_TYPE) IN ('datetime', 'timestamp', 'date', 'datetime2', 'smalldatetime');""",
        "postgres": """select server_name
                            , table_catalog
                            , table_schema
                            , table_name
                            , column_name
                    from columns 
                    WHERE SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s
                    AND lower(DATA_TYPE) IN ('datetime', 'timestamp', 'date', 'datetime2', 'smalldatetime');""",
        "sqlite3": """select server_name
                            , table_catalog
                            , table_schema
                            , table_name
                            , column_name
                    from columns 
                    WHERE SERVER_NAME = ?
                    AND TABLE_CATALOG = ?
                    AND TABLE_SCHEMA = ?
                    AND TABLE_NAME = ?
                    AND lower(DATA_TYPE) IN ('datetime', 'timestamp', 'date', 'datetime2', 'smalldatetime');""",
        "mssqlserver": """select server_name
                            , table_catalog
                            , table_schema
                            , table_name
                            , column_name
                    from columns 
                    WHERE SERVER_NAME = ?
                    AND TABLE_CATALOG = ?
                    AND TABLE_SCHEMA = ?
                    AND TABLE_NAME = ?
                    AND lower(DATA_TYPE) IN ('datetime', 'timestamp', 'date', 'datetime2', 'smalldatetime');""",
    },
    "get_numeric_columns": {
        "mysql": """select server_name
                            , table_catalog
                            , table_schema
                            , table_name
                            , column_name
                        from columns 
                        WHERE SERVER_NAME = %s
                         AND TABLE_CATALOG = %s
                         AND TABLE_SCHEMA = %s
                         AND TABLE_NAME = %s
                         AND lower(DATA_TYPE) IN ('int', 'integer', 'decimal', 'numeric', 'float', 'money', 'tinyint', 'bigint', 'smallint', 'real');""",
        "postgres": """select server_name
                            , table_catalog
                            , table_schema
                            , table_name
                            , column_name
                        from columns 
                        WHERE SERVER_NAME = %s
                         AND TABLE_CATALOG = %s
                         AND TABLE_SCHEMA = %s
                         AND TABLE_NAME = %s
                         AND lower(DATA_TYPE) IN ('int', 'integer', 'decimal', 'numeric', 'float', 'money', 'tinyint', 'bigint', 'smallint', 'real');""",
        "sqlite3": """select server_name
                            , table_catalog
                            , table_schema
                            , table_name
                            , column_name
                        from columns 
                        WHERE SERVER_NAME = ?
                         AND TABLE_CATALOG = ?
                         AND TABLE_SCHEMA = ?
                         AND TABLE_NAME = ?
                         AND lower(DATA_TYPE) IN ('int', 'integer', 'decimal', 'numeric', 'float', 'money', 'tinyint', 'bigint', 'smallint', 'real');""",
        "mssqlserver": """select server_name
                            , table_catalog
                            , table_schema
                            , table_name
                            , column_name
                        from columns 
                        WHERE SERVER_NAME = ?
                         AND TABLE_CATALOG = ?
                         AND TABLE_SCHEMA = ?
                         AND TABLE_NAME = ?
                         AND lower(DATA_TYPE) IN ('int', 'integer', 'decimal', 'numeric', 'float', 'money', 'tinyint', 'bigint', 'smallint', 'real');""",
    },
    "get_first_day_of_month": {
        "mysql": """select date_add(`{0}`, interval - DAY(`{0}`) + 1 DAY) as date
                        , count(*) as N
                    from {1}.{2}
                    group by date_add(`{0}`, interval - DAY(`{0}`) + 1 DAY);"""
    },
    "get_basic_stats": {
        "mysql": """SELECT   CAST(AVG(`{0}`) as FLOAT) AS AVG_
                            , CAST(STD(`{0}`) as FLOAT) as STDEV_
                            , CAST(VARIANCE(`{0}`) as FLOAT) as VAR_
                            , CAST(SUM(`{0}`) as FLOAT) as SUM_
                            , CAST(MAX(`{0}`) as FLOAT) AS MAX_
                            , CAST(MIN(`{0}`) as FLOAT) AS MIN_
                            , CAST(MAX(`{0}`) - MIN(`{0}`) AS FLOAT) as RANGE_
                    FROM    {1}.{2};""",
        "postgres": """SELECT   AVG("{0}") AS AVG_
                                , stddev("{0}") as STDEV_
                                , VARIANCE("{0}") as VAR_
                                , SUM("{0}") as SUM_
                                , MAX("{0}") AS MAX_
                                , MIN("{0}") AS MIN_
                                , MAX("{0}") - MIN("{0}") as RANGE_
                        FROM {1}.{2};""",
    },
    "get_percentiles": {
        "mysql": """with cte1 as 
                    (select `{0}`
                        , NTILE (200) over (order by `{0}`) n_tile
                    from {1}.{2}  
                    where `{0}` is not null)

                    select  t2.P01
                            , t5.P025
                            , t10.P05
                            , t20.P10
                            , t50.P25 as Q1
                            , t100.P50 as Q2
                            , t150.P75 as Q3
                            , t180.P90
                            , t190.P95
                            , t195.P975
                            , t198.P99
                            , t150.P75 - t50.P25 as IQR
                    from    (select min(`{0}`) as P01 from cte1 where n_tile = 2) as t2
                            , (select min(`{0}`) as P025 from cte1 where n_tile = 5) as t5
                            , (select min(`{0}`) as P05 from cte1 where n_tile = 10) as t10
                            , (select min(`{0}`) as P10 from cte1 where n_tile = 20) as t20
                            , (select min(`{0}`) as P25 from cte1 where n_tile = 50) as t50
                            , (select min(`{0}`) as P50 from cte1 where n_tile = 100) as t100
                            , (select min(`{0}`) as P75 from cte1 where n_tile = 150) as t150
                            , (select min(`{0}`) as P90 from cte1 where n_tile = 180) as t180
                            , (select min(`{0}`) as P95 from cte1 where n_tile = 190) as t190
                            , (select min(`{0}`) as P975 from cte1 where n_tile = 195) as t195
                            , (select min(`{0}`) as P99 from cte1 where n_tile = 198) as t198;
                    """,
        "postgres": """select   percentile_disc(0.01) within group (order by "{0}") as P01
                                , percentile_disc(0.025) within group (order by "{0}") as P025
                                , percentile_disc(0.05) within group (order by "{0}") as P05
                                , percentile_disc(0.1) within group (order by "{0}") as P10
                                , percentile_disc(0.25) within group (order by "{0}") as Q1
                                , percentile_disc(0.5) within group (order by "{0}") as Q2
                                , percentile_disc(0.75) within group (order by "{0}") as Q3
                                , percentile_disc(0.90) within group (order by "{0}") as P90
                                , percentile_disc(0.95) within group (order by "{0}") as P95
                                , percentile_disc(0.975) within group (order by "{0}") as P975
                                , percentile_disc(0.99) within group (order by "{0}") as P99
                                , percentile_disc(0.975) within group (order by "{0}") - percentile_disc(0.25) within group (order by "{0}") as IQR
                        from {1}.{2}""",
    },
    "update_percentiles": {
        "mysql": """update stats set P01 = %s
                    , P025 = %s
                    , P05 = %s
                    , P10 = %s
                    , Q1  = %s
                    , Q2  = %s 
                    , Q3  = %s
                    , P90  = %s
                    , P95  = %s
                    , P975 = %s
                    , P99  = %s
                    , IQR  = %s
                    where SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s
                    AND COLUMN_NAME = %s;""",
        "postgres": """update stats set P01 = %s
                    , P025 = %s
                    , P05 = %s
                    , P10 = %s
                    , Q1  = %s
                    , Q2  = %s 
                    , Q3  = %s
                    , P90  = %s
                    , P95  = %s
                    , P975 = %s
                    , P99  = %s
                    , IQR  = %s
                    where SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s
                    AND COLUMN_NAME = %s;""",
        "sqlite3": """update stats set P01 = ?
                    , P025 = ?
                    , P05 = ?
                    , P10 = ?
                    , Q1  = ?
                    , Q2  = ? 
                    , Q3  = ?
                    , P90  = ?
                    , P95  = ?
                    , P975 = ?
                    , P99  = ?
                    , IQR  = ?
                    where SERVER_NAME = ?
                    AND TABLE_CATALOG = ?
                    AND TABLE_SCHEMA = ?
                    AND TABLE_NAME = ?
                    AND COLUMN_NAME = ?;""",
        "mssqlserver": """update stats set P01 = ?
                    , P025 = ?
                    , P05 = ?
                    , P10 = ?
                    , Q1  = ?
                    , Q2  = ? 
                    , Q3  = ?
                    , P90  = ?
                    , P95  = ?
                    , P975 = ?
                    , P99  = ?
                    , IQR  = ?
                    where SERVER_NAME = ?
                    AND TABLE_CATALOG = ?
                    AND TABLE_SCHEMA = ?
                    AND TABLE_NAME = ?
                    AND COLUMN_NAME = ?;""",
    },
}
