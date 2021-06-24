from pathlib import Path

AEDA_DIR = Path(__file__).parent.absolute()

SQLITE3_DB_DIR = AEDA_DIR / "metadata" / "aeda_metadata.db"
SQL_SCRIPTS_DIR = AEDA_DIR / "sql_scripts"
CONFIG_DB = AEDA_DIR / "connection_strings" / "databases.ini"

SUPPORTED_DB_ENGINES = ["sqlite3", "mysql"]
EXPLORATION_LEVELS = ["server", "catalog", "schema", "table", "view", "query"]

SQL_CREATE_SCRIPTS = {
    "sqlite3": SQL_SCRIPTS_DIR / "sqlite3" / "sqlite3.sql",
    "mysql": SQL_SCRIPTS_DIR / "mysql" / "mysql.sql",
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
                AND T.TABLE_SCHEMA = %s;"""
    },
    "insert_columns": {
        "mysql": """insert into columns (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE)
                    values (%s, %s, %s, %s, %s, %s, %s);"""
    },
    "insert_into_tables": {
        "mysql": """insert into tables (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, N_COLUMNS, N_ROWS)
                    values (%s, %s, %s, %s, %s, %s);"""
    },
    "insert_into_uniques": {
        "mysql": """insert into uniques (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, DISTINCT_VALUES, NULL_VALUES)
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    },
    "insert_into_data_values": {
        "mysql": """insert into data_values (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_VALUE, FREQUENCY_NUMBER)
                    values (%s, %s, %s, %s, %s, %s, %s);"""
    },
    "check_if_column_exists": {
        "mysql": """select * from columns
                WHERE SERVER_NAME = %s
                AND TABLE_CATALOG = %s
                AND TABLE_SCHEMA = %s
                AND TABLE_NAME = %s
                AND COLUMN_NAME = %s;"""
    },
    "check_if_table_exists": {
        "mysql": """select * from tables
                WHERE SERVER_NAME = %s
                AND TABLE_CATALOG = %s
                AND TABLE_SCHEMA = %s
                AND TABLE_NAME = %s;"""
    },
    "check_if_unique_exists": {
        "mysql": """select * from uniques
            WHERE SERVER_NAME = %s
             AND TABLE_CATALOG = %s
             AND TABLE_SCHEMA = %s
             AND TABLE_NAME = %s;"""
    },
    "check_if_data_value_exists": {
        "mysql": """select * from data_values
                    WHERE SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s
                    AND COLUMN_NAME = %s;"""
    },
    "delete_from_columns": {
        "mysql": """delete from columns
                WHERE SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s
                    AND COLUMN_NAME = %s;"""
    },
    "delete_from_tables": {
        "mysql": """delete from tables
                    WHERE SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s;"""
    },
    "delete_from_uniques": {
        "mysql": """delete from uniques
                    WHERE SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s;"""
    },
    "delete_from_data_values": {
        "mysql": """delete from data_values
                    WHERE SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s
                    AND COLUMN_NAME = %s;"""
    },
    "tables": {
        "mysql": """select distinct SERVER_NAME 
                    , TABLE_CATALOG 
                    , TABLE_SCHEMA 
                    , TABLE_NAME
                    from columns
                    where SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s;"""
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
                ORDER BY 1,2,3,4;"""
    },
    "number_of_rows": {"mysql": """select count(*) as n from {}.{}"""},
    "update_tables": {
        "mysql": """UPDATE tables 
                    SET N_ROWS = %s
                    WHERE SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s;"""
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
                    order by N_ROWS;"""
    },
    "get_columns": {
        "mysql": """select column_name
                        , ORDINAL_POSITION
                        , DATA_TYPE 
                    from columns
                    WHERE SERVER_NAME = %s
                        AND TABLE_CATALOG = %s
                        AND TABLE_SCHEMA = %s
                        AND TABLE_NAME = %s;"""
    },
    "get_unique_count": {
        "mysql": """select count(distinct `{0}`) as count_distinct
                            , sum(case when `{0}` is null then 1 else 0 end) as count_null
                    FROM    {1}.{2}"""
    },
    "get_frequency": {
        "mysql": """SELECT `{0}` AS `{0}`
                        , COUNT(*) AS N 
                    FROM {1}.{2} 
                    GROUP BY `{0}`;"""
    },
}
