import logging
from os import close

import pymysql
import utils as _utils
from config import SQL_CREATE_SCRIPTS, SQL_SCRIPTS
from pymysql import cursors

FORMAT = "%(asctime)-15s %(message)s"
logging.basicConfig(level=logging.INFO, format=FORMAT)
logger = logging.getLogger(__name__)

DBENGINE = "mysql"


def create_database(section):
    logger.info("Creating database in MySQL")

    conn_string = _utils.get_db_connection_string(section)
    conn = get_db_connection(conn_string)

    with open(SQL_CREATE_SCRIPTS[DBENGINE], "r") as f:
        sql_script = f.read()
        scripts = sql_script.split(";")
        scripts = [x for x in sql_script.split(";") if len(x.strip()) > 0]
    cursor = conn.cursor()

    for script in scripts:
        cursor.execute(script)
        conn.commit()
    cursor.close()

    close_db_connection(conn)

    logger.info("Database created in MySQL")

    return


def get_db_connection(conn_string):
    conn = None
    try:
        conn = pymysql.connect(
            host=conn_string["host"],
            user=conn_string["user"],
            password=conn_string["password"],
            database=conn_string["schema"],
            port=int(conn_string["port"]),
        )
    except Exception:
        logger.error("Database connection error")
        raise
    return conn


def close_db_connection(conn):
    conn.close()
    # logger.info("MySQL database connection closed")
    return


def explore_server(server_name: str):
    pass


def explore_catalog(server_name: str, catalog_name: str):
    pass


def explore_schema(server_name: str, catalog_name: str, schema_name: str):
    pass


def explore_table(
    server_name: str, catalog_name: str, schema_name: str, table_name: str
):
    pass


def insert_into_table():
    pass


def check_if_record_exists(
    db_engine_metadata,
    server_name,
    table_catalog,
    table_schema,
    table_name,
    column_name,
):
    query = SQL_SCRIPTS["check_if_column_exists"][DBENGINE]
    conn_string = _utils.get_db_connection_string(db_engine_metadata)
    conn = get_db_connection(conn_string)
    cursor = conn.cursor()
    cursor.execute(
        query, (server_name, table_catalog, table_schema, table_name, column_name)
    )
    rowcount = cursor.rowcount
    cursor.close()
    close_db_connection(conn)

    if rowcount > 0:
        return False
    else:
        return True


def delete_from_columns(
    db_engine_metadata,
    server_name,
    table_catalog,
    table_schema,
    table_name,
    column_name,
):
    conn_string = _utils.get_db_connection_string(db_engine_metadata)
    conn = get_db_connection(conn_string)
    query = SQL_SCRIPTS["delete_from_columns"][DBENGINE]
    cursor = conn.cursor()
    cursor.execute(
        query, (server_name, table_catalog, table_schema, table_name, column_name)
    )
    conn.commit()
    cursor.close()
    close_db_connection(conn)
    return


def insert_or_update_columns(db_engine_metadata: str, rows, overwrite: bool = True):
    conn_string = _utils.get_db_connection_string(db_engine_metadata)
    conn = get_db_connection(conn_string)
    query = SQL_SCRIPTS["insert_columns"][DBENGINE]
    cursor = conn.cursor()
    logger.info(
        "Inserting {} rows into `{}.{}.{}`".format(
            len(rows),
            conn_string["host"],
            conn_string["catalog"],
            conn_string["schema"],
        )
    )
    for row in rows:
        (
            server_name,
            table_catalog,
            table_schema,
            table_name,
            column_name,
            ordinal_position,
            data_type,
        ) = row
        if overwrite and check_if_record_exists(
            db_engine_metadata,
            server_name,
            table_catalog,
            table_schema,
            table_name,
            column_name,
        ):
            delete_from_columns(
                db_engine_metadata,
                server_name,
                table_catalog,
                table_schema,
                table_name,
                column_name,
            )

            cursor.execute(
                query,
                (
                    server_name,
                    table_catalog,
                    table_schema,
                    table_name,
                    column_name,
                    ordinal_position,
                    data_type,
                ),
            )
            conn.commit()
    cursor.close()
    logger.info(
        "{} rows inserted into `{}.{}.{}`".format(
            len(rows),
            conn_string["host"],
            conn_string["catalog"],
            conn_string["schema"],
        )
    )
    return


def get_tables(db_engine_source: str, db_engine_metadata: str):
    conn_string_source = _utils.get_db_connection_string(db_engine_source)
    conn_string_metadata = _utils.get_db_connection_string(db_engine_metadata)

    # query = SQL_SCRIPTS["tables"][conn_string_metadata["db_engine"]]
    query = """select distinct SERVER_NAME 
                    , TABLE_CATALOG 
                    , TABLE_SCHEMA 
                    , TABLE_NAME
                    from columns
                    where SERVER_NAME = %s
                    AND TABLE_CATALOG = %s
                    AND TABLE_SCHEMA = %s;"""

    conn = get_db_connection(conn_string_metadata)
    cursor = conn.cursor()
    cursor.execute(
        query,
        (
            conn_string_source["host"],
            conn_string_source["catalog"],
            conn_string_source["schema"],
        ),
    )
    rows = cursor.fetchall()

    cursor.close()
    close_db_connection(conn)

    return rows


def get_number_of_columns(
    db_engine_source: str,
    db_engine_metadata: str,
    server_name: str,
    catalog_name: str,
    schema_name: str,
    table_name: str,
):
    conn_string_source = _utils.get_db_connection_string(db_engine_source)
    conn_string_metadata = _utils.get_db_connection_string(db_engine_metadata)

    query = SQL_SCRIPTS["number_of_columns"][conn_string_metadata["db_engine"]]

    conn = get_db_connection(conn_string_source)
    cursor = conn.cursor()

    cursor.execute(query, (server_name, catalog_name, schema_name, table_name))
    row = cursor.fetchone()

    cursor.close()
    close_db_connection(conn)

    return row


def check_if_table_exists(
    db_engine_metadata: str, server_name, catalog_name, schema_name, table_name
):
    conn_string_metadata = _utils.get_db_connection_string(db_engine_metadata)
    query = SQL_SCRIPTS["check_if_table_exists"][conn_string_metadata["db_engine"]]
    conn = get_db_connection(conn_string_metadata)
    cursor = conn.cursor()
    cursor.execute(query, (server_name, catalog_name, schema_name, table_name))
    rowcount = cursor.rowcount
    cursor.close()
    close_db_connection(conn)
    return True if rowcount > 0 else False


def delete_from_table():
    pass


def get_number_of_rows(db_engine_source: str, schema_name: str, table_name: str):
    conn_string_source = _utils.get_db_connection_string(db_engine_source)
    conn = get_db_connection(conn_string_source)

    # _, _, catalog_name, schema_name = _utils.get_connection_parameters(db_engine_source)

    query = SQL_SCRIPTS["number_of_rows"][conn_string_source["db_engine"]]

    cursor = conn.cursor()
    cursor.execute(query.format(schema_name, table_name))

    num_rows = cursor.fetchone()

    cursor.close()
    close_db_connection(conn)

    return num_rows


def insert_or_update_tables(
    db_engine_source: str, db_engine_metadata: str, rows, overwrite: bool = True
):
    conn_string_metadata = _utils.get_db_connection_string(db_engine_metadata)

    conn = get_db_connection(conn_string_metadata)
    cursor = conn.cursor()

    query_insert = SQL_SCRIPTS["insert_into_tables"][conn_string_metadata["db_engine"]]
    query_delete = SQL_SCRIPTS["delete_from_tables"][conn_string_metadata["db_engine"]]
    query_update = SQL_SCRIPTS["update_tables"][conn_string_metadata["db_engine"]]

    for row in rows:
        server_name, catalog_name, schema_name, table_name = row
        _, _, _, _, n_columns, n_rows = get_number_of_columns(
            db_engine_source,
            db_engine_metadata,
            server_name,
            catalog_name,
            schema_name,
            table_name,
        )
        if (
            check_if_table_exists(
                db_engine_metadata, server_name, catalog_name, schema_name, table_name
            )
            and overwrite
        ):
            cursor.execute(
                query_delete, (server_name, catalog_name, schema_name, table_name)
            )
            conn.commit()
        else:
            continue
        cursor.execute(
            query_insert,
            (server_name, catalog_name, schema_name, table_name, n_columns, n_rows),
        )
        conn.commit()
        num_rows = get_number_of_rows(db_engine_source, schema_name, table_name)
        cursor.execute(
            query_update, (num_rows, server_name, catalog_name, schema_name, table_name)
        )
        conn.commit()

    cursor.close()
    close_db_connection(conn)

    return


def get_uniques(db_engine_source, db_engine_metadata):
    pass


def get_tables_from_metadata(
    db_engine_source: str, db_engine_metadata: str, n_rows: int = 0
):
    """
    Returns SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME and N_ROWS
    """
    conn_string_metadata = _utils.get_db_connection_string(db_engine_metadata)
    query = SQL_SCRIPTS["get_tables"][conn_string_metadata["db_engine"]]

    _, server_name, catalog_name, schema_name = _utils.get_connection_parameters(
        db_engine_source
    )

    conn = get_db_connection(conn_string_metadata)
    cursor = conn.cursor()

    cursor.execute(query.format(n_rows), (server_name, catalog_name, schema_name))

    rows = cursor.fetchall()

    cursor.close()
    close_db_connection(conn)

    return rows


def get_columns_from_metadata(
    db_engine_metadata, server_name, catalog_name, schema_name, table_name
):
    """
    Returns column_name, ORDINAL_POSITION and DATA_TYPE
    """
    conn_string = _utils.get_db_connection_string(db_engine_metadata)
    query = SQL_SCRIPTS["get_columns"][conn_string["db_engine"]]
    conn = get_db_connection(conn_string)
    cursor = conn.cursor()
    cursor.execute(query, (server_name, catalog_name, schema_name, table_name))
    rows = cursor.fetchall()
    cursor.close()
    close_db_connection(conn)
    return rows


def insert_or_update_uniques(
    db_engine_source: str, db_engine_metadata: str, overwrite: bool = True
):
    """
    Parameters:
        db_engine_source (str):

        db_engine_metadata (str):

        overwrite (bool): (Optional)
    """

    def get_unique_values(db_engine_source: str, table_name: str, column_name: str):
        """
        Returns `count_distinct` and `count_null`
        """
        _, _, _, schema_name = _utils.get_connection_parameters(db_engine_source)
        conn_string_source = _utils.get_db_connection_string(db_engine_source)
        conn_source = get_db_connection(conn_string_source)

        query = SQL_SCRIPTS["get_unique_count"][conn_string_source["db_engine"]]

        cursor = conn_source.cursor()
        cursor.execute(query.format(column_name, schema_name, table_name))
        rows = cursor.fetchone()
        cursor.close()
        close_db_connection(conn_source)
        return rows

    def insert_into_uniques(
        db_engine_metadata: str,
        server_name: str,
        catalog_name: str,
        schema_name: str,
        table_name: str,
        column_name: str,
        ordinal_position: str,
        data_type: str,
        count_distinct: int,
        count_null: int,
    ):
        conn_string_metadata = _utils.get_db_connection_string(db_engine_metadata)
        query_insert = SQL_SCRIPTS["insert_into_uniques"][
            conn_string_metadata["db_engine"]
        ]
        conn = get_db_connection(conn_string_metadata)
        cursor = conn.cursor()
        cursor.execute(
            query_insert,
            (
                server_name,
                catalog_name,
                schema_name,
                table_name,
                column_name,
                ordinal_position,
                data_type,
                count_distinct,
                count_null,
            ),
        )
        conn.commit()
        cursor.close()
        close_db_connection(conn)
        return

    def check_if_unique_exists():
        pass

    def check_if_unique_exists(
        db_engine_metadata,
        server_name,
        table_catalog,
        table_schema,
        table_name,
    ):
        query = SQL_SCRIPTS["check_if_unique_exists"][DBENGINE]
        conn_string = _utils.get_db_connection_string(db_engine_metadata)
        conn = get_db_connection(conn_string)
        cursor = conn.cursor()
        cursor.execute(query, (server_name, table_catalog, table_schema, table_name))
        rowcount = cursor.rowcount
        cursor.close()
        close_db_connection(conn)

        if rowcount > 0:
            return True
        else:
            return False

    def delete_from_uniques(
        db_engine_metadata,
        server_name,
        table_catalog,
        table_schema,
        table_name,
    ):
        conn_string = _utils.get_db_connection_string(db_engine_metadata)
        conn = get_db_connection(conn_string)
        query = SQL_SCRIPTS["delete_from_uniques"][conn_string["db_engine"]]
        cursor = conn.cursor()
        cursor.execute(query, (server_name, table_catalog, table_schema, table_name))
        conn.commit()
        cursor.close()
        close_db_connection(conn)
        return

    _, server_name, catalog_name, schema_name = _utils.get_connection_parameters(
        db_engine_source
    )

    table_rows = get_tables_from_metadata(db_engine_source, db_engine_metadata)

    for row in table_rows:
        server_name, catalog_name, schema_name, table_name, n_rows = row
        if check_if_unique_exists(
            db_engine_metadata, server_name, catalog_name, schema_name, table_name
        ):
            delete_from_uniques(
                db_engine_metadata, server_name, catalog_name, schema_name, table_name
            )
        column_rows = get_columns_from_metadata(
            db_engine_metadata, server_name, catalog_name, schema_name, table_name
        )
        for column_row in column_rows:
            column_name, ordinal_position, data_type = column_row
            count_distinct, count_null = get_unique_values(
                db_engine_source, table_name, column_name
            )

            insert_into_uniques(
                db_engine_metadata,
                server_name,
                catalog_name,
                schema_name,
                table_name,
                column_name,
                ordinal_position,
                data_type,
                int(count_distinct),
                int(count_null),
            )


def insert_or_update_data_values(db_engine_source: str, db_engine_metadata: str):
    def check_if_data_value_exists(
        db_engine_metadata,
        server_name,
        table_catalog,
        table_schema,
        table_name,
        column_name,
    ):
        conn_string = _utils.get_db_connection_string(db_engine_metadata)
        query = SQL_SCRIPTS["check_if_data_value_exists"][conn_string["db_engine"]]
        conn = get_db_connection(conn_string)
        cursor = conn.cursor()
        cursor.execute(
            query, (server_name, table_catalog, table_schema, table_name, column_name)
        )
        rowcount = cursor.rowcount
        cursor.close()
        close_db_connection(conn)

        if rowcount > 0:
            return True
        else:
            return False

    def delete_from_data_values(
        db_engine_metadata,
        server_name,
        table_catalog,
        table_schema,
        table_name,
        column_name,
    ):
        conn_string = _utils.get_db_connection_string(db_engine_metadata)
        conn = get_db_connection(conn_string)
        query = SQL_SCRIPTS["delete_from_data_values"][conn_string["db_engine"]]
        cursor = conn.cursor()
        cursor.execute(
            query, (server_name, table_catalog, table_schema, table_name, column_name)
        )
        conn.commit()
        cursor.close()
        close_db_connection(conn)
        return

    def get_frequency(db_engine_source, schema_name, table_name, column_name):
        conn_string = _utils.get_db_connection_string(db_engine_source)
        query = SQL_SCRIPTS["get_frequency"][conn_string["db_engine"]]
        conn = get_db_connection(conn_string)
        cursor = conn.cursor()
        cursor.execute(query.format(column_name, schema_name, table_name))
        rows = cursor.fetchall()
        cursor.close()
        close_db_connection(conn)
        return rows

    def insert_into_data_values(
        db_engine_metadata,
        server_name,
        catalog_name,
        schema_name,
        table_name,
        column_name,
        value,
        num_rows,
    ):
        conn_string_metadata = _utils.get_db_connection_string(db_engine_metadata)
        query_insert = SQL_SCRIPTS["insert_into_data_values"][
            conn_string_metadata["db_engine"]
        ]
        conn = get_db_connection(conn_string_metadata)
        cursor = conn.cursor()
        cursor.execute(
            query_insert,
            (
                server_name,
                catalog_name,
                schema_name,
                table_name,
                column_name,
                value,
                num_rows,
            ),
        )
        conn.commit()
        cursor.close()
        close_db_connection(conn)
        return

    def insert_many_into_data_values(
        db_engine_metadata,
        data_value_rows,
    ):
        conn_string_metadata = _utils.get_db_connection_string(db_engine_metadata)
        query_insert = SQL_SCRIPTS["insert_into_data_values"][
            conn_string_metadata["db_engine"]
        ]
        conn = get_db_connection(conn_string_metadata)
        cursor = conn.cursor()
        cursor.executemany(
            query_insert,
            (list(data_value_rows)),
        )
        conn.commit()
        cursor.close()
        close_db_connection(conn)
        return

    table_rows = get_tables_from_metadata(db_engine_source, db_engine_metadata)
    for table_row in table_rows:
        server_name, catalog_name, schema_name, table_name, n_rows = table_row

        column_rows = get_columns_from_metadata(
            db_engine_metadata, server_name, catalog_name, schema_name, table_name
        )
        for j, column_row in enumerate(column_rows):
            column_name, ordinal_position, data_type = column_row
            if check_if_data_value_exists(
                db_engine_metadata,
                server_name,
                catalog_name,
                schema_name,
                table_name,
                column_name,
            ):
                delete_from_data_values(
                    db_engine_metadata,
                    server_name,
                    catalog_name,
                    schema_name,
                    table_name,
                    column_name,
                )
            data_value_rows = get_frequency(
                db_engine_source, schema_name, table_name, column_name
            )
            logger.info(
                "Data values table {}.{} {}/{}".format(table_name, column_name, j, len(column_rows))
            )
            data = []
            for i, data_value in enumerate(data_value_rows):
                value, num_rows = data_value
                data.append(
                    (server_name,
                    catalog_name,
                    schema_name,
                    table_name,
                    column_name,
                    value,
                    num_rows)
                )
                # logger.info(
                #     "Inserting {}/{} into `data_values`.{} {} {}".format(
                #         i, len(data_value_rows), column_name, value, num_rows
                #     )
                # )
                # insert_into_data_values(
                #     db_engine_metadata,
                #     server_name,
                #     catalog_name,
                #     schema_name,
                #     table_name,
                #     column_name,
                #     value,
                #     num_rows,
                # )
            insert_many_into_data_values(db_engine_metadata, data)


def get_columns(db_engine_source: str):
    conn_string = _utils.get_db_connection_string(db_engine_source)
    conn = get_db_connection(conn_string)

    query = SQL_SCRIPTS["columns"][conn_string["db_engine"]]
    cursor = conn.cursor()
    cursor.execute(
        query, (conn_string["host"], conn_string["catalog"], conn_string["schema"])
    )
    rows = cursor.fetchall()

    logger.info(
        "{} columns from {}.{}.{}".format(
            cursor.rowcount,
            conn_string["host"],
            conn_string["catalog"],
            conn_string["schema"],
        )
    )

    cursor.close()
    close_db_connection(conn)

    return rows


def explore(db_engine_source: str, level: str):
    conn_string = _utils.get_db_connection_string(db_engine_source)
    conn = get_db_connection(conn_string)

    query = SQL_SCRIPTS["columns"][DBENGINE]
    cursor = conn.cursor()
    cursor.execute(
        query, (conn_string["host"], conn_string["catalog"], conn_string["schema"])
    )
    rows = cursor.fetchall()

    logger.info(
        "{} columns from {}.{}.{}".format(
            cursor.rowcount,
            conn_string["host"],
            conn_string["catalog"],
            conn_string["schema"],
        )
    )

    cursor.close()
    close_db_connection(conn)

    return rows
