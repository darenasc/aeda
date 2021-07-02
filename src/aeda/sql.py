import logging
from pathlib import Path

import sqlite3

from config import SQL_CREATE_SCRIPTS, SQL_SCRIPTS
import utils as _utils


FORMAT = "%(asctime)-15s %(message)s"
logging.basicConfig(level=logging.INFO, format=FORMAT)
logger = logging.getLogger(__name__)


def create_database(section: str):
    """Creates the metadata database based on the database engine.

    Parameters:
        section (str): Section of the .ini file with the database configuration with connection parameters file.
    """
    conn_string = _utils.get_db_connection_string(section)
    conn = _utils.get_db_connection(conn_string)

    logger.info("Creating a {} database".format(conn_string["db_engine"]))

    if conn_string["db_engine"] in ["mysql", "postgres"]:
        with open(SQL_CREATE_SCRIPTS[conn_string["db_engine"]], "r") as f:
            sql_script = f.read()
            scripts = sql_script.split(";")
            scripts = [x for x in sql_script.split(";") if len(x.strip()) > 0]
        cursor = conn.cursor()

        for script in scripts:
            cursor.execute(script)
            conn.commit()
        cursor.close()

        conn.close()
    elif conn_string["db_engine"] == "sqlite3":
        dbname = str(conn_string["schema"] + ".db")
        if not Path(conn_string["folder"]).is_dir():
            Path(conn_string["folder"]).mkdir(parents=True)

        conn = sqlite3.connect(Path(conn_string["folder"]) / dbname)

        with open(SQL_CREATE_SCRIPTS[conn_string["db_engine"]], "r") as f:
            sql_script = f.read()
        sql_scripts = sql_script.split(";")

        cursor = conn.cursor()

        for script in sql_scripts:
            cursor.execute(script)
            conn.commit()

        cursor.close()
        conn.close()

    logger.info("A {} database created".format(conn_string["db_engine"]))

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
    conn_string = _utils.get_db_connection_string(db_engine_metadata)
    query = SQL_SCRIPTS["check_if_column_exists"][conn_string["db_engine"]]
    conn = _utils.get_db_connection(conn_string)
    cursor = conn.cursor()
    cursor.execute(
        query, (server_name, table_catalog, table_schema, table_name, column_name)
    )
    rowcount = cursor.rowcount
    cursor.close()
    conn.close()

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
    conn = _utils.get_db_connection(conn_string)
    query = SQL_SCRIPTS["delete_from_columns"][conn_string["db_engine"]]
    cursor = conn.cursor()
    cursor.execute(
        query, (server_name, table_catalog, table_schema, table_name, column_name)
    )
    conn.commit()
    cursor.close()
    conn.close()
    return


def insert_or_update_columns(
    db_engine_source, db_engine_metadata: str, overwrite: bool = True
):
    column_rows = get_columns(db_engine_source)
    conn_string = _utils.get_db_connection_string(db_engine_metadata)
    conn = _utils.get_db_connection(conn_string)
    query = SQL_SCRIPTS["insert_into_columns"][conn_string["db_engine"]]
    cursor = conn.cursor()
    if conn_string["db_engine"] == "sqlite3":
        logger.info(
            "Inserting {} rows into `{}.db`".format(
                len(column_rows),
                conn_string["schema"],
            )
        )
    else:
        logger.info(
            "Inserting {} rows into `{}.{}.{}`".format(
                len(column_rows),
                conn_string["host"],
                conn_string["catalog"],
                conn_string["schema"],
            )
        )
    for row in column_rows:
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
    if conn_string["db_engine"] == "sqlite3":
        logger.info(
            "{} rows inserted into `{}.db`".format(
                len(column_rows),
                conn_string["schema"],
            )
        )
    else:
        logger.info(
            "{} rows inserted into `{}.{}.{}`".format(
                len(column_rows),
                conn_string["host"],
                conn_string["catalog"],
                conn_string["schema"],
            )
        )
    return


def get_tables(db_engine_source: str, db_engine_metadata: str):
    conn_string_source = _utils.get_db_connection_string(db_engine_source)
    conn_string_metadata = _utils.get_db_connection_string(db_engine_metadata)

    query = SQL_SCRIPTS["tables"][conn_string_metadata["db_engine"]]

    conn = _utils.get_db_connection(conn_string_metadata)
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
    conn.close()

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

    query = SQL_SCRIPTS["number_of_columns"][conn_string_source["db_engine"]]

    conn = _utils.get_db_connection(conn_string_source)
    cursor = conn.cursor()

    cursor.execute(query, (server_name, catalog_name, schema_name, table_name))
    row = cursor.fetchone()

    cursor.close()
    conn.close()

    return row


def check_if_table_exists(
    db_engine_metadata: str, server_name, catalog_name, schema_name, table_name
):
    conn_string_metadata = _utils.get_db_connection_string(db_engine_metadata)
    query = SQL_SCRIPTS["check_if_table_exists"][conn_string_metadata["db_engine"]]
    conn = _utils.get_db_connection(conn_string_metadata)
    cursor = conn.cursor()
    if conn_string_metadata["db_engine"] in ["mssqlserver", "sqlite3"]:
        rows = cursor.execute(
            query, (server_name, catalog_name, schema_name, table_name)
        ).fetchall()
        rowcount = len(rows)
    else:
        cursor.execute(query, (server_name, catalog_name, schema_name, table_name))
        rowcount = cursor.rowcount
    cursor.close()
    conn.close()
    return True if rowcount > 0 else False


def delete_from_table():
    pass


def get_number_of_rows(db_engine_source: str, schema_name: str, table_name: str):
    conn_string_source = _utils.get_db_connection_string(db_engine_source)
    conn = _utils.get_db_connection(conn_string_source)

    query = SQL_SCRIPTS["number_of_rows"][conn_string_source["db_engine"]]

    cursor = conn.cursor()
    cursor.execute(query.format(schema_name, table_name))

    num_rows = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    return num_rows


def insert_or_update_tables(
    db_engine_source: str, db_engine_metadata: str, overwrite: bool = True
):
    conn_string_metadata = _utils.get_db_connection_string(db_engine_metadata)

    conn = _utils.get_db_connection(conn_string_metadata)
    cursor = conn.cursor()

    query_insert = SQL_SCRIPTS["insert_into_tables"][conn_string_metadata["db_engine"]]
    query_delete = SQL_SCRIPTS["delete_from_tables"][conn_string_metadata["db_engine"]]
    query_update = SQL_SCRIPTS["update_tables"][conn_string_metadata["db_engine"]]

    table_rows = get_tables(db_engine_source, db_engine_metadata)
    logger.info("{} tables to be inserted into `tables`".format(len(table_rows)))
    for row in table_rows:
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
    conn.close()

    logger.info("{} tables inserted into `tables`".format(len(table_rows)))

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

    conn = _utils.get_db_connection(conn_string_metadata)
    cursor = conn.cursor()

    cursor.execute(query.format(n_rows), (server_name, catalog_name, schema_name))

    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return rows


def get_columns_from_metadata(
    db_engine_metadata, server_name, catalog_name, schema_name, table_name
):
    """
    Returns column_name, ORDINAL_POSITION and DATA_TYPE
    """
    conn_string = _utils.get_db_connection_string(db_engine_metadata)
    query = SQL_SCRIPTS["get_columns"][conn_string["db_engine"]]
    conn = _utils.get_db_connection(conn_string)
    cursor = conn.cursor()
    cursor.execute(query, (server_name, catalog_name, schema_name, table_name))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
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
        conn_source = _utils.get_db_connection(conn_string_source)

        query = SQL_SCRIPTS["get_unique_count"][conn_string_source["db_engine"]]

        cursor = conn_source.cursor()
        cursor.execute(query.format(column_name, schema_name, table_name))
        rows = cursor.fetchone()
        cursor.close()
        conn_source.close()
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
        conn = _utils.get_db_connection(conn_string_metadata)
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
        conn.close()
        return

    def check_if_unique_exists(
        db_engine_metadata,
        server_name,
        table_catalog,
        table_schema,
        table_name,
    ):
        conn_string = _utils.get_db_connection_string(db_engine_metadata)
        query = SQL_SCRIPTS["check_if_unique_exists"][conn_string["db_engine"]]
        conn = _utils.get_db_connection(conn_string)
        cursor = conn.cursor()
        cursor.execute(query, (server_name, table_catalog, table_schema, table_name))
        rows = cursor.fetchall()
        rowcount = len(rows)
        cursor.close()
        conn.close()

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
        conn = _utils.get_db_connection(conn_string)
        query = SQL_SCRIPTS["delete_from_uniques"][conn_string["db_engine"]]
        cursor = conn.cursor()
        cursor.execute(query, (server_name, table_catalog, table_schema, table_name))
        conn.commit()
        cursor.close()
        conn.close()
        return

    _, server_name, catalog_name, schema_name = _utils.get_connection_parameters(
        db_engine_source
    )

    table_rows = get_tables_from_metadata(db_engine_source, db_engine_metadata)

    logger.info("{} tables counting unique and null values".format(len(table_rows)))

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
        logger.info("{} columns inserted into `uniques`".format(len(column_rows)))


def insert_or_update_data_values(
    db_engine_source: str, db_engine_metadata: str, threshold: int = 5_000
):
    """
    Parameters:
        db_engine_source (str):

        db_engine_metadata (str):

        threshold (int): [Optional] Maximum value of unique values to compute the frequency.
    """

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
        conn = _utils.get_db_connection(conn_string)
        cursor = conn.cursor()
        cursor.execute(
            query, (server_name, table_catalog, table_schema, table_name, column_name)
        )
        rows = cursor.fetchall()
        rowcount = len(rows)
        cursor.close()
        conn.close()

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
        conn = _utils.get_db_connection(conn_string)
        query = SQL_SCRIPTS["delete_from_data_values"][conn_string["db_engine"]]
        cursor = conn.cursor()
        cursor.execute(
            query, (server_name, table_catalog, table_schema, table_name, column_name)
        )
        conn.commit()
        cursor.close()
        conn.close()
        return

    def get_frequency(db_engine_source, schema_name, table_name, column_name):
        conn_string = _utils.get_db_connection_string(db_engine_source)
        query = SQL_SCRIPTS["get_frequency"][conn_string["db_engine"]]
        conn = _utils.get_db_connection(conn_string)
        cursor = conn.cursor()
        cursor.execute(query.format(column_name, schema_name, table_name))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
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
        conn = _utils.get_db_connection(conn_string_metadata)
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
        conn.close()
        return

    def insert_many_into_data_values(
        db_engine_metadata,
        data_value_rows,
    ):
        conn_string_metadata = _utils.get_db_connection_string(db_engine_metadata)
        query_insert = SQL_SCRIPTS["insert_into_data_values"][
            conn_string_metadata["db_engine"]
        ]
        conn = _utils.get_db_connection(conn_string_metadata)
        cursor = conn.cursor()
        if conn_string_metadata["db_engine"] == "sqlite3":
            cursor.executemany(
                query_insert,
                list(data_value_rows),
            )
        else:
            cursor.executemany(
                query_insert,
                (list(data_value_rows)),
            )
        conn.commit()
        cursor.close()
        conn.close()
        return

    def get_num_distinct_values(
        db_engine_metadata,
        server_name,
        catalog_name,
        schema_name,
        table_name,
        column_name,
    ):
        conn_string_metadata = _utils.get_db_connection_string(db_engine_metadata)
        query = SQL_SCRIPTS["get_distinct_values"][conn_string_metadata["db_engine"]]
        conn = _utils.get_db_connection(conn_string_metadata)
        cursor = conn.cursor()
        cursor.execute(
            query, (server_name, catalog_name, schema_name, table_name, column_name)
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        return row[0]

    table_rows = get_tables_from_metadata(db_engine_source, db_engine_metadata)
    for table_row in table_rows:
        server_name, catalog_name, schema_name, table_name, n_rows = table_row

        column_rows = get_columns_from_metadata(
            db_engine_metadata, server_name, catalog_name, schema_name, table_name
        )
        for j, column_row in enumerate(column_rows):
            column_name, ordinal_position, data_type = column_row
            num_uniques = get_num_distinct_values(
                db_engine_metadata,
                server_name,
                catalog_name,
                schema_name,
                table_name,
                column_name,
            )
            if num_uniques > threshold:
                logger.info(
                    "{}.{}.{} has {} unique values, more than the threshold {}".format(
                        table_name, column_name, value, num_uniques, threshold
                    )
                )
                continue
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

            data = []
            for i, data_value in enumerate(data_value_rows):
                value, num_rows = data_value
                data.append(
                    (
                        server_name,
                        catalog_name,
                        schema_name,
                        table_name,
                        column_name,
                        str(value),
                        int(num_rows),
                    )
                )
            logger.info(
                "Inserting {} records into `data_values` for {}.{} {}/{}".format(
                    len(data), table_name, column_name, j + 1, len(column_rows)
                )
            )
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
    return


def insert_or_update_dates(db_engine_source, db_engine_metadata):
    def get_date_columns(
        db_engine_metadata,
        server_name,
        catalog_name,
        schema_name,
        table_name,
    ):
        """
        Returns server_name, table_catalog, table_schema, table_name, column_name
        """
        conn_string_metadata = _utils.get_db_connection_string(db_engine_metadata)
        query = SQL_SCRIPTS["get_date_columns"][conn_string_metadata["db_engine"]]
        conn = _utils.get_db_connection(conn_string_metadata)
        cursor = conn.cursor()
        cursor.execute(query, (server_name, catalog_name, schema_name, table_name))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return rows

    def check_if_dates_exists(
        db_engine_metadata,
        server_name,
        catalog_name,
        schema_name,
        table_name,
        column_name,
    ):
        conn_string = _utils.get_db_connection_string(db_engine_metadata)
        query = SQL_SCRIPTS["check_if_dates_exists"][conn_string["db_engine"]]
        conn = _utils.get_db_connection(conn_string)
        cursor = conn.cursor()
        cursor.execute(
            query, (server_name, catalog_name, schema_name, table_name, column_name)
        )
        rowcount = cursor.rowcount
        cursor.close()
        conn.close()

        if rowcount > 0:
            return True
        else:
            return False

    def delete_from_dates(
        db_engine_metadata,
        server_name,
        catalog_name,
        schema_name,
        table_name,
        column_name,
    ):
        conn_string = _utils.get_db_connection_string(db_engine_metadata)
        conn = _utils.get_db_connection(conn_string)
        query = SQL_SCRIPTS["delete_from_dates"][conn_string["db_engine"]]
        cursor = conn.cursor()
        cursor.execute(
            query, (server_name, catalog_name, schema_name, table_name, column_name)
        )
        conn.commit()
        cursor.close()
        conn.close()
        return

    def get_dates(db_engine_source, schema_name, table_name, column_name):
        conn_string = _utils.get_db_connection_string(db_engine_source)
        query = SQL_SCRIPTS["get_first_day_of_month"][conn_string["db_engine"]]
        conn = _utils.get_db_connection(conn_string)
        cursor = conn.cursor()
        cursor.execute(query.format(column_name, schema_name, table_name))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return rows

    def insert_many_into_dates(db_engine_metadata: str, data: list):
        conn_string_metadata = _utils.get_db_connection_string(db_engine_metadata)
        query_insert = SQL_SCRIPTS["insert_into_dates"][
            conn_string_metadata["db_engine"]
        ]
        conn = _utils.get_db_connection(conn_string_metadata)
        cursor = conn.cursor()
        cursor.executemany(
            query_insert,
            (data),
        )
        conn.commit()
        cursor.close()
        conn.close()
        return

    table_rows = get_tables_from_metadata(db_engine_source, db_engine_metadata)
    for table_row in table_rows:
        server_name, catalog_name, schema_name, table_name, n_rows = table_row
        column_rows = get_date_columns(
            db_engine_metadata, server_name, catalog_name, schema_name, table_name
        )
        for column_row in column_rows:
            _, _, _, _, column_name = column_row
            if check_if_dates_exists(
                db_engine_metadata,
                server_name,
                catalog_name,
                schema_name,
                table_name,
                column_name,
            ):
                delete_from_dates(
                    db_engine_metadata,
                    server_name,
                    catalog_name,
                    schema_name,
                    table_name,
                    column_name,
                )
            date_rows = get_dates(
                db_engine_source, schema_name, table_name, column_name
            )
            data = []
            for date_row in date_rows:
                date_value, frequency = date_row
                data.append(
                    (
                        server_name,
                        catalog_name,
                        schema_name,
                        table_name,
                        column_name,
                        date_value,
                        int(frequency),
                    )
                )
            logger.info(
                "Inserting {} records into `dates` for {}.{}.{}".format(
                    len(data), table_name, column_name, date_value
                )
            )
            insert_many_into_dates(db_engine_metadata, data)


def insert_or_update_stats(
    db_engine_source: str, db_engine_metadata: str, with_percentiles: bool = False
):
    def get_numeric_columns(
        db_engine_metadata, server_name, catalog_name, schema_name, table_name
    ):
        """
        Returns server_name, catalog_name, schema_name, table_name, column_name
        """
        conn_string_metadata = _utils.get_db_connection_string(db_engine_metadata)
        query = SQL_SCRIPTS["get_numeric_columns"][conn_string_metadata["db_engine"]]
        conn = _utils.get_db_connection(conn_string_metadata)
        cursor = conn.cursor()
        cursor.execute(query, (server_name, catalog_name, schema_name, table_name))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return rows

    def check_if_stats_exists(
        db_engine_metadata,
        server_name,
        catalog_name,
        schema_name,
        table_name,
        column_name,
    ):
        conn_string = _utils.get_db_connection_string(db_engine_metadata)
        query = SQL_SCRIPTS["check_if_stats_exists"][conn_string["db_engine"]]
        conn = _utils.get_db_connection(conn_string)
        cursor = conn.cursor()
        rows = cursor.execute(
            query, (server_name, catalog_name, schema_name, table_name, column_name)
        ).fetchall()
        rowcount = len(rows)
        cursor.close()
        conn.close()

        if rowcount > 0:
            return True
        else:
            return False

    def delete_from_stats(
        db_engine_metadata,
        server_name,
        catalog_name,
        schema_name,
        table_name,
        column_name,
    ):
        conn_string = _utils.get_db_connection_string(db_engine_metadata)
        conn = _utils.get_db_connection(conn_string)
        query = SQL_SCRIPTS["delete_from_stats"][conn_string["db_engine"]]
        cursor = conn.cursor()
        cursor.execute(
            query, (server_name, catalog_name, schema_name, table_name, column_name)
        )
        conn.commit()
        cursor.close()
        conn.close()
        return

    def get_basic_stats(
        db_engine_source, catalog_name, schema_name, table_name, column_name
    ):
        """
        Returns avg, stdev, var, sum, max, min, range
        """
        conn_string = _utils.get_db_connection_string(db_engine_source)
        query = SQL_SCRIPTS["get_basic_stats"][conn_string["db_engine"]]
        conn = _utils.get_db_connection(conn_string)
        cursor = conn.cursor()
        cursor.execute(query.format(column_name, schema_name, table_name))
        rows = cursor.fetchone()
        cursor.close()
        conn.close()
        return rows

    def insert_many_into_stats(db_engine_metadata, data):
        conn_string_metadata = _utils.get_db_connection_string(db_engine_metadata)
        query_insert = SQL_SCRIPTS["insert_into_stats"][
            conn_string_metadata["db_engine"]
        ]
        conn = _utils.get_db_connection(conn_string_metadata)
        cursor = conn.cursor()
        cursor.executemany(
            query_insert,
            (data),
        )
        conn.commit()
        cursor.close()
        conn.close()
        return

    def get_percentiles(db_engine_source, schema_name, table_name, column_name):
        conn_string = _utils.get_db_connection_string(db_engine_source)
        query = SQL_SCRIPTS["get_percentiles"][conn_string["db_engine"]]
        conn = _utils.get_db_connection(conn_string)
        cursor = conn.cursor()
        cursor.execute(query.format(column_name, schema_name, table_name))
        rows = cursor.fetchone()
        cursor.close()
        conn.close()
        return rows

    def update_percentiles(db_engine_metadata, data):
        conn_string_metadata = _utils.get_db_connection_string(db_engine_metadata)
        query_insert = SQL_SCRIPTS["update_percentiles"][
            conn_string_metadata["db_engine"]
        ]
        conn = _utils.get_db_connection(conn_string_metadata)
        cursor = conn.cursor()
        cursor.executemany(
            query_insert,
            (data),
        )
        conn.commit()
        cursor.close()
        conn.close()
        return

    table_rows = get_tables_from_metadata(db_engine_source, db_engine_metadata)
    for table_row in table_rows:
        server_name, catalog_name, schema_name, table_name, n_rows = table_row
        column_rows = get_numeric_columns(
            db_engine_metadata, server_name, catalog_name, schema_name, table_name
        )
        data = []
        if with_percentiles:
            percentiles = []
        for column_row in column_rows:
            _, _, _, _, column_name = column_row
            if check_if_stats_exists(
                db_engine_metadata,
                server_name,
                catalog_name,
                schema_name,
                table_name,
                column_name,
            ):
                delete_from_stats(
                    db_engine_metadata,
                    server_name,
                    catalog_name,
                    schema_name,
                    table_name,
                    column_name,
                )
            stats_rows = get_basic_stats(
                db_engine_source, catalog_name, schema_name, table_name, column_name
            )
            if with_percentiles:
                percentile_rows = get_percentiles(
                    db_engine_source, schema_name, table_name, column_name
                )
                (
                    p01,
                    p025,
                    p05,
                    p10,
                    q2,
                    q3,
                    q4,
                    p90,
                    p95,
                    p975,
                    p99,
                    iqr,
                ) = percentile_rows
                percentiles.append(
                    (
                        float(p01),
                        float(p025),
                        float(p05),
                        float(p10),
                        float(q2),
                        float(q3),
                        float(q4),
                        float(p90),
                        float(p95),
                        float(p975),
                        float(p99),
                        float(iqr),
                        server_name,
                        catalog_name,
                        schema_name,
                        table_name,
                        column_name,
                    )
                )
            avg_, stdev_, var_, sum_, max_, min_, range_ = stats_rows
            data.append(
                (
                    server_name,
                    catalog_name,
                    schema_name,
                    table_name,
                    column_name,
                    float(avg_),
                    float(stdev_),
                    float(var_),
                    float(sum_),
                    float(max_),
                    float(min_),
                    float(range_),
                )
            )
        if len(data) > 0:
            logger.info(
                "Inserting {} records into `stats` for {}.{}".format(
                    len(data), schema_name, table_name
                )
            )
            insert_many_into_stats(db_engine_metadata, data)
            if with_percentiles:
                update_percentiles(db_engine_metadata, percentiles)
        else:
            logger.info(
                "{} numeric columns found in {}.{}".format(
                    len(data), schema_name, table_name
                )
            )


def get_columns(db_engine_source: str):
    conn_string = _utils.get_db_connection_string(db_engine_source)
    conn = _utils.get_db_connection(conn_string)

    query = SQL_SCRIPTS["columns"][conn_string["db_engine"]]
    cursor = conn.cursor()
    cursor.execute(
        query, (conn_string["host"], conn_string["catalog"], conn_string["schema"])
    )
    rows = cursor.fetchall()

    logger.info(
        "{} columns from {}.{}.{}".format(
            len(rows),
            conn_string["host"],
            conn_string["catalog"],
            conn_string["schema"],
        )
    )

    cursor.close()
    conn.close()

    return rows


def explore(db_engine_source: str, level: str):
    conn_string = _utils.get_db_connection_string(db_engine_source)
    conn = _utils.get_db_connection(conn_string)

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
    conn.close()

    return rows
