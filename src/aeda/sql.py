import logging
import sqlite3
from pathlib import Path

from termcolor import colored
from tqdm import tqdm

from aeda import utils as _utils
from aeda.config import MAX_LENGTH_VALUES, SQL_CREATE_SCRIPTS, SQL_SCRIPTS

FORMAT = "%(asctime)-15s %(message)s"
logging.basicConfig(level=logging.INFO, format=FORMAT)
logger = logging.getLogger(__name__)


def create_database(section: str) -> None:
    """Creates the metadata database based on the database engine.

    Parameters:
        section (str): Section of the .ini file with the database configuration with connection parameters file.
    """
    conn_string = _utils.get_db_connection_string(section)
    conn = _utils.get_db_connection(conn_string)

    logger.info(f"""Creating a {colored(conn_string["db_engine"], 'green')} database""")

    if conn_string["db_engine"] in ["mysql", "postgres"]:
        # TODO Check if create database works for `mysql`
        conn.autocommit = True
        with open(SQL_CREATE_SCRIPTS[conn_string["db_engine"]], "r") as f:
            sql_script = f.read()
            scripts = sql_script.split(";")
            scripts = [
                x
                for x in sql_script.split(";")
                if len(x.strip()) > 0 and not x.strip().startswith("--")
            ]
        cursor = conn.cursor()

        for script in scripts:
            script += ";"
            cursor.execute(script)
            conn.commit()
        cursor.close()
        conn.close()
    elif conn_string["db_engine"] in ["snowflake"]:
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

        logger.info(
            f"Creating database {colored(dbname, 'green')} in {colored(conn_string['folder'], 'yellow')}"
        )

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
    else:
        logger.info(
            f'{colored(conn_string["db_engine"], "green")} database not supported yet. Please create a new issue on github https://github.com/darenasc/aeda/issues'
        )
        return None

    logger.info(
        f"A {colored(conn_string['db_engine'], 'green')} metadata database created"
    )

    return


def explore_server(server_name: str):
    # TODO: Implement explore_server
    pass


def explore_catalog(server_name: str, catalog_name: str):
    # TODO: Implement explore_catalog
    pass


def explore_schema(server_name: str, catalog_name: str, schema_name: str):
    # TODO: Implement explore_schema
    pass


def explore_table(
    server_name: str, catalog_name: str, schema_name: str, table_name: str
):
    # TODO: Implement explore_table
    pass


def insert_into_table():
    pass


def get_chunks(data: list, n: int = 5_000):
    for i in range(0, len(data), n):
        yield data[i : i + n]


def insert_or_update_columns(
    db_engine_source: str, db_engine_metadata: str, overwrite: bool = True
):
    """
    Parameters:
        db_engine_source (str): Section in the databases.ini file with databases connection parameters.

        db_engine_metadata (str): Section in the databases.ini file with the metadata database connection.

        overwrite (bool) = True.
    """

    def check_if_column_exists(
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
        rows = cursor.fetchall()
        rowcount = len(rows)
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

    def insert_many_into_columns(db_engine_metadata, data):
        conn_string = _utils.get_db_connection_string(db_engine_metadata)
        conn = _utils.get_db_connection(conn_string)
        query = SQL_SCRIPTS["insert_into_columns"][conn_string["db_engine"]]
        cursor = conn.cursor()

        for _ in tqdm(get_chunks(data)):
            cursor.executemany(query, (_))
            conn.commit()

        cursor.close()
        conn.close()
        return

    column_rows = get_columns(db_engine_source)
    data = []
    pbar = tqdm(column_rows, desc="Columns - ")
    for row in pbar:
        (
            server_name,
            table_catalog,
            table_schema,
            table_name,
            column_name,
            ordinal_position,
            data_type,
        ) = row

        pbar.set_description(f"Columns: {table_name}.{column_name}")
        if overwrite and check_if_column_exists(
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

            data.append(
                (
                    server_name,
                    table_catalog,
                    table_schema,
                    table_name,
                    column_name,
                    ordinal_position,
                    data_type,
                )
            )
    insert_many_into_columns(db_engine_metadata, data)
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
    cursor.execute(query, (server_name, catalog_name, schema_name, table_name))
    rows = cursor.fetchall()
    rowcount = len(rows)
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
    try:
        # FIXME chars in SAPHANA with '=', '>', or '#' chars in table names
        cursor.execute(query.format(schema_name, table_name))

        num_rows = cursor.fetchone()[0]
    except Exception as e:
        logger.error(
            f"Exception: {e} Could't get number of rows from table {colored('.'.join([schema_name,table_name]), 'red')}"
        )
        return None
    finally:
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
    # logger.info("{} tables to be inserted into `tables`".format(len(table_rows)))
    pbar = tqdm(table_rows, desc="Tables: ")
    for row in pbar:
        server_name, catalog_name, schema_name, table_name = row
        _, _, _, _, n_columns, n_rows = get_number_of_columns(
            db_engine_source,
            db_engine_metadata,
            server_name,
            catalog_name,
            schema_name,
            table_name,
        )
        pbar.set_description(f"Tables - {table_name}")
        if check_if_table_exists(
            db_engine_metadata, server_name, catalog_name, schema_name, table_name
        ):
            if overwrite:
                cursor.execute(
                    query_delete, (server_name, catalog_name, schema_name, table_name)
                )
                conn.commit()
            elif not overwrite:
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
    conn.close()

    # logger.info("{} tables inserted into `tables`".format(len(table_rows)))

    return


def get_uniques(db_engine_source, db_engine_metadata):
    pass


def get_tables_from_metadata(
    db_engine_source: str, db_engine_metadata: str, min_n_rows: int = 0
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

    cursor.execute(query.format(min_n_rows), (server_name, catalog_name, schema_name))

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
    db_engine_source: str,
    db_engine_metadata: str,
    overwrite: bool = True,
    min_n_rows: int = 0,
    max_rows: int = 100_000,
    max_columns: int = 50,
):
    """Insert or update unique values.

    Args:
        db_engine_source (str): Connection parameters of the source.
        db_engine_metadata (str): Connection parameters of the target.
        overwrite (bool, optional): Overwrite the data. Defaults to True.
        min_n_rows (int, optional): Minimum row count. Defaults to 0.
        max_rows (int, optional): Max number of rows to process using pandas.
            Use 0 (zero) to process everything on the server side.
            Defaults to 100_000.
        max_columns (int, optional): Max number of columns to query all to the
            database. Defaults to 50.
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

        return True if rowcount > 0 else False

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

    table_rows = get_tables_from_metadata(
        db_engine_source, db_engine_metadata, min_n_rows=min_n_rows
    )

    # logger.info("{} tables counting unique and null values".format(len(table_rows)))
    pbar = tqdm(table_rows, desc="Uniques")
    for row in pbar:
        server_name, catalog_name, schema_name, table_name, n_rows = row
        pbar.set_description(f"Uniques - {table_name} ({n_rows:,} rows)")
        if check_if_unique_exists(
            db_engine_metadata, server_name, catalog_name, schema_name, table_name
        ):
            if overwrite:
                delete_from_uniques(
                    db_engine_metadata,
                    server_name,
                    catalog_name,
                    schema_name,
                    table_name,
                )
            else:
                continue
        column_rows = get_columns_from_metadata(
            db_engine_metadata, server_name, catalog_name, schema_name, table_name
        )
        if n_rows < max_rows:
            # process in pandas for tables with less than `max_rows` rows
            import math

            import pandas as pd

            df_ = pd.DataFrame(
                column_rows, columns=["column_name", "ordinal_position", "data_type"]
            )
            _, _, _, schema_name = _utils.get_connection_parameters(db_engine_source)
            conn_string_source = _utils.get_db_connection_string(db_engine_source)
            conn_source = _utils.get_db_connection(conn_string_source)
            cursor = conn_source.cursor()
            count_distinct = []
            count_null = []
            for i in range(math.ceil(len(df_.column_name.to_list()) / max_columns)):
                columns = df_.column_name.to_list()[
                    i * max_columns : i * max_columns + max_columns
                ]

                query = f"select {', '.join(columns)} from {catalog_name}.{schema_name}.{table_name};"

                cursor.execute(query)
                rows = cursor.fetchall()
                df_aux = pd.DataFrame(rows, columns=columns)

                count_distinct.append(df_aux.nunique())
                count_null.append(df_aux.isnull().sum())
            cursor.close()
            conn_source.close()

            df_unique = pd.concat(count_distinct).reset_index()
            df_unique.columns = ["column_name", "distinct_values"]  # type:ignore
            df_null = pd.concat(count_null).reset_index()
            df_null.columns = ["column_name", "null_values"]  # type:ignore

            df_ = df_.merge(df_unique).merge(df_null)
            for _, r in df_.iterrows():
                insert_into_uniques(
                    db_engine_metadata,
                    server_name,
                    catalog_name,
                    schema_name,
                    table_name,
                    r["column_name"],
                    r["ordinal_position"],
                    r["data_type"],
                    r["distinct_values"],
                    r["null_values"],
                )
        else:
            pbar1 = tqdm(column_rows, leave=False)
            for column_row in pbar1:
                column_name, ordinal_position, data_type = column_row
                pbar1.set_description(f"Uniques - {table_name}.{column_name}")
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
                    int(count_distinct),  # type:ignore
                    int(count_null),  # type:ignore
                )
            # logger.info("{} columns inserted into `uniques`".format(len(column_rows)))


def insert_or_update_data_values(
    db_engine_source: str,
    db_engine_metadata: str,
    overwrite: bool = False,
    threshold: int = 5_000,
    min_n_rows: int = 0,
    max_rows: int = 50_000,
    max_columns: int = 50,
):
    """Insert or update data values.

    Args:
        db_engine_source (str): Connection parameters of the source.
        db_engine_metadata (str): Connection parameters of the target.
        overwrite (bool, optional): Overwrite the data. Defaults to False.
        threshold (int, optional): Maximum value of unique values to compute
            the frequency.. Defaults to 5_000.
        min_n_rows (int, optional): Minimum row count to start processing.
            Defaults to 0.
        max_rows (int, optional): Max number of rows to process using pandas.
            Use 0 (zero) to process everything on the server side.
            Defaults to 100_000.
        max_columns (int, optional): Max number of columns to query all to the
            database. Defaults to 50.
    """

    def get_data_values_columns(
        db_engine_metadata, server_name, catalog_name, schema_name, table_name
    ):
        """
        Returns column_name, ORDINAL_POSITION and DATA_TYPE
        """
        conn_string = _utils.get_db_connection_string(db_engine_metadata)
        query = SQL_SCRIPTS["get_data_values_columns"][conn_string["db_engine"]]
        conn = _utils.get_db_connection(conn_string)
        cursor = conn.cursor()
        cursor.execute(query, (server_name, catalog_name, schema_name, table_name))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return rows

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
        if row:
            return row[0]
        else:
            return -1

    table_rows = get_tables_from_metadata(
        db_engine_source, db_engine_metadata, min_n_rows=min_n_rows
    )
    pbar = tqdm(table_rows, desc="Data values")
    for table_row in pbar:
        server_name, catalog_name, schema_name, table_name, n_rows = table_row
        pbar.set_description(f"Data values - {table_name} ({n_rows:,})")
        column_rows = get_data_values_columns(
            db_engine_metadata, server_name, catalog_name, schema_name, table_name
        )

        if n_rows < max_rows:
            # Process in pandas
            import math

            import pandas as pd

            df_ = pd.DataFrame(
                column_rows, columns=["column_name", "ordinal_position", "data_type"]
            )
            _, _, _, schema_name = _utils.get_connection_parameters(db_engine_source)
            conn_string_source = _utils.get_db_connection_string(db_engine_source)
            conn_source = _utils.get_db_connection(conn_string_source)
            cursor = conn_source.cursor()
            dfs_ = []
            for i in range(math.ceil(len(df_.column_name.to_list()) / max_columns)):
                columns = df_.column_name.to_list()[
                    i * max_columns : i * max_columns + max_columns
                ]

                query = f"select {', '.join(columns)} from {catalog_name}.{schema_name}.{table_name};"
                cursor.execute(query)
                rows = cursor.fetchall()
                df_aux = pd.DataFrame(rows, columns=columns)

                for col in df_aux.columns:
                    df_values = df_aux[col].value_counts(dropna=False).reset_index()
                    df_values.columns = [
                        "data_value",
                        "frequency_number",
                    ]  # type:ignore
                    df_values["column_name"] = col
                    if df_values.shape[0] < threshold:
                        delete_from_data_values(
                            db_engine_metadata,
                            server_name,
                            catalog_name,
                            schema_name,
                            table_name,
                            col,
                        )
                        dfs_.append(df_values)

                df_data_values = pd.concat(dfs_)
            cursor.close()
            conn_source.close()

            data_ = []
            for i, r in df_data_values.iterrows():  # type:ignore
                data_.append(
                    (
                        server_name,
                        catalog_name,
                        schema_name,
                        table_name,
                        r["column_name"],
                        str(r["data_value"]),
                        int(r["frequency_number"]),
                    )
                )

            insert_many_into_data_values(db_engine_metadata, data_)
        else:
            pbar1 = tqdm(column_rows, leave=False)
            for column_row in pbar1:
                column_name, ordinal_position, data_type = column_row
                pbar1.set_description(f"Data values - {table_name}.{column_name}")
                num_uniques = get_num_distinct_values(
                    db_engine_metadata,
                    server_name,
                    catalog_name,
                    schema_name,
                    table_name,
                    column_name,
                )
                if num_uniques > threshold or num_uniques < 0:
                    # logger.info(
                    #     "{}.{}.{} has {} unique values, more than the threshold {}".format(
                    #         table_name, column_name, value, num_uniques, threshold
                    #     )
                    # )
                    continue
                if check_if_data_value_exists(
                    db_engine_metadata,
                    server_name,
                    catalog_name,
                    schema_name,
                    table_name,
                    column_name,
                ):
                    if overwrite:
                        delete_from_data_values(
                            db_engine_metadata,
                            server_name,
                            catalog_name,
                            schema_name,
                            table_name,
                            column_name,
                        )
                    else:
                        continue
                data_value_rows = get_frequency(
                    db_engine_source, schema_name, table_name, column_name
                )

                data = []
                for i, data_value in enumerate(data_value_rows):
                    value, num_rows = data_value
                    if len(str(value)) > MAX_LENGTH_VALUES:
                        continue
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
                insert_many_into_data_values(db_engine_metadata, data)
    return


def insert_or_update_dates(
    db_engine_source, db_engine_metadata, overwrite=False, min_n_rows: int = 0
):
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
        rows = cursor.fetchall()
        rowcount = len(rows)
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

    table_rows = get_tables_from_metadata(
        db_engine_source, db_engine_metadata, min_n_rows=min_n_rows
    )
    pbar = tqdm(table_rows, desc="Dates")
    for table_row in pbar:
        server_name, catalog_name, schema_name, table_name, n_rows = table_row
        pbar.set_description(f"Dates - {table_name} ({n_rows:,} rows)")
        column_rows = get_date_columns(
            db_engine_metadata, server_name, catalog_name, schema_name, table_name
        )
        pbar1 = tqdm(column_rows, leave=False, desc="Dates")
        for column_row in pbar1:
            _, _, _, _, column_name = column_row
            pbar1.set_description("Dates - {}.{}".format(table_name, column_name))
            if check_if_dates_exists(
                db_engine_metadata,
                server_name,
                catalog_name,
                schema_name,
                table_name,
                column_name,
            ):
                if overwrite:
                    delete_from_dates(
                        db_engine_metadata,
                        server_name,
                        catalog_name,
                        schema_name,
                        table_name,
                        column_name,
                    )
                else:
                    continue
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
            # logger.info(
            #     "Inserting {} records into `dates` for {}.{}.{}".format(
            #         len(data), table_name, column_name, date_value
            #     )
            # )
            insert_many_into_dates(db_engine_metadata, data)


def insert_or_update_stats(
    db_engine_source: str,
    db_engine_metadata: str,
    overwrite: bool = False,
    with_percentiles: bool = False,
    min_n_rows: int = 0,
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
        cursor.execute(
            query, (server_name, catalog_name, schema_name, table_name, column_name)
        )
        rows = cursor.fetchall()
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

    def cast_to_float(value):
        if value is None:
            return None
        else:
            return float(value)

    table_rows = get_tables_from_metadata(
        db_engine_source, db_engine_metadata, min_n_rows=min_n_rows
    )
    pbar = tqdm(table_rows, desc="Stats")
    for table_row in pbar:
        server_name, catalog_name, schema_name, table_name, n_rows = table_row
        pbar.set_description(f"Stats - {table_name} ({n_rows:,} rows)")
        column_rows = get_numeric_columns(
            db_engine_metadata, server_name, catalog_name, schema_name, table_name
        )
        data = []
        if with_percentiles:
            percentiles = []
        pbar1 = tqdm(column_rows, leave=False, desc="Stats")
        for column_row in pbar1:
            _, _, _, _, column_name = column_row
            pbar1.set_description("Stats - {}.{}".format(table_name, column_name))
            if check_if_stats_exists(
                db_engine_metadata,
                server_name,
                catalog_name,
                schema_name,
                table_name,
                column_name,
            ):
                if overwrite:
                    delete_from_stats(
                        db_engine_metadata,
                        server_name,
                        catalog_name,
                        schema_name,
                        table_name,
                        column_name,
                    )
                else:
                    continue
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
                        cast_to_float(p01),
                        cast_to_float(p025),
                        cast_to_float(p05),
                        cast_to_float(p10),
                        cast_to_float(q2),
                        cast_to_float(q3),
                        cast_to_float(q4),
                        cast_to_float(p90),
                        cast_to_float(p95),
                        cast_to_float(p975),
                        cast_to_float(p99),
                        cast_to_float(iqr),
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
                    cast_to_float(avg_),
                    cast_to_float(stdev_),
                    cast_to_float(var_),
                    cast_to_float(sum_),
                    cast_to_float(max_),
                    cast_to_float(min_),
                    cast_to_float(range_),
                )
            )
        if len(data) > 0:
            # logger.info(
            #     "Inserting {} records into `stats` for {}.{}".format(
            #         len(data), schema_name, table_name
            #     )
            # )
            insert_many_into_stats(db_engine_metadata, data)
            if with_percentiles:
                update_percentiles(db_engine_metadata, percentiles)
        else:
            # logger.info(
            #     "{} numeric columns found in {}.{}".format(
            #         len(data), schema_name, table_name
            #     )
            # )
            pass


def get_columns(db_engine_source: str):
    conn_string = _utils.get_db_connection_string(db_engine_source)
    conn = _utils.get_db_connection(conn_string)

    query = SQL_SCRIPTS["columns"][conn_string["db_engine"]]
    cursor = conn.cursor()
    cursor.execute(
        query, (conn_string["host"], conn_string["catalog"], conn_string["schema"])
    )
    rows = cursor.fetchall()

    # logger.info(
    #     "{} columns from {}.{}.{}".format(
    #         len(rows),
    #         conn_string["host"],
    #         conn_string["catalog"],
    #         conn_string["schema"],
    #     )
    # )

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
        f"{cursor.rowcount} columns from {conn_string['host']}.{conn_string['catalog']}.{conn_string['schema']}"
    )

    cursor.close()
    conn.close()

    return rows
