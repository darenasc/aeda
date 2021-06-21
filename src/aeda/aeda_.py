import logging

import typer

from config import (
    AEDA_DIR,
    EXPLORATION_LEVELS,
    SQL_CREATE_SCRIPTS,
    SQL_SCRIPTS,
    SUPPORTED_DB_ENGINES,
)
from controller import explore_server, explore_table
from engines import mysql, sqlite3db
import utils as _utils

FORMAT = "%(asctime)-15s %(message)s"
logging.basicConfig(level=logging.INFO, format=FORMAT)
logger = logging.getLogger(__name__)

app = typer.Typer()


@app.command("create_db")
def create_database(db_engine: str, section: str = None, overwrite: bool = False):
    """
    Parameters:
        db_engine (str): Class of database engine to be used.

        db_conf_section: Contains the information to connect to the database of the `db_engine` type.
    """
    assert db_engine in SUPPORTED_DB_ENGINES, "{} is not supported.".format(db_engine)

    if db_engine == "sqlite3":
        sqlite3db.create_database(overwrite=overwrite)
    elif db_engine == "mysql":
        mysql.create_database(section)
    else:
        pass


def get_columns(db_engine_source: str):
    conn_string_source = _utils.get_db_connection_string(db_engine_source)
    if conn_string_source["db_engine"] == "mysql":
        column_rows = mysql.get_columns(db_engine_source)
    else:
        pass
    return column_rows


def get_tables(db_engine_source: str, db_engine_metadata: str):
    conn_string_metadata = _utils.get_db_connection_string(db_engine_metadata)
    if conn_string_metadata["db_engine"] == "mysql":
        table_rows = mysql.get_tables(db_engine_source, db_engine_metadata)
    else:
        pass
    return table_rows


def insert_or_update_columns(db_engine_metadata: str, column_rows):
    conn_string_metadata = _utils.get_db_connection_string(db_engine_metadata)
    if conn_string_metadata["db_engine"] == "mysql":
        mysql.insert_or_update_columns(db_engine_metadata, column_rows)
    else:
        pass
    return


def insert_or_update_tables(db_engine_source: str, db_engine_metadata: str, table_rows):
    conn_string_metadata = _utils.get_db_connection_string(db_engine_metadata)
    if conn_string_metadata["db_engine"] == "mysql":
        mysql.insert_or_update_tables(db_engine_source, db_engine_metadata, table_rows)
    else:
        pass
    return


@app.command()
def explore(db_engine_source: str, db_engine_metadata: str, level: str = "server"):
    """
    Parameters:
        db_engine (str):

        level (str): ['server', 'catalog', 'schema', 'table', 'view', 'query']
    """
    conn_string_source = _utils.get_db_connection_string(db_engine_source)
    conn_string_metadata = _utils.get_db_connection_string(db_engine_metadata)

    assert (
        conn_string_source["db_engine"] in SUPPORTED_DB_ENGINES
    ), "{} is not supported.".format(conn_string_source["db_engine"])
    assert (
        conn_string_metadata["db_engine"] in SUPPORTED_DB_ENGINES
    ), "{} is not supported.".format(conn_string_metadata["dn_engine"])
    assert level in EXPLORATION_LEVELS, "{} is not supported.".format(level)

    if level == "server":
        column_rows = get_columns(db_engine_source)
        insert_or_update_columns(db_engine_metadata, column_rows)

        table_rows = get_tables(db_engine_source, db_engine_metadata)
        insert_or_update_tables(db_engine_source, db_engine_metadata, table_rows)

    return


def load():
    pass


def connect():
    pass


def create():
    pass


if __name__ == "__main__":
    app()
