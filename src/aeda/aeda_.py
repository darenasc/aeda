import logging

import typer

from config import (
    AEDA_DIR,
    EXPLORATION_LEVELS,
    SQL_CREATE_SCRIPTS,
    SQL_SCRIPTS,
    SUPPORTED_DB_ENGINES,
)
from engines import mysql, sqlite3db, postgres
import sql as _sql
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


def insert_or_update_columns(db_engine_metadata: str, column_rows):
    conn_string_metadata = _utils.get_db_connection_string(db_engine_metadata)
    if conn_string_metadata["db_engine"] == "mysql":
        mysql.insert_or_update_columns(db_engine_metadata, column_rows)
    elif conn_string_metadata["db_engine"] == "postgres":
        postgres.insert_or_update_columns(db_engine_metadata, column_rows)
    else:
        pass
    return


def insert_or_update_tables(db_engine_source: str, db_engine_metadata: str):
    conn_string_metadata = _utils.get_db_connection_string(db_engine_metadata)
    if conn_string_metadata["db_engine"] == "mysql":
        mysql.insert_or_update_tables(db_engine_source, db_engine_metadata)
    elif conn_string_metadata["db_engine"] == "postgres":
        postgres.insert_or_update_tables(db_engine_source, db_engine_metadata)
    else:
        pass
    return


def insert_or_update_uniques(db_engine_source: str, db_engine_metadata: str):
    conn_string_metadata = _utils.get_db_connection_string(db_engine_metadata)
    if conn_string_metadata["db_engine"] == "mysql":
        mysql.insert_or_update_uniques(db_engine_source, db_engine_metadata)
    else:
        pass
    return


def insert_or_update_data_values(db_engine_source: str, db_engine_metadata: str):
    conn_string_metadata = _utils.get_db_connection_string(db_engine_metadata)
    if conn_string_metadata["db_engine"] == "mysql":
        mysql.insert_or_update_data_values(db_engine_source, db_engine_metadata)
    else:
        pass
    return


def insert_or_update_dates(db_engine_source: str, db_engine_metadata: str):
    conn_string_metadata = _utils.get_db_connection_string(db_engine_metadata)
    if conn_string_metadata["db_engine"] == "mysql":
        mysql.insert_or_update_dates(db_engine_source, db_engine_metadata)
    else:
        pass
    return


def insert_or_update_stats(
    db_engine_source: str, db_engine_metadata: str, with_percentiles: bool = False
):
    conn_string_metadata = _utils.get_db_connection_string(db_engine_metadata)
    if conn_string_metadata["db_engine"] == "mysql":
        mysql.insert_or_update_stats(
            db_engine_source, db_engine_metadata, with_percentiles
        )
    else:
        pass
    return


@app.command()
def explore(source: str, metadata: str, level: str = "server"):
    """
    Parameters:
        db_engine (str):

        level (str): ['server', 'catalog', 'schema', 'table', 'view', 'query']
    """
    db_engine_source = source
    db_engine_metadata = metadata
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
        _sql.insert_or_update_columns(db_engine_source, db_engine_metadata)
        _sql.insert_or_update_tables(db_engine_source, db_engine_metadata)
        _sql.insert_or_update_uniques(db_engine_source, db_engine_metadata)
        _sql.insert_or_update_data_values(db_engine_source, db_engine_metadata)
        _sql.insert_or_update_dates(db_engine_source, db_engine_metadata)
        _sql.insert_or_update_stats(
            db_engine_source, db_engine_metadata, with_percentiles=True
        )

    return


def load():
    pass


def connect():
    pass


def create():
    pass


if __name__ == "__main__":
    app()
