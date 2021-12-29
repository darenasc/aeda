import logging

import typer

from config import (
    EXPLORATION_LEVELS,
    SUPPORTED_DB_ENGINES,
)
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

    _sql.create_database(section)

    return


@app.command()
def explore(source: str, metadata: str, level: str = "server"):
    """
    Parameters:
        db_engine (str):

        level (str): ['server', 'catalog', 'schema', 'table', 'view', 'query']
    """

    _utils.get_quote()

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

    _utils.check_database_connections(conn_string_source, conn_string_metadata)

    if level == "server":
        _sql.insert_or_update_columns(db_engine_source, db_engine_metadata)
        _sql.insert_or_update_tables(db_engine_source, db_engine_metadata)
        _sql.insert_or_update_uniques(db_engine_source, db_engine_metadata)
        _sql.insert_or_update_data_values(db_engine_source, db_engine_metadata)
        _sql.insert_or_update_dates(db_engine_source, db_engine_metadata)
        _sql.insert_or_update_stats(
            db_engine_source, db_engine_metadata, with_percentiles=True
        )

    _utils.get_quote()
    print("Done!")

    return


@app.command()
def test_connections(source: str, metadata: str):

    db_engine_source = source
    db_engine_metadata = metadata
    conn_string_source = _utils.get_db_connection_string(db_engine_source)
    conn_string_metadata = _utils.get_db_connection_string(db_engine_metadata)

    _utils.check_database_connections(conn_string_source, conn_string_metadata)

    return

def load():
    pass


def connect():
    pass


def create():
    pass


if __name__ == "__main__":
    app()
