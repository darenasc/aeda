import logging
from datetime import datetime

import typer

from aeda import sql as _sql
from aeda import utils as _utils
from aeda.config import EXPLORATION_LEVELS, SUPPORTED_DB_ENGINES

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
def explore(
    source: str,
    metadata: str,
    level: str = "server",
    overwrite: bool = True,
    threshold: int = 5_000,
    min_n_rows: int = 0,
    percentiles: bool = False,
):
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

    _utils.check_database_connections(conn_string_source, conn_string_metadata)

    start_time = datetime.now()

    if level == "server":
        # _sql.insert_or_update_columns(
        #     db_engine_source, db_engine_metadata, overwrite=overwrite
        # )
        # _sql.insert_or_update_tables(
        #     db_engine_source, db_engine_metadata, overwrite=overwrite
        # )
        # _sql.insert_or_update_uniques(
        #     db_engine_source,
        #     db_engine_metadata,
        #     overwrite=overwrite,
        #     min_n_rows=min_n_rows,
        # )
        # _sql.insert_or_update_data_values(
        #     db_engine_source,
        #     db_engine_metadata,
        #     overwrite=overwrite,
        #     threshold=threshold,
        #     min_n_rows=min_n_rows,
        # )
        _sql.insert_or_update_dates(
            db_engine_source,
            db_engine_metadata,
            overwrite=overwrite,
            min_n_rows=min_n_rows,
        )
        _sql.insert_or_update_stats(
            db_engine_source,
            db_engine_metadata,
            with_percentiles=percentiles,
            min_n_rows=min_n_rows,
        )

    logger.info(f"Profiled in: {datetime.now() - start_time}")
    logger.info("Done!")

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
