import logging
import typer

from config import AEDA_DIR, SQL_CREATE_SCRIPTS, SUPPORTED_DB_ENGINES
from engines import sqlite3db, mysql

FORMAT = "%(asctime)-15s %(message)s"
logging.basicConfig(level=logging.INFO, format=FORMAT)
logger = logging.getLogger(__name__)

app = typer.Typer()


@app.command("create_db")
def create_metadata_database(
    db_engine: str, section: str = None, overwrite: bool = False
):
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


@app.command()
def explore(db_engine: str, param: str):
    """
    Parameters:
        db_engine (str):
    - server
    - database
    - table
    - view
    - query
    """
    pass


def load():
    pass


def connect():
    pass


def create():
    pass


if __name__ == "__main__":
    app()
