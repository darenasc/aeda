import typer

from config import AEDA_DIR, SQL_CREATE_SCRIPTS, SUPPORTED_DB_ENGINES
from engines import sqlite3db

app = typer.Typer()


@app.command("create_db")
def create_metadata_database(dbengine: str):
    assert dbengine in SUPPORTED_DB_ENGINES, "{} is not supported.".format(dbengine)
    if dbengine == "sqlite3":
        sqlite3db.create_metadata_database()
    else:
        pass


if __name__ == "__main__":
    app()
