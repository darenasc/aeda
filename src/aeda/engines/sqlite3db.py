import logging
import sqlite3
from pathlib import Path

from pymysql import cursors

# from config import SQL_CREATE_SCRIPTS, SQLITE3_DB_DIR

FORMAT = "%(asctime)-15s %(message)s"
logging.basicConfig(level=logging.INFO, format=FORMAT)
logger = logging.getLogger(__name__)

DBENGINE = "sqlite3"


def create_database(overwrite: bool = False):
    logger.info("Creating metadata database in SQLite3")

    if Path.is_file(SQLITE3_DB_DIR):
        logger.info("Metadata database in SQLite3 exists in {}".format(SQLITE3_DB_DIR))

    if overwrite:
        with open(SQL_CREATE_SCRIPTS[DBENGINE], "r") as f:
            sql_script = f.read()
        sql_scripts = sql_script.split(";")

        conn = sqlite3.connect(SQLITE3_DB_DIR)
        cur = conn.cursor()

        for script in sql_scripts:
            cur.execute(script)
            conn.commit()
        conn.close()

        logger.info("Metadata database created in SQLite3 in {}".format(SQLITE3_DB_DIR))
    else:
        logger.info("Include `--overwrite` to create the database again")
        return
    return


def get_db_connection():
    return sqlite3.connect(SQLITE3_DB_DIR)


def close_db_connection(conn):
    conn.close()
    return
