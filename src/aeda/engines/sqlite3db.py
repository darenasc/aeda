import logging
from os import close
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


conn = sqlite3.connect(
    "/Users/darenasc/Dropbox/Business Intelligence/Proyectos/aeda/aeda/sqlite3-dbs/metadata.db"
)
cursor = conn.cursor()
cursor.execute(
    "select * from dates WHERE SERVER_NAME = 'localhost' AND TABLE_CATALOG = 'BikeStores' AND TABLE_SCHEMA = 'sales' AND TABLE_NAME = 'orders' AND COLUMN_NAME = 'order_date';"
)
rows = cursor.fetchall()
for row in rows:
    print(row)
cursor.close()
conn.close()
