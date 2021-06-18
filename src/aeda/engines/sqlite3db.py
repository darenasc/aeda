import sqlite3

from config import SQL_CREATE_SCRIPTS, SQLITE3_DB_DIR

DBENGINE = "sqlite3"


def create_metadata_database():
    with open(SQL_CREATE_SCRIPTS[DBENGINE], "r") as f:
        sql_script = f.read()
    sql_scripts = sql_script.split(";")

    conn = sqlite3.connect(SQLITE3_DB_DIR)
    cur = conn.cursor()

    for script in sql_scripts:
        cur.execute(script)
        conn.commit()
    conn.close()


def get_db_connection():
    return sqlite3.connect(SQLITE3_DB_DIR)
