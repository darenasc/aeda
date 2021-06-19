import logging
import pymysql

from config import SQL_CREATE_SCRIPTS, SQLITE3_DB_DIR
import utils as _utils

FORMAT = "%(asctime)-15s %(message)s"
logging.basicConfig(level=logging.INFO, format=FORMAT)
logger = logging.getLogger(__name__)

DBENGINE = "mysql"


def create_database(section):
    logger.info("Creating database in MySQL")

    conn = get_db_connection(section)

    with open(SQL_CREATE_SCRIPTS[DBENGINE], "r") as f:
        sql_script = f.read()
        scripts = sql_script.split(";")
        scripts = [x for x in sql_script.split(";") if len(x.strip()) > 0]
    cursor = conn.cursor()

    for script in scripts:
        cursor.execute(script)
        conn.commit()
    cursor.close()

    close_db_connection(conn)

    logger.info("Database created in MySQL")

    return


def get_db_connection(section):
    conn_string = _utils.get_db_connection_string(section)
    conn = None
    logger.info(
        "Connecting to `{}.{}` ".format(conn_string["host"], conn_string["database"])
    )
    print(conn_string)
    try:
        conn = pymysql.connect(
            host=conn_string["host"],
            user=conn_string["user"],
            password=conn_string["password"],
            database=conn_string["database"],
            port=int(conn_string["port"]),
        )
        logger.info(
            "Connection established to `{}` database in `{}`".format(
                conn_string["database"], conn_string["host"]
            )
        )
    except Exception:
        logger.error("Database connection error")
        raise
    return conn


def close_db_connection(conn):
    conn.close()
    logger.info("MySQL database connection closed")
    return
