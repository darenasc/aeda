from configparser import ConfigParser
from pathlib import Path
from typing import Union

import pymysql
import psycopg2
import sqlite3

AEDA_DIR = Path(__file__).parent.absolute().parent
DB_CONNECTIONS = AEDA_DIR / "connection_strings" / "databases.ini"


def get_db_config():
    config = ConfigParser()
    config.read(DB_CONNECTIONS)
    return config

def get_db_connection_string(db_conf: str) -> dict:
    parser = ConfigParser()
    filename = DB_CONNECTIONS
    parser.read(filename)

    db = {}
    if parser.has_section(db_conf):
        params = parser.items(db_conf)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(
            "Section {0} not found in the {1} file".format(db_conf, filename)
        )

    return db

def get_connection_parameters(db_conf: str) -> Union[str, str, str, str]:
    connection_string = get_db_connection_string(db_conf)
    db_engine = connection_string["db_engine"]
    server_name = connection_string["host"]
    catalog_name = connection_string["catalog"]
    schema_name = connection_string["schema"]
    return db_engine, server_name, catalog_name, schema_name


def get_db_connection(conn_string):
    conn = None
    if conn_string["db_engine"] == "postgres":
        try:
            conn = psycopg2.connect(
                host=conn_string["host"],
                user=conn_string["user"],
                password=conn_string["password"],
                dbname=conn_string["catalog"],
                port=int(conn_string["port"]),
            )
        except Exception:
            raise
    elif conn_string["db_engine"] == "mysql":
        try:
            conn = pymysql.connect(
                host=conn_string["host"],
                user=conn_string["user"],
                password=conn_string["password"],
                database=conn_string["schema"],
                port=int(conn_string["port"]),
            )
        except Exception:
            raise
    return conn
