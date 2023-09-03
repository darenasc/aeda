import sqlite3
from configparser import ConfigParser
from pathlib import Path
from typing import Union


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
            import psycopg2

            conn = psycopg2.connect(
                host=conn_string["host"],
                user=conn_string["user"],
                password=conn_string["password"],
                dbname=conn_string["catalog"],
                port=int(conn_string["port"]),
            )
        except Exception:
            raise
    elif conn_string["db_engine"] in ["mysql"]:
        try:
            import pymysql

            conn = pymysql.connect(
                host=conn_string["host"],
                user=conn_string["user"],
                password=conn_string["password"],
                database=conn_string["schema"],
                port=int(conn_string["port"]),
            )
        except Exception:
            raise
    elif conn_string["db_engine"] == "mssqlserver":
        try:
            import pyodbc

            conn = pyodbc.connect(
                DRIVER="{ODBC Driver 17 for SQL Server}",
                server=conn_string["host"],
                database=conn_string["catalog"],
                user=conn_string["user"],
                tds_version="7.4",
                password=conn_string["password"],
                port=conn_string["port"],
            )
        except Exception:
            raise
    elif conn_string["db_engine"] == "mariadb":
        try:
            import mariadb

            conn = mariadb.connect(
                user=conn_string["user"],
                password=conn_string["password"],
                host=conn_string["host"],
                port=int(conn_string["port"]),
                database=conn_string["schema"],
            )
        except:
            raise
    elif conn_string["db_engine"] == "sqlite3":
        try:
            dbname = str(conn_string["schema"] + ".db")
            conn = sqlite3.connect(Path(conn_string["folder"]) / dbname)
        except:
            raise
    return conn
