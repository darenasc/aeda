from configparser import ConfigParser
import logging
from typing import Union

import mariadb
import pymysql
import psycopg2
import pyodbc

from config import CONFIG_DB, SQL_SCRIPTS

FORMAT = "%(asctime)-15s %(message)s"
logging.basicConfig(level=logging.INFO, format=FORMAT)
logger = logging.getLogger(__name__)


def get_db_connection_string(db_conf: str) -> dict:
    parser = ConfigParser()
    filename = CONFIG_DB
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


def get_query(query_type: str, db_engine: str):
    query = SQL_SCRIPTS[query_type][db_engine]
    return query


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
            logger.error("Database connection error")
            raise
    elif conn_string["db_engine"] in ["mysql"]:
        try:
            conn = pymysql.connect(
                host=conn_string["host"],
                user=conn_string["user"],
                password=conn_string["password"],
                database=conn_string["schema"],
                port=int(conn_string["port"]),
            )
        except Exception:
            logger.error("Database connection error")
            raise
    elif conn_string["db_engine"] == "mssqlserver":
        try:
            conn = pyodbc.connect(
                DRIVER="{ODBC Driver 17 for SQL Server}",
                server=conn_string["host"],
                database=conn_string["catalog"],
                user=conn_string["user"],
                tds_version="7.4",
                password = conn_string["password"],
                # password="MyStrongPasswordForSQLServer#2019!",
                port=conn_string["port"],
            )
        except Exception:
            logger.error("Database connection error")
            raise
    elif conn_string["db_engine"] == "mariadb":
        try:
            conn = mariadb.connect(
                user=conn_string["user"],
                password=conn_string["password"],
                host=conn_string["host"],
                port=int(conn_string["port"]),
                database=conn_string["schema"],
            )
        except:
            logger.error("Database connection error")
            raise
    return conn
