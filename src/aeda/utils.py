import logging
import sqlite3
from configparser import ConfigParser
from pathlib import Path
from typing import Union

import pandas as pd
from tabulate import tabulate
from termcolor import colored

from aeda.config import CONFIG_DB, SQL_SCRIPTS

FORMAT = "%(asctime)-15s %(message)s"
logging.basicConfig(level=logging.INFO, format=FORMAT)
logger = logging.getLogger(__name__)


def get_db_connection_string(db_conf: str, filename: Path = CONFIG_DB) -> dict:
    """get_db_connection_string(db_conf: str, filename: Path = CONFIG_DB) -> dict

    Args:
        db_conf (str): _description_
        filename (Path, optional): _description_. Defaults to CONFIG_DB.

    Returns:
        dict: _description_
    """
    parser = ConfigParser()
    # filename = CONFIG_DB
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


def get_connection_parameters(
    db_conf: str, filename: Path = CONFIG_DB
) -> tuple[str, str, str, str]:
    """get_connection_parameters(db_conf: str, filename: Path = CONFIG_DB) -> Union[str, str, str, str]

    Args:
        db_conf (str): _description_
        filename (Path, optional): _description_. Defaults to CONFIG_DB.

    Returns:
        Union[str, str, str, str]: _description_
    """
    connection_string = get_db_connection_string(db_conf, filename=filename)
    db_engine = connection_string["db_engine"]
    server_name = connection_string["host"]
    catalog_name = connection_string["catalog"]
    schema_name = connection_string["schema"]
    return db_engine, server_name, catalog_name, schema_name


def get_query(query_type: str, db_engine: str):
    query = SQL_SCRIPTS[query_type][db_engine]
    return query


def get_db_connection(conn_string: dict):
    conn = None
    if conn_string["db_engine"] == "postgres":
        try:
            # TODO Add profiling to schemas different than `public`
            import psycopg2

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
            import pymysql  # type: ignore

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
    elif conn_string["db_engine"] in ["aurora"]:
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
            logger.error("Database connection error")
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
            logger.error("Database connection error")
            raise
    elif conn_string["db_engine"] == "mariadb":
        try:
            import mariadb  # type: ignore

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
    elif conn_string["db_engine"] == "sqlite3":
        try:
            dbname = str(conn_string["schema"] + ".db")
            conn = sqlite3.connect(Path(conn_string["folder"]) / dbname)
        except:
            logger.error("Database connection error")
            raise
    elif conn_string["db_engine"] == "snowflake":
        try:
            import snowflake.connector

            logging.getLogger("snowflake.connector").setLevel(logging.WARNING)

            conn = snowflake.connector.connect(
                user=conn_string["user"],
                password=conn_string["password"],
                account=conn_string["account"],
                warehouse=conn_string["warehouse"],
                database=conn_string["catalog"],
                schema=conn_string["schema"],
                role=conn_string["role"],
            )
        except:
            logger.error("Database connection error")
            raise
    elif conn_string["db_engine"] == "saphana":
        try:
            from hdbcli import dbapi

            conn = dbapi.connect(
                user=conn_string["user"],
                password=conn_string["password"],
                address=conn_string["host"],
                port=int(conn_string["port"]),
            )
        except Exception as e:
            logger.error("Database connection error SAP")
            print(e)
            raise
    elif conn_string["db_engine"] == "saphana_odbc":
        try:
            import pyodbc

            conn = pyodbc.connect(
                f"DSN={conn_string['odbc_name']};UID={conn_string['user']};PWD={conn_string['password']}"
            )
        except Exception as e:
            logger.error("Database connection error")
            print(e)
            raise
    return conn


def check_database_connection(conn_string: dict):
    # conn = get_db_connection(conn_string)
    try:
        conn = get_db_connection(conn_string)
        cursor = conn.cursor()
        if conn_string["db_engine"] == "sqlite3":
            db_connection_name = f"{conn_string['schema']}.db"
            print(
                f"[ {colored('OK', 'green')} ]\t{colored(conn_string['db_engine'], 'green', attrs=['bold'])} connection to {colored(db_connection_name, 'green', attrs=['bold'])} established..."
            )
        else:
            db_connection_name = f"{conn_string['host']}.{conn_string['catalog']}.{conn_string['schema']}"
            print(
                f"[ {colored('OK', 'green')} ]\t{colored(conn_string['db_engine'], 'green', attrs=['bold'])} connection to {colored(db_connection_name, 'green', attrs=['bold'])} established..."
            )

        cursor.close()
        conn.close()
    except:
        print(
            f"[{colored('Error', 'red')}]\tCan't establish connection to the metadata database..."
        )
    return


def check_database_connections(conn_string_source, conn_string_metadata):
    check_database_connection(conn_string_source)
    check_database_connection(conn_string_metadata)
    return


def list_connections():
    """List all connections in the config file"""
    parser = ConfigParser()
    if Path.exists(CONFIG_DB):
        filename = CONFIG_DB
    else:
        raise FileNotFoundError(f'Config file not found in "{CONFIG_DB}"')

    parser.read(filename)

    dfs = []
    for section in parser.sections():
        columns = [key for key, value in parser.items(section)]
        values = [value for key, value in parser.items(section)]
        columns.append("section")
        values.append(section)
        df = pd.DataFrame([values], columns=columns)
        dfs.append(df)

    df_connections = (
        pd.concat(dfs, axis=0, ignore_index=True)
        .fillna("")
        .sort_values(["db_engine", "host", "schema", "catalog", "metadata_database"])
    )
    df_connections = df_connections[
        ["section", "db_engine"]
        + [x for x in df_connections.columns if x not in ("db_engine", "section")]
    ]
    columns = [
        column
        for column in df_connections.columns
        if column not in ["user", "password", "encoding"]
    ]
    print(
        tabulate(
            df_connections[columns], headers="keys", tablefmt="pretty", showindex=False
        )
    )
    return
