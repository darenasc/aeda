from pathlib import Path

import pandas as pd
import streamlit as st

from apps import data_values_app, stats_app, dates_app
import config as _config


def data_values():
    st.subheader("Data Values")
    query_data_values = """select * from data_values;"""
    df_data_values = pd.read_sql(query_data_values, conn)
    df_data_values


def dates():
    st.subheader("Dates")
    query_dates = """select * from dates;"""
    df_dates = pd.read_sql(query_dates, conn)
    df_dates


def stats():
    st.subheader("Statistics")
    query_stats = """select * from stats;"""
    df_stats = pd.read_sql(query_stats, conn)
    df_stats




st.title("Automated Exploratory Data Analysis app")

config = _config.get_db_config()
config_metadata = []
for section in config.sections():
    if 'metadata_database' in config[section]:
        if config[section]['metadata_database'] == 'yes':
            config_metadata.append(section)
add_selectbox = st.sidebar.selectbox(
    "What database would you like to connect?", tuple(config_metadata))

if config[add_selectbox]["db_engine"] == "sqlite3":
    st.sidebar.write(
        """DB ENGINE: `{}`\n
Schema: `{}`""".format(
            config[add_selectbox]["db_engine"], config[add_selectbox]["schema"]
        )
    )
else:
    st.sidebar.write(
        """DB ENGINE: `{}`\n
Host: `{}`\n
Catalog: `{}`\n
Schema: `{}`""".format(
            config[add_selectbox]["db_engine"],
            config[add_selectbox]["host"],
            config[add_selectbox]["catalog"],
            config[add_selectbox]["schema"],
        )
    )


apps = {"Data values": data_values, "Dates": dates, "Stats": stats}
app = st.sidebar.selectbox("Choice your page: ", tuple(apps.keys()))

connection_string = _config.get_db_connection_string(add_selectbox)
conn = _config.get_db_connection(connection_string)

query_metrics = """select count(distinct server_name) as n 
                        , count(distinct table_catalog) as databases
                        , sum(n_tables) as tables
                        , sum(n_rows) as rows
                        , sum(n_columns) as n_columns
                    from servers;"""
df_metrics = pd.read_sql(query_metrics, conn)
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Servers", int(df_metrics.n.values[0]))
col2.metric("Databases", int(df_metrics.databases.values[0]))
col3.metric("Tables", int(df_metrics.tables.values[0]))
col4.metric("Columns", int(df_metrics.n_columns.values[0]))
col5.metric("Records", "{:,}".format(int(df_metrics.rows.values[0])))


st.subheader("Servers")
query_servers = """select * from servers;"""
df_servers = pd.read_sql(query_servers, conn)
df_servers

st.subheader("Tables")
query_tables = """select * from tables;"""
df_tables = pd.read_sql(query_tables, conn)
df_tables

st.subheader("Columns")
query_uniques = """select * from uniques;"""
df_uniques = pd.read_sql(query_uniques, conn)
df_uniques

apps[app]()
