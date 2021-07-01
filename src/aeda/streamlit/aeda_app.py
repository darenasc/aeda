from pathlib import Path

import pandas as pd
import streamlit as st

import config as _config


st.title("Automated Exploratory Data Analysis app")

config = _config.get_db_config()

add_selectbox = st.sidebar.selectbox(
    "What database would you like to connect?", tuple(config.sections())
)

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

connection_string = _config.get_db_connection_string(add_selectbox)
conn = _config.get_db_connection(connection_string)

st.subheader("Tables")
query_tables = """select * from tables;"""
df_tables = pd.read_sql(query_tables, conn)
df_tables

st.subheader("Columns")
query_uniques = """select * from uniques;"""
df_uniques = pd.read_sql(query_uniques, conn)
df_uniques

st.subheader("Data Values")
query_data_values = """select * from data_values;"""
df_data_values = pd.read_sql(query_data_values, conn)
df_data_values

st.subheader("Dates")
query_dates = """select * from dates;"""
df_dates = pd.read_sql(query_dates, conn)
df_dates

st.subheader("Statistics")
query_stats = """select * from stats;"""
df_stats = pd.read_sql(query_stats, conn)
df_stats
