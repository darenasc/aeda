from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

import aeda.streamlit.config as _config
from aeda.streamlit.apps import data_values_app, dates_app, stats_app


def data_values():
    st.subheader("Data Values")
    query_data_values = """select * from data_values;"""
    df_data_values = pd.read_sql(query_data_values, conn)
    df_data_values.sort_values("FREQUENCY_NUMBER", ascending=False, inplace=True)
    # df_data_values

    col1, col2, col3, col4 = st.columns(4)

    server_filter = col1.selectbox(
        "Select a server", df_data_values["SERVER_NAME"].unique()
    )
    catalog_filter = col2.selectbox(
        "Select a catalog",
        df_data_values[df_data_values["SERVER_NAME"] == server_filter][
            "TABLE_CATALOG"
        ].unique(),
    )
    schema_filter = col3.selectbox(
        "Select a schema",
        df_data_values[
            (df_data_values["SERVER_NAME"] == server_filter)
            & (df_data_values["TABLE_CATALOG"] == catalog_filter)
        ]["TABLE_SCHEMA"].unique(),
    )
    table_filter = col4.selectbox(
        "Select a table",
        df_data_values[
            (
                (df_data_values["SERVER_NAME"] == server_filter)
                & (df_data_values["TABLE_CATALOG"] == catalog_filter)
                & (df_data_values["TABLE_SCHEMA"] == schema_filter)
            )
        ]["TABLE_NAME"].unique(),
    )
    column_filter = st.selectbox(
        "Select a column",
        df_data_values[
            (
                (df_data_values["SERVER_NAME"] == server_filter)
                & (df_data_values["TABLE_CATALOG"] == catalog_filter)
                & (df_data_values["TABLE_SCHEMA"] == schema_filter)
                & (df_data_values["TABLE_NAME"] == table_filter)
            )
        ]["COLUMN_NAME"].unique(),
    )
    df_data_values = df_data_values[
        (df_data_values["SERVER_NAME"] == server_filter)
        & (df_data_values["TABLE_CATALOG"] == catalog_filter)
        & (df_data_values["TABLE_SCHEMA"] == schema_filter)
        & (df_data_values["TABLE_NAME"] == table_filter)
        & (df_data_values["COLUMN_NAME"] == column_filter)
    ]
    # .sort_values("FREQUENCY_NUMBER", ascending=False)
    # df_data_values[["COLUMN_NAME", "DATA_VALUE", "FREQUENCY_NUMBER"]]
    # df_data_values.columns

    bars = (
        alt.Chart(df_data_values)
        .mark_bar()
        .encode(
            x=alt.X("FREQUENCY_NUMBER:Q", title="Frequency number"),
            y=alt.Y(
                "DATA_VALUE:N",
                sort=alt.EncodingSortField(
                    field="FREQUENCY_NUMBER", op="sum", order="descending"
                ),
                title="Data value",
            ),
            tooltip=["DATA_VALUE", "FREQUENCY_NUMBER"],
        )
        # .interactive()
    )

    text = bars.mark_text(
        align="left",
        baseline="middle",
        dx=3,  # Nudges text to right so it doesn't appear on top of the bar
    ).encode(text="FREQUENCY_NUMBER:Q")

    chart = (bars + text).properties().interactive()

    st.altair_chart(chart, use_container_width=True)
    # st.altair_chart(bars, use_container_width=True)


def dates():
    st.subheader("Dates")
    query_dates = """select * from dates;"""
    df_dates = pd.read_sql(query_dates, conn)
    # df_dates

    col1, col2, col3, col4 = st.columns(4)
    server_filter = col1.selectbox(
        "Select a server", df_dates["SERVER_NAME"].unique(), key="server_filter_dates"
    )
    catalog_filter = col2.selectbox(
        "Select a catalog",
        df_dates[df_dates["SERVER_NAME"] == server_filter]["TABLE_CATALOG"].unique(),
        key="catalog_filter_dates",
    )
    schema_filter = col3.selectbox(
        "Select a schema",
        df_dates[
            (df_dates["SERVER_NAME"] == server_filter)
            & (df_dates["TABLE_CATALOG"] == catalog_filter)
        ]["TABLE_SCHEMA"].unique(),
        key="schema_filter_dates",
    )
    table_filter = col4.selectbox(
        "Select a table",
        df_dates[
            (
                (df_dates["SERVER_NAME"] == server_filter)
                & (df_dates["TABLE_CATALOG"] == catalog_filter)
                & (df_dates["TABLE_SCHEMA"] == schema_filter)
            )
        ]["TABLE_NAME"].unique(),
        key="table_filter_dates",
    )
    column_filter = st.selectbox(
        "Select a column",
        df_dates[
            (
                (df_dates["SERVER_NAME"] == server_filter)
                & (df_dates["TABLE_CATALOG"] == catalog_filter)
                & (df_dates["TABLE_SCHEMA"] == schema_filter)
                & (df_dates["TABLE_NAME"] == table_filter)
            )
        ]["COLUMN_NAME"].unique(),
    )
    df_dates = df_dates[
        (df_dates["SERVER_NAME"] == server_filter)
        & (df_dates["TABLE_CATALOG"] == catalog_filter)
        & (df_dates["TABLE_SCHEMA"] == schema_filter)
        & (df_dates["TABLE_NAME"] == table_filter)
        & (df_dates["COLUMN_NAME"] == column_filter)
    ]

    bars = (
        alt.Chart(df_dates[df_dates["DATA_VALUE"] != "NULL"])
        .mark_line()
        .encode(
            y=alt.Y("FREQUENCY_NUMBER:Q", title="Data points"),
            x=alt.X(
                "DATA_VALUE:N",
                sort=alt.EncodingSortField(
                    field="DATA_VALUE", op="mean", order="ascending"
                ),
                title="Date",
            ),
            tooltip=["DATA_VALUE", "FREQUENCY_NUMBER"],
        )
        # .interactive()
    )

    text = bars.mark_text(
        align="left",
        baseline="middle",
        dx=3,  # Nudges text to right so it doesn't appear on top of the bar
    ).encode(text="FREQUENCY_NUMBER:Q")

    chart = (bars + text).properties().interactive()

    st.altair_chart(chart, use_container_width=True)

    col5, col6, col7, col8 = st.columns(4)
    col5.write("Min date: {}".format(pd.to_datetime(df_dates["DATA_VALUE"]).min()))
    col6.write("Max date: {}".format(pd.to_datetime(df_dates["DATA_VALUE"]).max()))
    col7.write("Number of unique dates: {}".format(df_dates["DATA_VALUE"].nunique()))
    # range of dates
    col8.write(
        "Range of dates: {}".format(
            pd.to_datetime(pd.to_datetime(df_dates["DATA_VALUE"])).max()
            - pd.to_datetime(pd.to_datetime(df_dates["DATA_VALUE"])).min()
        )
    )

    st.table(
        df_dates[
            (df_dates["SERVER_NAME"] == server_filter)
            & (df_dates["TABLE_CATALOG"] == catalog_filter)
            & (df_dates["TABLE_SCHEMA"] == schema_filter)
            & (df_dates["TABLE_NAME"] == table_filter)
            & (df_dates["COLUMN_NAME"] == column_filter)
        ][["DATA_VALUE", "FREQUENCY_NUMBER"]]
    )


def stats():
    st.subheader("Statistics")
    query_stats = """select * from stats;"""
    df_stats = pd.read_sql(query_stats, conn)
    st.table(
        df_stats[
            [
                "TABLE_NAME",
                "COLUMN_NAME",
                "AVG",
                "STDEV",
                "VAR",
                "SUM",
                "MIN",
                "MAX",
                "RANGE",
            ]
        ]
    )


st.title("Automated Exploratory Data Analysis app")

config = _config.get_db_config()
config_metadata = []
for section in config.sections():
    if "metadata_database" in config[section]:
        if config[section]["metadata_database"] == "yes":
            config_metadata.append(section)
add_selectbox = st.sidebar.selectbox(
    "What database would you like to connect?", tuple(config_metadata)
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


# apps = {"Data values": data_values, "Dates": dates, "Stats": stats}
# app = st.sidebar.selectbox("Choice your page: ", tuple(apps.keys()))

connection_string = _config.get_db_connection_string(add_selectbox)
conn = _config.get_db_connection(connection_string)

query_metrics = """select count(distinct server_name) as n 
                        , count(distinct table_catalog) as databases
                        , sum(n_tables) as tables
                        , sum(n_rows) as rows
                        , sum(n_columns) as n_columns
                    from servers;"""
df_metrics = pd.read_sql(query_metrics, conn)
col1, col2, col3 = st.columns(3)
col4, col5 = st.columns(2)
col1.metric("Servers", int(df_metrics.n.values[0]))
col2.metric("Databases", int(df_metrics.databases.values[0]))
col3.metric("Tables", int(df_metrics.tables.values[0]))
col4.metric("Columns", int(df_metrics.n_columns.values[0]))
col5.metric("Records", "{:,}".format(int(df_metrics.rows.values[0])))


st.subheader("Servers")
query_servers = """select * from servers;"""
df_servers = pd.read_sql(query_servers, conn)
st.table(df_servers)

st.subheader("Tables")
query_tables = """select * from tables;"""
df_tables = pd.read_sql(query_tables, conn)
# df_tables

# scatter plot of tables by number of columns and rows
c = (
    alt.Chart(df_tables)
    .mark_circle()
    .encode(
        x=alt.X("N_ROWS", title="Number of rows"),
        y=alt.Y("N_COLUMNS", title="Number of columns"),
        color="TABLE_SCHEMA",
        tooltip=[
            "SERVER_NAME",
            "TABLE_CATALOG",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "N_ROWS",
            "N_COLUMNS",
        ],
    )
    .interactive()
)
st.altair_chart(c, use_container_width=True)

st.subheader("Columns")
query_uniques = """select * from uniques;"""
df_uniques = pd.read_sql(query_uniques, conn)
# df_uniques

# merge tables and uniques
df_uniques = df_uniques.merge(
    df_tables, on=["SERVER_NAME", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME"]
)
# calculate percentage of nulls
df_uniques["NULL_VALUES_PERCENTAGE"] = (
    df_uniques["NULL_VALUES"] / df_uniques["N_ROWS"] * 100
).round(2)

unique_tables = df_uniques.sort_values(by="NULL_VALUES_PERCENTAGE", ascending=False)[
    "TABLE_NAME"
].unique()
unique_tables_with_nulls = (
    df_uniques[df_uniques["NULL_VALUES_PERCENTAGE"] >= 0.2]
    .sort_values(by="NULL_VALUES_PERCENTAGE", ascending=False)["TABLE_NAME"]
    .unique()
)
option = st.multiselect("Select a table", unique_tables, unique_tables_with_nulls)
# df_uniques[df_uniques["TABLE_NAME"].isin(option)]

# plot of uniques by column and number of uniques and nulls filtered by table
c = (
    alt.Chart(df_uniques[df_uniques["TABLE_NAME"].isin(option)])
    .mark_circle()
    .encode(
        x=alt.X("COLUMN_NAME", sort="-y", title="Column"),
        y=alt.Y("NULL_VALUES_PERCENTAGE", title="Nulls (%)"),
        color="TABLE_NAME",
        tooltip=[
            "TABLE_NAME",
            "COLUMN_NAME",
            "DISTINCT_VALUES",
            "NULL_VALUES",
            "N_ROWS",
            "NULL_VALUES_PERCENTAGE",
        ],
    )
    .interactive()
)
st.altair_chart(c, use_container_width=True)

tab1, tab2, tab3 = st.tabs(["Data Values", "Dates", "Stats"])
with tab1:
    data_values()
with tab2:
    dates()
with tab3:
    stats()

# apps[app]()
