# AEDA stands for Automated Exploratory Data Analysis

![](https://img.shields.io/github/license/darenasc/aeda)
![](https://img.shields.io/github/last-commit/darenasc/aeda)
![](https://img.shields.io/github/stars/darenasc/aeda?style=social)

**AEDA** will automatically profile any [supported database](documentation/supported_databases.md) 
using reading access priviledges. The results of the profiling will be stored 
in a second [supported database](documentation/supported_databases.md) with write 
priviledges.

Profiling a database means **metadata extraction** from all the tables of a 
given database and storing this information into a second metadata database 
that can be used to query information about the source database. The metadata 
database is a **data catalog**.

**AEDA** generates SQL queries to be executed in the source database and 
store the results in a metadata database. The structure of the metadata 
database can be found in this [document](documentation/sql_code.md).

## Usage

### 1. Clone and install the repository

Download or clone this repository and install the dependencies.

```bash
git clone https://github.com/darenasc/aeda.git
cd aeda
```

If you don't have [pipenv](https://pipenv.pypa.io/en/latest/) installed, you 
can install it with:

```bash
pip install pipenv
```

Then, you can install the dependencies with:

```bash
pipenv install Pipfile
```

### 2. Create a database connection file

`aeda` requires a `databases.ini` file in the `src/aeda/connection_strings/` 
folder to store the connections to databases. You can rename the 
[`databases.ini.template`](src/aeda/connection_strings/databases_template.ini) 
file that is included with the repo and then add your connections there. 
The `databases.ini` file is not syncronised with the repo.

### 3. Add database connections

The database connections have the following format. 

```ini
# databases.ini
[my-source-database]
db_engine = <A-SUPPORTED-DB-ENGINE>
host = <IP-OR-HOSTNAME-SOURCE-DATABASE>
schema = <SCHEMA-SOURCE-DATABASE>
catalog = <CATALOG-SOURCE-DATABASE>
user = <SOURCE-USER>
password = <SOURCE-PASSWORD>
port = <SOURCE-PORT>

[my-metadata-database]
db_engine = <A-SUPPORTED-DB-ENGINE>
host = <IP-OR-HOSTNAME-METADATA-DATABASE>
schema = <SCHEMA-METADATA-DATABASE>
catalog = <CATALOG-METADATA-DATABASE>
user = <METADATA-USER>
password = <METADATA-PASSWORD>
port = <METADATA-PORT>
metadata_database = yes # yes or no optional parameter

[<SQLITE3-REFERENCE-NAME>]
db_engine = sqlite3
schema = <SQLITE3-DATABASE-NAME>
folder = <PATH/TO/THE/FOLDER/OF/THE/SQLITE3/DATABASE>
metadata_database = yes
```

A **`[connection-name]`** in square brackets that is used by `aeda` to identify 
what database you want to use. In the example above there are two database 
connections `[my-source-database]` and `[my-metadata-database]`.

`[my-source-database]` is the database that we want to profile, we need reading 
priviledges to that database.
`[my-metadata-database]` is the database where we will store the metadata from 
`[my-source-database]`. The database defined by `[my-metadata-database]` 
requires writing priviledges.

You can check the [SQL Code](docs/sql_code.md) documentation file to learn 
about the database structure of the metadata database and what metadata is 
extracted from the profiled sources.

> Note: Do not use quotes in the `databases.ini` file and remove '<' and '>' chars.

The `metadata_database` parameter is optional. It is used by the streamlit app to 
show the connection and presents the `metadata_database` as a dropdown list.

The supported database engines, to fill the `db_engine` property in the `databases.ini` 
file are:

* [x] `sqlite3`
* [x] `mysql`
* [x] `postgres`
* [x] `mssqlserver`
* [x] `mariadb`
* [x] `snowflake`
* [x] `aurora`
* [x] `saphana`
* [x] `saphana_odbc`

#### 3.1 Create the metadata database

You could create a SQLite3 local database or create metadata databases using 
`MySQL`, `PostgreSQL`, or `MS SQL Server`. Using the following commands from 
the terminal in the `src/aeda` folder:

```shell
python aeda_.py create_db sqlite3 --section <YOUR-SQLITE3-DATABASE>  # Creates a sqlite3 database, or
python aeda_.py create_db mysql --section <my-metadata-database>
```

A connection definition for a SQLite3 database has only three properties:

```CONF
[<SQLITE3-REFERENCE-NAME>]
db_engine = sqlite3
schema = <SQLITE3-DATABASE-NAME>
folder = <PATH/TO/THE/FOLDER/OF/THE/SQLITE3/DATABASE>
```

#### 3.2. Check connections

You can check what connections are available using `list-connections` that will list the connections available. You can use the name in the `section` column to refer to that specific connection.

```bash
python aeda_.py list-connections
```

#### 3.3 Test the connections

To test the connections to the databases you have created, you can use the 
following command:

```bash
cd src/aeda
python aeda_.py test-connection my-source-database # or
python aeda_.py test-connections my-source-database my-metadata-database # list of connection names from `databases.ini` separate by spaces
```

Where `my-source-database` and `my-metadata-database` are the names of the 
connection definitions in the `databases.ini` configuration file.

This should print the following:

```bash
[ OK ]  Connection to the ****.****.**** source tested successfully...
[ OK ]  Connection to the ****.****.**** source tested successfully...
```

#### 3.3 List the connections

Once you add your connections, you can check them using the `list-connections`.

```bash
cd src/aeda
python aeda_.py list-connections
```

### 4. Exploring the source database

To explore a database you need to run the following command from the terminal 
in the `src/aeda` folder:

```bash
cd src/aeda
python aeda_.py explore --source my-source-database --metadata my-metadata-database
```

Where `my-source-database` and `my-metadata-database` are the names of the 
connection definitions in the `databases.ini` configuration file.

### 5. Relax and wait for the results.

The process has 6 stages and will print `Done!` when the process is finished.

The phases of the profiling are six:

1. It's going to get all the columns from the metadata.
2. It's going to compute number of columns and number of rows per table.
3. It's going to compute the number of unique values and number of `NULL` values per column.
4. It's going to compute the data value frequency per column.
5. It's going to compute the monthly frequency of the timestamp or date type columns.
6. It's going to compute statistics of the numeric type columns.

The tables are processed by number of rows, so from step 3 it's going to process the tables with less rows first.

### 6. Visualising the results

You can query the resulting database or use a minimalistic user interface 
develped with [streamlit](https://streamlit.io) from the `src/aeda/streamlit` 
folder. It will publish the report in the port `5000` of your `localhost`.

```bash
cd src/aeda/streamlit
streamlit run aeda_app.py
```

## Feedback is appreciated!

- Any questions or feedback? just create an [issue](https://github.com/darenasc/aeda/issues)
- There are issues with `help wanted` to test commercial databases.
