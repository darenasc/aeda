# AEDA stands for Automated Exploratory Data Analysis

![](https://img.shields.io/github/license/darenasc/aeda)
![](https://img.shields.io/github/last-commit/darenasc/aeda)
![](https://img.shields.io/github/stars/darenasc/aeda?style=social)

**AEDA** is a library to automatically profile any [supported database](docs/supported_databases.md) 
using a reading access connection to the database. The results of the profiling 
will be stored to a second [supported database](docs/supported_databases.md) 
that you need write priviledges.

Profiling a database means **metadata extraction** from all the tables of a given 
database and storing this information into a second metadata database that can 
be used to query information about the source database. The metadata database 
is a **data catalog**.

**AEDA** generates SQL queries to be executed in the source database and 
store the results in a metadata database. The structure of the metadata database 
can be found in this [document](docs/sql_code.md).

## Usage

0. Download or clone this repository and install the dependencies.

    `pipenv install Pipfile`

1. Create a `databases.ini` file that can be a copy of 
[`databases_template.ini`](src/aeda/connection_strings/databases_template.ini) 
and just rename it. 

2. Add two database connection descriptions. One for the `source database` 
(the database you want to run the profiling, with reading priviledges) and one 
for the `metadata database`, with writing priviledges, where the resutls will be 
stored.

```CONF
[my-source-database]
db_engine = <A-SUPPORTED-DB-ENGINE>
host = <IP-OR-HOSTNAME-SOURCE-DATABASE>
database = <SOURCE-DATABASE-NAME>
user = <SOURCE-USER>
password = <SOURCE-PASSWORD>
port = <SOURCE-PORT>

[my-metadata-database]
db_engine = <A-SUPPORTED-DB-ENGINE>
host = <IP-OR-HOSTNAME-METADATA-DATABASE>
database = <METADATA-DATABASE-NAME>
user = <METADATA-USER>
password = <METADATA-PASSWORD>
port = <METADATA-PORT>
```

The supported database engines, to fill the `db_engine` property in the `databases.ini` 
file are:

* [x] `sqlite3`
* [x] `mysql`
* [x] `postgres`
* [x] `mssqlserver`
* [x] `mariadb`
* [x] `snowflake`

You could create a SQLite3 local database or create metadata databases using `MySQL`, 
`PostgreSQL`, or `MS SQL Server`. Using the following commands from the terminal 
in the `src/aeda` folder:

```
python aeda_.py create_db sqlite3 # Creates a sqlite3 database by default, or
python aeda_.py create_db mysql --section <YOUR-MYSQL-DATABASE>
```

A connection definition for a SQLite3 database has only three properties:

```CONF
[<SQLITE3-REFERENCE-NAME>]
db_engine = sqlite3
schema = <SQLITE3-DATABASE-NAME>
folder = <PATH/TO/THE/FOLDER/OF/THE/SQLITE3/DATABASE>
```

3. To explore a database you need to run the following command from the terminal 
in the `src/aeda` folder:

```
python aeda_.py explore my-source-database my-metadata-database
```

Where `my-source-database` and `my-metadata-database` are sections in the 
`databases.ini` configuration file.

4. Relax and wait for the results.

5. You can query the resulting database or use a minimalistic user interface 
develped with streamlit from the `src/aeda/streamlit` folder. It will publish the 
report in the port `5000` of your `localhost`.

```
streamlit run aeda_app.py
```


## Feedback is appreciated!

- Any questions or feedback? just create an [issue](https://github.com/darenasc/aeda/issues)
- There are issues with `help wanted` to test commercial databases.
