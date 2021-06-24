# aeda

To create test databases.
```
python aeda_.py create_db sqlite3
python aeda_.py create_db mysql --section mysql-metadata
python aeda_.py create_db mysql --section mysql-demo
```

To run the `aeda`:
```
python aeda_.py explore mysql-demo mysql-metadata
```

Where `mysql-demo` and `mysql-metadata` are sections in the `databases.ini` configuration file.

## Connections

Connections are declared in `databases.ini`. 
```
[<REFERENCE-NAME>]
db_engine = <DB-ENGINE>
host = <HOST>
schema = <SCHEMA>
catalog = <CATALOG>
user = <USER>
password = <PASSWORD>
port = <PORT>
encoding = <>
```