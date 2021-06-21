# aeda

```
python aeda_.py create_db sqlite3
python aeda_.py create_db mysql --section mysql-metadata
python aeda_.py create_db mysql --section mysql-demo
```

## Connections

Connections are declared in `databases.ini`. 
[<REFERENCE-NAME>]
db_engine = <YOUR-DB-ENGINE>
host = <>
schema = <>
catalog = <>
user = <>
password = <>
port = <>
encoding = <>