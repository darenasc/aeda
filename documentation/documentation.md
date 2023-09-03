# Databases using Docker

This documentation shows how to create different test databases using Docker.

## 1. MySQL test database

From the terminal you run the following command to create a container with MySQL 8.0

```docker
docker run \
    --name mysql-demo \
    -p 6603:3306 \
    --rm \
    -e MYSQL_USER=demo \
    -e MYSQL_PASSWORD=demo \
    -e MYSQL_ROOT_PASSWORD=rootpassword \
    -e MYSQL_DATABASE=metadata \
    -d mysql:8.0
```

Then you need to copy the test database and load it.

```bash
docker cp src/aeda/sql_scripts/mysql/world.sql mysql-demo:/tmp \
&& docker exec -it mysql-demo sh -c "mysql -prootpassword < /tmp/world.sql"
```

To create the `metadata` database in the container you need to run the following command from the terminal.

```bash
docker cp src/aeda/sql_scripts/mysql/mysql.sql mysql-demo:/tmp \
&& docker exec -it mysql-demo sh -c "mysql -prootpassword < /tmp/mysql.sql"
```
## 2. MariaDB database

```bash
docker run \
    -p 6605:3306 \
    --name mariadb \
    -e MARIADB_ROOT_PASSWORD=rootpassword \
    -e MARIADB_DATABASE=world \
    -e MARIADB_USER=demo \
    -e MARIADB_PASSWORD=demo \
    -d \
    --rm \
    mariadb:10.6
```

## 3. Postgres database

```bash
docker run \
    --name postgres-demo \
    -p 6604:5432 \
    --rm \
    -e POSTGRES_PASSWORD=demo \
    -e POSTGRES_USER=demo \
    -e POSTGRES_DB=world \
    -d postgres:12.4
```

```bash
docker exec -i \
    postgres-demo \
    psql -U demo \
        -d world \
        --quiet \
        < src/aeda/sql_scripts/postgres/world.sql

docker exec -i \
    postgres-demo \
    psql -U demo \
        -d world \
        --quiet \
        < src/aeda/sql_scripts/postgres/postgres.sql
```

## 4. MS SQL Server

Documentation about the docker container available [here](https://docs.microsoft.com/en-us/sql/linux/quickstart-install-connect-docker)

```bash
# Creating the container with SQL Server 2019
docker run \
    -e 'ACCEPT_EULA=Y' \
    -e 'SA_PASSWORD=MyStrongPasswordForSQLServer2022!' \
    -p 1433:1433 \
    --name sqlserver \
    --rm \
    -d \
    mcr.microsoft.com/mssql/server:2022-latest

docker run \
    --platform linux/amd64 \ # for macOS M1/M2 processors
    -e 'ACCEPT_EULA=Y' \
    -e 'SA_PASSWORD=MyStrongPasswordForSQLServer2022!' \
    -p 1433:1433 \
    --name sqlserver \
    --rm \
    -d \
    mcr.microsoft.com/mssql/server:2022-latest

# Copying the files to create the BikeStores database
docker cp src/aeda/sql_scripts/mssqlserver/BikeStores-drop-all-objects.sql sqlserver:/tmp
docker cp src/aeda/sql_scripts/mssqlserver/BikeStores-create-objects.sql sqlserver:/tmp
docker cp src/aeda/sql_scripts/mssqlserver/BikeStores-load-data.sql sqlserver:/tmp

# Executing the .sql files
docker exec -it sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P MyStrongPasswordForSQLServer2022! -i /tmp/BikeStores-drop-all-objects.sql
docker exec -it sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P MyStrongPasswordForSQLServer2022! -i /tmp/BikeStores-create-objects.sql
docker exec -it sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P MyStrongPasswordForSQLServer2022! -i /tmp/BikeStores-load-data.sql

# Copying and creating the metadata database
docker cp src/aeda/sql_scripts/mssqlserver/mssqlserver.sql sqlserver:/tmp
docker exec -it sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P MyStrongPasswordForSQLServer2022! -i /tmp/mssqlserver.sql
```

# Connections

Connections are declared in `databases.ini`.

```ini
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