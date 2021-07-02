# MySQL test database

From the terminal you run the following command to create a container with MySQL 8.0

```
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

```
docker cp src/aeda/sql_scripts/mysql/world.sql mysql-demo:/tmp \
&& docker exec -it mysql-demo sh -c "mysql -prootpassword < /tmp/world.sql"
```

To create the `metadata` database in the container you need to run the following command from the terminal.

```
docker cp src/aeda/sql_scripts/mysql/mysql.sql mysql-demo:/tmp \
&& docker exec -it mysql-demo sh -c "mysql -prootpassword < /tmp/mysql.sql"
```
# MariaDB database

```
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

# Postgres database

```
docker run \
    --name postgres-demo \
    -p 6604:5432 \
    --rm \
    -e POSTGRES_PASSWORD=demo \
    -e POSTGRES_USER=demo \
    -e POSTGRES_DB=world \
    -d postgres:12.4
```

```
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

# MS SQL Server

Documentation about the docker container available [here](https://docs.microsoft.com/en-us/sql/linux/quickstart-install-connect-docker)
```
docker run \
    -e 'ACCEPT_EULA=Y' \
    -e 'SA_PASSWORD=MyStrongPasswordForSQLServer#2019!' \
    -p 1433:1433 \
    --name sqlserver \
    --rm \
    -d \
    mcr.microsoft.com/mssql/server:2019-latest

docker exec -it sqlserver mkdir /var/opt/mssql/backup

docker cp src/aeda/sql_scripts/mssqlserver/BikeStores Sample Database - create objects.sql sqlserver:/var/opt/mssql/backup
docker cp src/aeda/sql_scripts/mssqlserver/BikeStores Sample Database - drop all objects.sql sqlserver:/var/opt/mssql/backup
docker cp src/aeda/sql_scripts/mssqlserver/BikeStores Sample Database - load data.sql sqlserver:/var/opt/mssql/backup
```