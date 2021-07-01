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
    -p 127.0.0.1:3306:3306 \
    --name some-mariadb \
    -e MARIADB_ROOT_PASSWORD=my-secret-pw \
    -d \
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


docker exec -it sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost \
   -U SA -P 'MyStrongPasswordForSQLServer#2019!' \
   -Q 'RESTORE FILELISTONLY FROM DISK = "/var/opt/mssql/backup/wwi.bak"' \
   | tr -s ' ' | cut -d ' ' -f 1-2

sudo docker exec -it sqlserver /opt/mssql-tools/bin/sqlcmd \
   -S localhost -U SA -P 'MyStrongPasswordForSQLServer#2019!' \
   -Q 'RESTORE DATABASE WideWorldImporters FROM DISK = "/var/opt/mssql/backup/wwi.bak" WITH MOVE "WWI_Primary" TO "/var/opt/mssql/data/WideWorldImporters.mdf", MOVE "WWI_UserData" TO "/var/opt/mssql/data/WideWorldImporters_userdata.ndf", MOVE "WWI_Log" TO "/var/opt/mssql/data/WideWorldImporters.ldf", MOVE "WWI_InMemory_Data_1" TO "/var/opt/mssql/data/WideWorldImporters_InMemory_Data_1"'

docker cp src/aeda/sql_scripts/mssqlserver/Corrupt2008DemoFatalCorruption1.bak sqlserver:/var/opt/mssql/backup
docker cp src/aeda/sql_scripts/mssqlserver/Corrupt2008DemoFatalCorruption2.bak sqlserver:/var/opt/mssql/backup

docker exec -it sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost \
   -U SA -P 'MyStrongPasswordForSQLServer#2019!' \
   -Q 'RESTORE FILELISTONLY FROM DISK = "/var/opt/mssql/backup/Corrupt2008DemoFatalCorruption1.bak"' \
   | tr -s ' ' | cut -d ' ' -f 1-2

sudo docker exec -it sqlserver /opt/mssql-tools/bin/sqlcmd \
   -S localhost -U SA -P 'MyStrongPasswordForSQLServer#2019!' \
   -Q 'RESTORE DATABASE WideWorldImporters FROM DISK = "/var/opt/mssql/backup/wwi.bak" WITH MOVE "WWI_Primary" TO "/var/opt/mssql/data/WideWorldImporters.mdf", MOVE "WWI_UserData" TO "/var/opt/mssql/data/WideWorldImporters_userdata.ndf", MOVE "WWI_Log" TO "/var/opt/mssql/data/WideWorldImporters.ldf", MOVE "WWI_InMemory_Data_1" TO "/var/opt/mssql/data/WideWorldImporters_InMemory_Data_1"'
```