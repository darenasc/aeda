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