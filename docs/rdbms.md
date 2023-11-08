Working with different database systems
=======================================

Tangos is built on sqlalchemy, which means that it is in principle possible to use any database system supported by sqlalchemy. However, different database systems have different features and limitations of which it is worth being aware.

The tangos tests are run with SQLite, mySQL and postgresql. Other databases, while supported by sqlalchemy, have not been directly tested. The following contain some notes on using these different systems.

SQLite
------

SQLite is the default database. It is simple in the sense that it keeps your entire database within a single file which can easily be transferred to different systems. Additionally, the SQLite driver is included with Python and so it's quick to get started.

There are two major, related drawbacks to SQLite. The first is that the

PostgreSQL and MySQL
--------------------

PostgreSQL and MySQL are both server-based systems, and as such take a little more effort to set up and maintain. If one exposes PostgreSQL to the outside world, there are potential security implications. One can of course run it on a firewalled computer and manage access appropriately, but this takes some expertise of its own (that will not be covered here). The major advantage is that you can host your data in a single location and allow multiple users to connect.



MySQL
-----

MySQL is a server-based system, and as such takes a little more effort to set up. The advantage is that you can host your data in a single location and allow multiple users to connect. Additionally, it is able to cope much better with complex parallel writes than SQLite.

For most users, MySQL and PostgreSQL are

To try this out, if you have [docker](https://docker.com), you can run a test
MySQL server very easily:

```bash
docker pull mysql
docker run -d --name=mysql-server -p3306:3306 -e MYSQL_ROOT_PASSWORD=my_secret_password mysql
echo "create database database_name;" | docker exec -i mysql-server mysql -pmy_secret_password
```

Or, just as easily, you can get going with PostgreSQL:
```bash
docker pull postgres
docker run --name tangos-postgres -e POSTGRES_USER=tangos -e POSTGRES_PASSWORD=my_secret_password -e POSTGRES_DB=database_name -p 5432:5432 -d postgres
```

To be sure that python can connect to MySQL or PostgreSQL, install the appropriate modules:
```bash
pip install PyMySQL # for MySQL
pip install psycopg2-binary # for PostgreSQL
```

Tangos can now connect to your test MySQL server using the connection:
```bash
export TANGOS_DB_CONNECTION=mysql+pymysql://root:my_secret_password@localhost:3306/database_name
```
or for PostgreSQL:
```bash
export TANGOS_DB_CONNECTION=postgresql+psycopg2://tangos:my_secret_password@localhost/database_name
```

You can now use all the tangos tools as normal, and they will populate the MySQL/PostgreSQL database
instead of a SQLite file.
