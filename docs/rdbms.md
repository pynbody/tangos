Working with different database systems
=======================================

Tangos is built on sqlalchemy, which means that it is in principle possible to use any database system supported by sqlalchemy. However, different database systems have different features and limitations of which it is worth being aware.

The tangos tests are run with SQLite, mySQL and postgresql. Other databases, while supported by sqlalchemy, have not been directly tested. The following contain some notes on using these different systems.

SQLite
------

SQLite is the default database. It is simple in the sense that it keeps your entire database within a single file which can easily be transferred to different systems. Additionally, the SQLite driver is included with Python and so it's quick to get started.

There are two major, related drawbacks to SQLite. The first is that to access it one must copy over
the file, and there is no automated way to keep files synchronised between hosts. (Probably the best
thing to do is to write to the database only on one cluster, and then `rsync` it to the relevant
analysis machines.) The second is that it is not really designed for parallel writes, so when tangos
is writing to the database it must manually try to synchronise writes between different workers.
Tangos does a pretty good job of this, but some network file systems can be slow at releasing file
locks that SQLite uses extensively. If you run into errors about 'database is locked', you have reached
the limit of how many tangos processes can safely write to SQLite simultaneously.

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


 You can now create new users that can access your mysql server with their own username and password.

 ```bash
 echo "create user 'my_new_user'@'%' identified by 'new_password';" | docker exec -i mysql-server mysql -pmy_secret_password
 ```

 Note that in MySQL the `%` acts as a wild card, so this command creates a new user
 logging in from any host.

 The new user would then connect to the database:

 ```bash
 export TANGOS_DB_CONNECTION=mysql+pymysql://my_new_user:new_password@localhost:3306/database_name
 ```

The database can be accessed remotely if allowed by any applicable firewalls, by replacing `localhost`
with the actual host like `fancy_computer.astro.fancy_school.edu`. Note, however, that
running a database server open to the world has security implications and may be disallowed by
relevant institutions. The simplest approach, rather than opening up firewalls, is to tunnel in.
For example, the server can be accessed as though it's running on `localhost` if the user
first ssh tunnels into `fancy_computer.astro.fancy_school.edu`:

 ```bash
 ssh -N -f -L localhost:3306:localhost:3306 my_username@fancy_computer.astro.fancy_school.edu
 ```

Note that new users will by default only be able to view a database. Granting
additional permissions should be done on a case-by-case basis. Only the root user can
do this by defualt. To give a user complete permission to edit an existing database:

 ```bash
 echo "grant all on database_name.* to 'new_user'@'%';" | docker exec -i mysql-server mysql -pmy_secret_password
 echo "flush privileges;" | docker exec -i mysql-server mysql -pmy_secret_password
 ```

 You (and whatever users you choose) can now use all the tangos tools as normal, and they will
 populate the MySQL/PostgreSQL database instead of a SQLite file.
