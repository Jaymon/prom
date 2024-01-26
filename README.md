# Prom

An opinionated asynchronous lightweight orm for PostgreSQL or SQLite.


## 1 Minute Getting Started with SQLite

First, install prom:

    $ pip install prom[sqlite]

Set an environment variable:

    $ export PROM_DSN=sqlite://:memory:

Start python:

    $ python

Create a prom Orm:

```python
>>> import prom
>>>
>>> class Foo(prom.Orm):
...     table_name = "foo_table_name"
...     bar = prom.Field(int)
...
>>>
```

Now go wild and create some `Foo` objects:

```python
>>> for x in range(10):
...     f = await Foo.create(bar=x)
...
>>>
```

Now query them:

```python
>>> f = await Foo.query.one()
>>> f.bar
0
>>> f.pk
1
>>>
>>> async for f in await Foo.query.in_bar([3, 4, 5]):
...     f.pk
...
3
4
5
>>>
```

Update them:

```python
>>> async for f in await Foo.query:
...     f.bar += 100
...     await f.save()
...
>>>
```

and get rid of them:

```python
>>> async for f in await Foo.query:
...     await f.delete()
...
>>>
```

Congratulations, you have now created, retrieved, updated, and deleted from your database.


-------------------------------------------------------------------------------

## Configuration

Prom can be automatically configured on import by setting the environment variable `PROM_DSN`.

The `PROM_DSN` should define a dsn url:

    <full.python.path.InterfaceClass>://<username>:<password>@<host>:<port>/<database>?<options=val&query=string>#<name>

The built-in interface classes don't need their full python paths, you can just use `sqlite` and `postgres`.

So to use the builtin Postgres interface on `testdb` database on host `localhost` with username `testuser` and password `testpw`:

    postgres://testuser:testpw@localhost/testdb

And to set it in your environment:

    export PROM_DSN=postgres://testuser:testpw@localhost/testdb

After you've set the environment variable, then you just need to import Prom in your code:

```python
import prom
```

and Prom will take care of parsing the dsn url(s) and creating the connection(s) automatically.



### Multiple db interfaces or connections

If you have multiple connections, you can actually set multiple environment variables:

    export PROM_DSN_1=postgres://testuser:testpw@localhost/testdb1#conn_1
    export PROM_DSN_2=sqlite://testuser:testpw@localhost/testdb2#conn_2

It's easy to have one set of `prom.Orm` children use one connection and another set use a different connection, since the fragment part of a Prom dsn url sets the name:

```python
import prom

class Orm1(prom.Orm):
    connection_name = "conn_1"
  
class Orm2(prom.Orm):
    connection_name = "conn_2"
```

Now, any child class that extends `Orm1` will use `conn_1` and any child class that extends `Orm2` will use `conn_2`.


## Creating Models

Checkout the [README](https://github.com/Jaymon/prom/blob/master/docs/README_MODEL.md) to see how to define the db schema and create models your python code can use.


## Querying Rows

Checkout the [README](https://github.com/Jaymon/prom/blob/master/docs/README_QUERY.md) to see how to perform queries on the db.


## Versions

While Prom will most likely work on other versions, Prom is tested to work on 3.10.


## Installation


### Postgres

If you want to use Prom with Postgres:

    $ apt-get install libpq-dev python-dev
    $ pip install prom[postgres]


### Prom

Prom installs using pip:

    $ pip install prom[sqlite]
    $ pip install prom[postgres]

and to install the latest and greatest:

    $ pip install --upgrade "git+https://github.com/Jaymon/prom#egg=prom"


### Using for the first time

Prom takes the approach that you don't want to be hassled with table installation while developing, so when it tries to do something and sees that the table doesn't yet exist, it will use your defined fields for your `prom.model.Orm` child and create a table for you, that way you don't have to remember to run a script or craft some custom db query to add your tables. Prom takes care of that for you automatically.

Likewise, if you add a field (and the field is not required) then prom will go ahead and add that field to your table so you don't have to bother with crafting `ALTER` queries while developing.

If you want to install the tables manually, you can create a script or something and use the Orm's `install()` method:

```python
await SomeOrm.install()
```

