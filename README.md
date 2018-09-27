# Prom

An opinionated lightweight orm for PostgreSQL or SQLite.

Prom has been used in both single threaded and multi-threaded environments, including environments using Greenthreads.


## 1 Minute Getting Started with SQLite

First, install prom:

    $ pip install prom

Set an environment variable:

    $ export PROM_DSN=prom.interface.sqlite.SQLite://:memory:

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
...     f = Foo.create(bar=x)
...
>>>
```

Now query them:

```python
>>> f = Foo.query.first()
>>> f.bar
0
>>> f.pk
1
>>>
>>> for f in Foo.query.in_bar([2, 3, 4]):
...     f.pk
...
3
4
5
>>>
```

Update them:

```python
>>> for f in Foo.query:
...     f.bar += 100
...     f.save()
...
>>>
```

and get rid of them:

```python
>>> for f in Foo.query:
...     f.delete()
...
>>>
```

Congratulations, you have now created, retrieved, updated, and deleted from your database.


-------------------------------------------------------------------------------

## Example -- Create a User class

Here is how you would define a new Orm class:

```python
# app.models (app/models.py)
from prom import Orm, Field, Index

class User(Orm):

    table_name = "user_table_name"

    username = Field(str, True, unique=True), # string field (required) with a unique index

    password = Field(str, True), # string field (required)

    email = Field(str), # string field (not required)

    index_email = Index('email') # set a normal index on email field
```

You can specify the connection using a prom dsn url:

    <full.python.path.InterfaceClass>://<username>:<password>@<host>:<port>/<database>?<options=val&query=string>#<name>

So to use the builtin Postgres interface on `testdb` database on host `localhost` with username `testuser` and password `testpw`:

    prom.interface.postgres.PostgreSQL://testuser:testpw@localhost/testdb

To use our new User class:

```python
# testprom.py
import prom
from app.models import User

prom.configure("prom.interface.postgres.PostgreSQL://testuser:testpw@localhost/testdb")

# create a user
u = User(username='foo', password='awesome_and_secure_pw_hash', email='foo@bar.com')
u.save()

# query for our new user
u = User.query.is_username('foo').get_one()
print u.username # foo

# get the user again via the primary key:
u2 = User.query.get_pk(u.pk)
print u.username # foo

# let's add a bunch more users:
for x in range(10):
    username = "foo{}".format(x)
    ut = User(username=username, password="...", email="{}@bar.com".format(username))
    ut.save()

# now let's iterate through all our new users:
for u in User.query.get():
    print u.username
```


## Environment Configuration

Prom can be automatically configured on import by setting the environment variable `PROM_DSN`:

    export PROM_DSN=prom.interface.postgres.PostgreSQL://testuser:testpw@localhost/testdb

If you have multiple connections, you can actually set multiple environment variables:

    export PROM_DSN_1=prom.interface.postgres.PostgreSQL://testuser:testpw@localhost/testdb1#conn_1
    export PROM_DSN_2=prom.interface.postgres.PostgreSQL://testuser:testpw@localhost/testdb2#conn_2

After you've set the environment variable, then you just need to import Prom in your code:

```python
import prom
```

and Prom will take care of parsing the dsn url(s) and creating the connection(s) automatically.


## The Query class

You can access the query, or table, instance for each `prom.Orm` child you create by calling its `.query` class property:

```python
print User.query # prom.Query
```

Through the power of magic, everytime you call this property, a new `prom.Query` instance will be created.


### Customize the Query class

You can also extend the default `prom.Query` class and let your `prom.Orm` child know about it

```python
# app.models (app/models.py)

class DemoQuery(prom.Query):
    def get_by_foo(self, *foos):
        """get all demos with matching foos, ordered by last updated first"""
        return self.in_foo(*foos).desc_updated().get()

class DemoOrm(prom.Orm):
    query_class = DemoQuery


DemoOrm.query.get_by_foo(1, 2, 3) # this now works
```

Notice the `query_class` class property on the `DemoOrm` class. Now every instance of `DemoOrm` (or child that derives from it) will forever use `DemoQuery`.


### Using the Query class

You should check the actual code for the query class in `prom.query.Query` for all the methods you can use to create your queries, Prom allows you to set up the query using psuedo method names in the form:

    command_fieldname(field_value)

So, if you wanted to select on the `foo` fields, you could do:

```python
query.is_foo(5)
```

or, if you have the name in the field as a string:

    command_field(fieldname, field_value)

so, we could also select on `foo` this way:

```python
query.is_field('foo', 5)
```

The different WHERE commands:

  * `in` -- `in_field(fieldname, field_vals)` -- do a sql `fieldname IN (field_val1, ...)` query
  * `nin` -- `nin_field(fieldname, field_vals)` -- do a sql `fieldname NOT IN (field_val1, ...)` query
  * `is` -- `is_field(fieldname, field_val)` -- do a sql `fieldname = field_val` query
  * `not` -- `not_field(fieldname, field_val)` -- do a sql `fieldname != field_val` query
  * `gt` -- `gt_field(fieldname, field_val)` -- do a sql `fieldname > field_val` query
  * `gte` -- `gte_field(fieldname, field_val)` -- do a sql `fieldname >= field_val` query
  * `lt` -- `lt_field(fieldname, field_val)` -- do a sql `fieldname < field_val` query
  * `lte` -- `lte_field(fieldname, field_val)` -- do a sql `fieldname <= field_val` query

The different ORDER BY commands:

  * `asc` -- `asc_field(fieldname)` -- do a sql `ORDER BY fieldname ASC` query
  * `desc` -- `desc_field(fieldname)` -- do a sql `ORDER BY fieldname DESC` query

You can also sort by a list of values:

```python
foos = [3, 5, 2, 1]

rows = query.select_foo().in_foo(foos).asc_foo(foos).values()
print rows # [3, 5, 2, 1]
```

And you can also set limit and page:

```python
query.get(10, 1) # get 10 results for page 1 (offset 0)
query.get(10, 2) # get 10 results for page 2 (offset 10)
```

They can be chained together:

```python
# SELECT * from table_name WHERE foo=10 AND bar='value 2' ORDER BY che DESC LIMIT 5
query.is_foo(10).is_bar("value 2").desc_che().get(5)
```

You can also write your own queries by hand:

```python
query.raw("SELECT * FROM table_name WHERE foo = %s", [foo_val])
```

The `prom.Query` has a couple helpful query methods to make grabbing rows easy:

  * get -- `get(limit=None, page=None)` -- run the select query.
  * get_one -- `get_one()` -- run the select query with a LIMIT 1.
  * value -- `value()` -- similar to `get_one()` but only returns the selected field(s)
  * values -- `values(limit=None, page=None)` -- return the selected fields as a tuple, not an Orm instance

    This is really handy for when you want to get all the ids as a list:

    ```python
    # get all the bar ids we want
    bar_ids = Bar.query.select_pk().values()

    # now pull out the Foo instances that correspond to the Bar ids
    foos = Foo.query.is_bar_id(bar_ids).get()
    ```

  * pk -- `pk()` -- return the selected primary key
  * pks -- `pks(limit=None, page=None)` -- return the selected primary keys
  * has -- `has()` -- return True if there is atleast one row in the db matching query
  * get_pk -- `get_pk(pk)` -- run the select query with a `WHERE _id = pk`
  * get_pks -- `get_pks([pk1, pk2,...])` -- run the select query with `WHERE _id IN (...)`
  * raw -- `raw(query_str, *query_args, **query_options)` -- run a raw query
  * all -- `all()` -- return an iterator that can move through every row in the db matching query
  * count -- `count()` -- return an integer of how many rows match the query

**NOTE**, Doing custom queries using `raw` would be the only way to do join queries.


#### Specialty Queries

If you have a date or datetime field, you can pass kwargs to [fine tune date queries](http://www.postgresql.org/docs/8.3/static/functions-datetime.html#FUNCTIONS-DATETIME-EXTRACT):

```python
import datetime

class Foo(prom.Orm):

    table_name = "foo_table"

    dt = prom.Field(datetime.datetime)

    index_dt = prom.Index('dt')

# get all the foos that have the 7th of every month
r = q.is_dt(day=7).all() # SELECT * FROM foo_table WHERE EXTRACT(DAY FROM dt) = 7

# get all the foos in 2013
r = q.is_dt(year=2013).all()
```

Hopefully you get the idea from the above code.


### The Iterator class

the `get` and `all` query methods return a `prom.query.Iterator` instance. This instance has a useful attribute `has_more` that will be true if there are more rows in the db that match the query.

Similar to the Query class, you can customize the Iterator class by setting the `iterator_class` class variable:

```python
class DemoIterator(prom.Iterator):
    pass

class DemoOrm(prom.Orm):
    iterator_class = DemoIterator
```


## Multiple db interfaces or connections

It's easy to have one set of `prom.Orm` children use one connection and another set use a different connection, the fragment part of a Prom dsn url sets the name:

```python
import prom
prom.configure("Interface://testuser:testpw@localhost/testdb#connection_1")
prom.configure("Interface://testuser:testpw@localhost/testdb#connection_2")

class Orm1(prom.Orm):
    connection_name = "connection_1"
  
class Orm2(prom.Orm):
    connection_name = "connection_2"
```

Now, any class that extends `Orm1` will use `connection_1` and any orm that extends `Orm2` will use `connection_2`.


## Schema class


### The Field class

You can create fields in your schema using the `Field` class, the field has a signature like this:

```python
Field(field_type, field_required, **field_options)
```

The `field_type` is the python type (eg, `str` or `int` or `datetime`) you want the field to be.

The `field_required` is a boolean, it is true if the field needs to have a value, false if it doesn't need to be in the db.

The `field_options` are any other settings for the fields, some possible values:

  * `size` -- the size of the field (for a `str` this would be the number of characters in the string)
  * `max_size` -- The max size of the field (for a `str`, the maximum number of characters, for an `int`, the biggest number you're expecting)
  * `min_size` -- The minimum size of the field (can only be used with a corresponding `max_size` value)
  * `unique` -- set to True if this field value should be unique among all the fields in the db.
  * `ignore_case` -- set to True if indexes on this field should ignore case


### Foreign Keys

You can have a field reference the primary key of another field:

```python
from prom import Orm, Field

class Orm1(Orm):
    table_name = "table_1"

    foo = Field(int)


class Orm2(Orm):
    table_name = "table_2"

    orm1_id=prom.Field(Orm1, True) # strong reference

    orm1_id_2=prom.Field(Orm1, False) # weak reference
```

Passing in an Orm class as the type of the field will create a foreign key reference to that Orm. If the field is required, then it will be a strong reference that deletes the row from `Orm2` if the row from `s1` is deleted, if the field is not required, then it is a weak reference, which will set the column to `NULL` in the db if the row from `Orm1` is deleted.


## Versions

While Prom will most likely work on other versions, these are the versions we are running it on (just for references):


### Python

    $ python --version
    Python 2.7.3


### Postgres

    $ psql --version
    psql (PostgreSQL) 9.3.6


## Installation


### Postgres

If you want to use Prom with Postgres, you need psycopg2:

    $ apt-get install libpq-dev python-dev
    $ pip install psycopg


### Green threads

If you want to use Prom with gevent, you'll need gevent and psycogreen:

    $ pip install gevent
    $ pip install psycogreen

These are the versions we're using:

    $ pip install "gevent==1.0.1"
    $ pip install "psycogreen==1.0"

Then you can setup Prom like this:

```python
import gevent.monkey
gevent.monkey.patch_all()

import prom.gevent
prom.gevent.patch_all()
```

Now you can use Prom in the same way you always have. If you would like to configure the threads and stuff, you can pass in some configuration options using the dsn, the three parameters are *async*, *pool_maxconn*, *pool_minconn*, and *pool_class*. The only one you'll really care about is *pool_maxconn* which sets how many connections should be created.

All the options will be automatically set when `prom.gevent.patch_all()` is called.


### Prom

Prom installs using pip:

    $ pip install prom

and to install the latest and greatest:

    $ pip install --upgrade git+https://github.com/Jaymon/prom#egg=prom


### Using for the first time

Prom takes the approach that you don't want to be hassled with table installation while developing, so when it tries to do something and sees that the table doesn't yet exist, it will use your defined fields for your `prom.Orm` child and create a table for you, that way you don't have to remember to run a script or craft some custom db query to add your tables, Prom takes care of that for you automatically. Likewise, if you add a field (and it's not required) then prom will go ahead and add that field to your table so you don't have to bother with crafting `ALTER` queries while developing.

If you want to install the tables manually, you can create a script or something and use the `install()` method:

    SomeOrm.install()

