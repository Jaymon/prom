# Querying

You can access the query, or table, instance for each `prom.model.Orm` child you create by calling its `.query` class property:

```python
print(Orm.query) # prom.query.Query
```

Everytime you call this property, a new `prom.query.Query` instance will be created.


## Customizing the queries

### The Query Class

You can also extend the default `prom.query.Query` class and let your `prom.model.Orm` child know about it

```python
import prom

class DemoQuery(prom.Query):
    def get_by_foo(self, *foos):
        """get all demos with matching foos, ordered by last updated first"""
        return self.in_foo(*foos).desc_updated().get()

class DemoOrm(prom.Orm):
    query_class = DemoQuery
    
    foo = prom.Field(int)


DemoOrm.query.get_by_foo(1, 2, 3) # this now works
```

Notice the `query_class` class property on the `DemoOrm` class. Now every instance of `DemoOrm` (or child that derives from it) will use `DemoQuery`.


### The Iterator Class

the `get` and `all` query methods return a `prom.query.Iterator` instance. This instance has a useful attribute `has_more` that will be true if there are more rows in the db that match the query.

Similar to the Query class, you can customize the Iterator class by setting the `iterator_class` class variable:

```python
class DemoIterator(prom.Iterator):
    pass

class DemoOrm(prom.Orm):
    iterator_class = DemoIterator
```


## Querying

You should check the actual code for the query class in `prom.query.Query` for all the methods you can use to create your queries, Prom allows you to set up the query using pseudo method names in the form:

    command_fieldname(field_value)

So, if you wanted to select on the `foo` fields, you could do:

```python
query.eq_foo(5)
```

or, if you have the name in the field as a string:

    command_field(fieldname, field_value)

so, we could also select on `foo` this way:

```python
query.eq_field('foo', 5)
```

### Selecting Fields

You can use the `select` method to grab certain fields:

```python
query.select("foo", "bar")
```


### Where Commands

The different WHERE commands:

  * `in` -- `in_field(fieldname, field_vals)` -- do a sql `fieldname IN (field_val1, ...)` query
  * `nin` -- `nin_field(fieldname, field_vals)` -- do a sql `fieldname NOT IN (field_val1, ...)` query
  * `eq` -- `eq_field(fieldname, field_val)` -- do a sql `fieldname = field_val` query
  * `ne` -- `ne_field(fieldname, field_val)` -- do a sql `fieldname != field_val` query
  * `gt` -- `gt_field(fieldname, field_val)` -- do a sql `fieldname > field_val` query
  * `gte` -- `gte_field(fieldname, field_val)` -- do a sql `fieldname >= field_val` query
  * `lt` -- `lt_field(fieldname, field_val)` -- do a sql `fieldname < field_val` query
  * `lte` -- `lte_field(fieldname, field_val)` -- do a sql `fieldname <= field_val` query
  * `between` -- `between_field(low, high)` -- do a sql `fieldname >= low AND fieldname <= high` query
  * `startswith` -- `startswith_field(fieldname, field_val)` -- do a sql `fieldname LIKE 'fieldname%'` query
  * `endswith` -- `endswith_field(fieldname, field_val)` -- do a sql `fieldname LIKE '%fieldname'` query
  * `contains` -- `contains_field(fieldname, field_val)` -- do a sql `fieldname LIKE '%fieldname%'` query


### Sorting Fields

The different ORDER BY commands:

  * `asc` -- `asc_field(fieldname)` -- do a sql `ORDER BY fieldname ASC` query
  * `desc` -- `desc_field(fieldname)` -- do a sql `ORDER BY fieldname DESC` query

You can also sort by a list of values:

```python
foos = [3, 5, 2, 1]

rows = query.select_foo().in_foo(foos).asc_foo(foos).values()
print rows # [3, 5, 2, 1]
```


### Bounding Queries

And you can also set limit and page:

```python
query.limit(10).offset(1) # LIMIT 10 OFFSET 1
query.limit(10).page(2) # get 10 results for page 2 (offset 10)
```

They can be chained together:

```python
# SELECT foo, che from table_name WHERE foo=10 AND bar='value 2' ORDER BY che DESC LIMIT 5
query.select("foo", "che").is_foo(10).is_bar("value 2").desc_che().limit(5).get()
```

You can also write your own queries by hand:

```python
query.raw("SELECT * FROM table_name WHERE foo = %s AND bar = %s", [10, "value 2"])
```


### Executing the Query

The `prom.query.Query` has a couple helpful query methods to make grabbing rows easy:

  * get -- `get()` -- run the select query.
  * all -- `all()` -- alias for `get`.
  * one -- `one()` -- run the select query with a LIMIT 1.
  * count -- `count()` -- return an integer of how many rows match the query
  * value -- `value()` -- similar to `one()` but only returns the selected field(s)
  * values -- `values()` -- return the selected fields as a tuple, not an `prom.model.Orm` instance

    This is really handy for when you want to get all the ids as a list:

    ```python
    # get all the bar ids we want
    bar_ids = Bar.query.select_pk().values()

    # now pull out the Foo instances that correspond to the Bar ids
    foos = Foo.query.is_bar_id(bar_ids).get()
    ```

  * has -- `has()` -- return True if there is at least one row in the db matching query
  * get_pk -- `get_pk(pk)` -- run the select query with a `WHERE _id = pk`
  * get_pks -- `get_pks([pk1, pk2,...])` -- run the select query with `WHERE _id IN (...)`
  * raw -- `raw(query_str, *query_args, **query_options)` -- run a raw query

      ```python
      Foo.query.raw("SELECT * FROM {} WHERE bar = %s".format(Foo.schema), ["bar value"])
      ```

    **NOTE**, Doing custom queries using `raw` would be the only way to do join queries.


### Specialty Queries

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



