# Defining Model Schemas

### The Field class

You can create fields in your schema using the `prom.config.Field` class, the field has a signature like this:

```python
Field(field_type, field_required, **field_options)
```

The `field_type` is the python type you want the field to be. The supported field types:

* `str` - a standard string, if `max_size` is passed in this will map to a sql `VARCHAR`.
* `int` - an integer.
* `float` - a decimal number.
* `dict` - a dictionary that will be mapped to json in the db.
* `list` - an array that will be mapped to json in the db.
* `set` - a set that will be serialized in the db.
* `object` - any object that will be serialized in the db.
* `bool` - a True/False value.
* `bytearray` - a blob.
* `datetime` - a `datetime.datetime` value.
* `date` - a `datetime.date` value.

The `field_required` is a boolean, it is true if the field needs to have a value, false if the field can be **NULL** in the db.

The `field_options` are any other settings for the fields, some possible values:

  * `size` -- the size of the field (for a `str` this would be the number of characters in the string)
  * `max_size` -- The max size of the field (for a `str`, the maximum number of characters, for an `int`, the biggest number you're expecting)
  * `min_size` -- The minimum size of the field (can only be used with a corresponding `max_size` value)
  * `unique` -- set to True if this field value should be unique among all the fields in the db.
  * `ignore_case` -- set to True if indexes on this field should ignore case.


### Foreign Keys

You can have a field reference the primary key of another field. Passing in an Orm class as the field type will create a foreign key reference to that Orm. If the field is required, then it will be a strong reference, if it isn't required then it will be a weak reference.

Example:

```python
from prom import Orm, Field


class StrongOrm(Orm):
    table_name = "table_strong"


class WeakOrm(Orm):
    table_name = "table_weak"


class ForeignOrm(Orm):
    table_name = "table_foreign"

    strong_id = Field(StrongOrm, True) # strong reference

    weak_id = Field(WeakOrm, False) # weak reference
```


### Field Lifecycle

You can customize the field's lifecycle by using an embedded subclass:

```python
from prom import Field, Orm


class Child(Orm):

    foo = Field(int, True)
    
    class bar(Field):
        def fget(self, orm, v):
            """Ran whenever the orm.field is accessed"""
            return v
        
        def iget(self, orm, v):
            """Ran whenever the orm.field is pulled from db"""
            return v
            
        def fset(self, orm, v):
            """Ran whenever the orm.field is set"""
            return v
            
        def iset(self, orm, v):
            """Ran whenever the orm.field is inserted/updated in the db"""
            return v
            
        def fdel(self, orm, v):
            """Ran whenever del orm.field is called"""
            return v
            
        def fdefault(self, orm, v):
            """Ran whenever the field has no value"""
            return v
        
        def iquery(self, query, v):
            """Ran whenever the field is used in the Query class"""
            return v
            
        def jsonable(self, orm, v):
            """Ran whenever orm.jsonable() is called"""
            return v   
```

So, the `Child` class has 2 fields: `foo` and `bar`.


