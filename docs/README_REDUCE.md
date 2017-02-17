# Query.reduce

Quick and dirty data processing


## Example

Count how many primary keys in your table are even.

First, let's setup the connection and the table and add some rows:

```python
import os

os.environ["PROM_DSN"] = "prom.interface.sqlite.SQLite://:memory:"

import prom

class Foo(prom.Orm):
    table_name = "foo"
    bar = prom.Field(int, True)

# now let's add a million rows
count = 0
total_count = 1000000
while count < total_count:
    f = None
    with Foo.interface.transaction():
        for c in range(10000):
            f = Foo.create(bar=count)
            count += 1
            if count >= total_count:
                break

    print(f.pk)
```


Alright, now let's make sure we have 500,000 even primary keys:


```python
from __future__ import print_function


def target_map(o):
    """This is our mapping function, any non None value is passed to our reduce function"""
    if o.pk % 2 == 0:
        return o.pk


d = {"count": 0}
def target_reduce(pk):
    """any non None value from target_map is run through this function"""
    d["count"] += 1


Foo.query.reduce(target_map=target_map, target_reduce=target_reduce)
print(d["count"]) # 500000
```

And that's all there is to it, anything `target_map` returns that is non-None will be passed to reduce (ran on the main master process) for final processing.

## Benchmark

My Macbook Pro ran through the million rows created above using `Query.reduce` in 65 seconds, and using `query.all()` in about 97 seconds:

```
reduce - 65363.0 ms
regular - 97316.8 ms
```

