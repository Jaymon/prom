# Command Line Interface

Prom exposes a `prom` command line interface.


## Commands

### generate

```
$ prom generate --help
usage: prom generate [-h] [--stream STREAM]
                            [table_names [table_names ...]]

This will print out valid prom python code for given tables that already exist
in a database. This is really handy when you want to bootstrap an existing
database to work with prom and don't want to manually create Orm objects for
the tables you want to use, let `generate` do it for you

positional arguments:
  table_names           the table(s) to generate a prom.Orm for (default:
                        None)

optional arguments:
  -h, --help            show this help message and exit
  --stream STREAM, --out-file STREAM, -o STREAM
                        Write to a file path, default stdout (default: )
```


#### Example

Create some tables:

```
$ psql db
db=> CREATE TABLE bar1 (_id BIGSERIAL PRIMARY KEY, foo INT, che TEXT NOT NULL);
CREATE TABLE
Time: 11.135 ms
db=> CREATE TABLE bar2 (_id BIGSERIAL PRIMARY KEY, foo INT, che TEXT NOT NULL);
CREATE TABLE
Time: 7.480 ms
db=> CREATE TABLE bar3 (_id BIGSERIAL PRIMARY KEY,
db(>   foo INT,
db(>   che TEXT NOT NULL,
db(>   bar1_id BIGINT REFERENCES bar1 (_id) ON UPDATE CASCADE ON DELETE CASCADE,
db(>   bar2_id BIGINT REFERENCES bar2 (_id) ON UPDATE CASCADE ON DELETE SET NULL
db(> );
CREATE TABLE
Time: 9.947 ms
```

Now generate prom definitions for your new tables:

```
$ prom generate bar1 bar2 bar3
from prom import Orm, Field

class Bar1(Orm):
    table_name = 'bar1'

    _id = Field(long, True, pk=True)
    foo = Field(int, False)
    che = Field(str, True)
    _updated = None
    _created = None


class Bar2(Orm):
    table_name = 'bar2'

    _id = Field(long, True, pk=True)
    foo = Field(int, False)
    che = Field(str, True)
    _updated = None
    _created = None


class Bar3(Orm):
    table_name = 'bar3'

    _id = Field(long, True, pk=True)
    bar2_id = Field(Bar2, False)
    foo = Field(int, False)
    bar1_id = Field(Bar1, False)
    che = Field(str, True)
    _updated = None
    _created = None
```

