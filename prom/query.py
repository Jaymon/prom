"""
Classes and stuff that handle querying the interface for a passed in Orm class
"""
import copy
from collections import defaultdict
from collections.abc import (
    AsyncIterable,
    Callable,
    Generator,
    Iterable,
    Mapping,
)
import re
from typing import Self, ForwardRef, Any, Protocol

from datatypes import ListIterator

from .compat import *
from .utils import make_list, get_objects, make_dict


type OrmType = ForwardRef(".model.Orm")
type QueryType = ForwardRef("Query")
type SchemaType = ForwardRef(".config.Schema")
type InterfaceType = ForwardRef(".interface.base.InterfaceABC")
class FieldCallable(Protocol):
    def __call__(
        self,
        field_name: str,
        field_val: Any,
        **field_kwargs,
    ) -> QueryType: ...


class Iterator(ListIterator, AsyncIterable):
    """The main iterator for all query methods that return iterators

    This is returned from the `Query.get()` method, this is also
    the Iterator class that is set in `Orm.iterator_class`

    fields --
        filter: callback, an iterator filter, all yielded rows will be passed
            through this callback and skipped if filter(row) returns False

    :example:
        # iterate through all the primary keys of some orm
        async for pk in SomeOrm.select_pk().query.get():
            print pk
    """
    def __init__(self, query: QueryType):
        """create an iterator for a query

        :param cursor: object, the cursor object retrieved from the interface
        :param query: Query, the query instance that produced this iterator
        """
        self.query = query
        self._cursor = None
        self._cursor_exhausted = False

    async def load(self, **kwargs):
        """Load this instance with a cursor retrieved from `.query`

        :keyword **kwargs: Same as all the query methods, you can pass in
            keywords like `connection` to fine tune the connection
        """
        self._cursor = await self.query.cursor(**kwargs)
        self._cursor_exhausted = False
        self._field_names = self.query.fields_select.names()

        if self.query._filter:
            self.filter = self.query._filter

    async def has_more(self) -> bool:
        """Return true if there are more results for this query

        :returns: boolean, True if this query could've returned more results
        """
        ret = False

        if self._cursor is None:
            await self._load()

        if self.query.bounds.has_more():
            cursor = self._cursor
            # https://www.psycopg.org/docs/cursor.html#cursor.rowcount says
            # that future versions of the spec reserve the right to return None
            if cursor.rowcount == -1 or cursor.rowcount is None:
                try:
                    if await self[self.query.bounds.find_more_index()]:
                        ret = True

                except IndexError:
                    pass

            else:
                ret = self.query.bounds.limit_paginate == cursor.rowcount

        return ret

    async def close(self):
        """Close the cursor that's tied to this iterator"""
        if self._cursor is not None:
            await self._cursor.close()
            self._cursor_exhausted = True
            self._cursor = None

    async def tolist(self) -> list[OrmType|tuple|Any]:
        """Returns this iterator as a list

        This is just syntactic sugar around having to do:

            [r async for r in self]

        when a non asyncronous list is needed. It uses the same naming
        convention as array.tolist which also seems to be used by numpy and
        pandas also

        https://docs.python.org/3/library/array.html#array.array.tolist

        :returns: list, all the items in the iterator as a list
        """
        return [r async for r in self]

    def __len__(self):
        """Make sure no one thinks we can get count syncronously"""
        raise NotImplementedError()

    def __iter__(self):
        """Make sure no one thinks we can iterate through this syncronously"""
        raise NotImplementedError()

    def __await__(self) -> Generator[None, None, Self]:
        """Turns this iterator into a coroutine so it can be awaited without
        it actually doing anything"""
        if False: yield # turn method into generator
        return self # return value of awaited coroutine

    async def __aiter__(self) -> AsyncIterable[OrmType|tuple|Any]:
        """
        https://docs.python.org/3/reference/datamodel.html#object.__aiter__
        """
        if self._cursor_exhausted:
            raise ValueError("Cursor has been exhausted, rerun the query")

        if self._cursor is None:
            await self.load()

        cursor_i = 0
        cursor_limit = -1
        if self.query.bounds.has_more():
            cursor_limit = self.query.bounds.limit

        try:
            async for row in self._cursor:
                if cursor_limit > 0 and cursor_i >= cursor_limit:
                    break

                o = self.from_query(row)
                if self.filter(o):
                    yield o

                cursor_i += 1

        finally:
            await self.close()

    async def __getitem__(self, i) -> OrmType|Self|dict:
        """Get a specific row or a slice of the rows represented by this
        iterator

        Honestly, you should probably make specific queries instead of using
        this since this will make new queries and you won't have control over
        things like the connection and stuff

        :param i: int|slice, the index(es) you want
        :returns: Orm|dict|Iterator
        """
        q = self.query.copy()
        b = q.bounds

        if isinstance(i, slice):
            if i.step:
                raise IndexError("slice stepping is not supported")

            if i.start:
                b.offset = b.find_offset(i.start)

            if i.stop:
                b.limit = b.find_limit(i.stop)

            return type(self)(q)

        else:
            if i < 0:
                raise IndexError("Indexes cannot be negative")

            b.offset = b.find_offset(i)

            o = await q.one()
            if o is None:
                raise IndexError("Iterator index {} out of range".format(i))

            return o

    def __repr__(self) -> str:
        format_str = "[ ... {} ... ]"
        format_args = [self.__class__.__name__]

        orm_class = self.query.orm_class
        if orm_class:
            format_str = "[ ... {} on {} ... ]"
            format_args.append(orm_class.__name__)

        return format_str.format(*format_args)

    def filter(self, o: OrmType|tuple|Any) -> bool:
        """run o through the filter, if True then orm o should be included

        .. note:: The filter callback needs to account for non Orm instance
            values of `o`

        :param o: Orm|tuple[Any]|Any, usually an Orm instance but can also be
            a tuple or single value
        :returns: boolean, True if o should be filtered
        """
        return True

    def from_query(self, d: dict) -> OrmType|tuple|Any:
        """Prepare the raw dict d returned from the interface cursor to be
        returned by higher level objects, this will usually mean hydrating an
        Orm instance or stuff like that

        .. note:: This will call `Orm.from_query` and should be the only place
            that will call that method

        :param d: dict, the raw dict cursor result returned from the interface
        :returns: Orm|tuple[Any]|Any, usually an Orm instance populated with d
            but can also be a tuple if the query selected more than one field.
            If the query selected one field then just that value will be
            returned
        """
        r = None
        orm_class = self.query.orm_class
        if self._field_names:
            field_vals = []
            for field_name in self._field_names:
                fv = d[field_name]
                if orm_class:
                    field = orm_class.schema.fields[field_name]
                    fv = field.from_interface(None, fv)
                field_vals.append(fv)
            r = field_vals if len(self._field_names) > 1 else field_vals[0]

        else:
            if orm_class:
                r = orm_class.from_query(d)

            else:
                r = d

        return r


class QueryBounds(object):
    """Stores the bounds (eg, DESC, ASC) information for a Query. It has a lot
    of hooks to make setting limit and offset easier
    """
    @property
    def limit(self):
        l = self._limit
        return l if l else 0

    @limit.setter
    def limit(self, v):
        v = int(v)
        if v < 0:
            raise ValueError("Limit cannot be negative")
        self._limit = v

    @limit.deleter
    def limit(self):
        self._limit = None

    @property
    def limit_paginate(self):
        limit = 0 if self._limit is None else self._limit
        return limit + 1 if limit > 0 else 0

    @property
    def offset(self):
        offset = self._offset
        if offset is None:
            page = self.page
            #limit = self.limit
            limit = self._limit
            if not limit: limit = 1
            offset = (page - 1) * limit

        else:
            offset = offset if offset >= 0 else 0

        return offset

    @offset.setter
    def offset(self, v):
        v = int(v)
        if v < 0:
            raise ValueError("Offset cannot be negative")
        del self.page
        self._offset = v

    @offset.deleter
    def offset(self):
        self._offset = None

    @property
    def page(self):
        page = 0 if self._page is None else self._page
        return page if page >= 1 else 1

    @page.setter
    def page(self, v):
        v = int(v)
        if v < 0:
            raise ValueError("Page cannot be negative")
        del self.offset
        self._page = int(v)

    @page.deleter
    def page(self):
        self._page = None

    def __init__(self, limit=None, page=None, offset=None):
        self.paginate = False
        self._limit = limit
        self._offset = offset
        self._page = page

    def set(self, limit=None, page=None, offset=None):
        if limit is not None:
            self.limit = limit

        if page is not None:
            if offset:
                raise ValueError("Cannot pass in both offset and page")
            self.page = page

        if offset is not None:
            self.offset = offset

    def get(self):
        limit = self.limit_paginate if self.paginate else self.limit
        return (limit, self.offset)

    def __bool__(self):
        return self.limit > 0 or self.offset > 0

    def has(self):
        return bool(self)

    def has_limit(self):
        return self.limit > 0

    def has_more(self):
        """Returns True if the current bounds are set up to query one extra row
        in order to make pagination easier (to know if there should be a next
        link)

        :returns: boolean
        """
        return self.has_limit() and self.paginate

    def has_pages(self):
        return self.has_more()

    def is_paginated(self):
        return self.has_more()

    def __str__(self):
        return "limit: {}, offset: {}".format(self.limit, self.offset)

    def find_offset(self, i: int) -> int:
        """Given an index i, use the current offset and limit to find the
        correct offset i would be

        :param i: int, the index used to find the new offset
        :returns: int, the new offset
        """
        if i >= 0:
            offset = self.offset + i
            if self.has_limit():
                maximum_offset = self.limit + self.offset
                if offset > maximum_offset:
                    raise IndexError(
                        f"Offset {offset} larger than"
                        " maximum offset {maximum_offset}",
                    )

        else:
            limit = self.limit
            offset = limit + i

            if offset < 0:
                raise IndexError("Negative offset")

        return offset

    def find_limit(self, i: int) -> int:
        """Given an index i, use the current offset and limit to find the
        correct limit i would be"""
        if i >= 0:
            limit = i - self.offset

        else:
            limit = self.limit + i

            if limit < 0:
                raise IndexError("Negative Limit")

        return limit

    def find_more_index(self) -> int:
        """Find the index that can be used to decide if there are more
        results for these bounds"""
        return self.offset + self.limit


class QueryField(object):
    """Holds information for a field in the query"""
    @property
    def schema(self):
        return self.query.schema if self.query else None

    @property
    def schema_field(self):
        schema = self.schema
        if schema:
            return getattr(schema, self.name, None)

    def __init__(self, query, field_name, field_val=None, **kwargs):
        self.query = query
        self.operator = kwargs.pop("operator", None)
        self.is_list = kwargs.pop("is_list", False)
        self.direction = kwargs.pop("direction", None) # 1 = ASC, -1 = DESC
        self.increment = kwargs.pop("increment", False) # Query.incr_field
        self.raw = kwargs.pop("raw", False)
        self.clause = kwargs.pop("clause", "")
        self.function_name = kwargs.pop("function_name", "")
        self.inverse = kwargs.pop("inverse", False)
        self.or_clause = False
        self.kwargs = kwargs

        if self.raw:
            self.name = field_name
            self.value = field_val

        else:
            self.set_name(field_name)
            self.set_value(field_val)

    def set_name(self, field_name):
        field_name, function_name = self.parse(field_name, self.schema)

        self.name = field_name

        if function_name:
            self.function_name = function_name

        if self.function_name and self.in_select_clause():
            self.alias = field_name

    def set_value(self, field_val):
        # we set this here so things like .is_subquery work as expected in a
        # Field.to_query method body
        self.value = field_val

        if self.is_list and not self.is_subquery():
            if field_val:
                field_val = make_list(field_val)
                for i in range(len(field_val)):
                    field_val[i] = self.to_query_value(field_val[i])

        else:
            field_val = self.to_query_value(field_val)

        self.value = field_val

    def to_query_value(self, field_val: Any) -> Any:
        if sf := self.schema_field:
            field_val = self.schema_field.to_query_value(self, field_val)

            if self.is_subquery() and not field_val.fields_select:
                if sub_schema := field_val.schema:
                    orm_class = self.query.orm_class

                    field_val.select_ref(orm_class)

                    if not field_val.fields_select:
                        if ref_class := sf.ref_class:
                            field_val.select_dep(ref_class)

                if not field_val.fields_select:
                    raise ValueError(
                        "Subquery select fields could not be inferred",
                    )

        else:
            if self.schema:
                raise KeyError(
                    f"Schema {self.schema} had no field {self.name}",
                )

        return field_val

    def modify_query(self):
        if sf := self.schema_field:
            sf.modify_query(self)

    def parse(self, field_name, schema):
        function_name = ""
        if schema:
            function_name = ""
            m = re.match(r"^([^\(]+)\(([^\)]+)\)$", field_name)
            if m:
                function_name = m.group(1)
                field_name = m.group(2)

            try:
                field_name = schema.field_name(field_name)

            except AttributeError:
                field_name = schema.field_model_name(field_name)

        return field_name, function_name

    def in_clause(self, clause):
        """Check if clause corresponds to self.clause

        :param clause: str, the clause name (eg, where, select, sort, set)
        :returns: bool, True if the clauses match
        """
        return self.clause == clause

    def in_select_clause(self):
        return self.in_clause("select")

    def in_set_clause(self):
        """Return True if this field instance belongs to the fields_set
        clause/portion of the query"""
        return self.in_clause("set")

    def in_where_clause(self):
        """Return True if this field instance belongs to the fields_where
        clause/portion of the query"""
        return self.in_clause("where")

    def is_subquery(self):
        """Return True if this field's value is a subquery"""
        return isinstance(self.value, Query)


class QueryFields(list):
    """Holds all the QueryField instances for a given clause"""
    @property
    def fields(self):
        """Returns a dict of field_name: field_value"""
        ret = {}
        for f in self:
            ret[f.name] = f.value

        return ret

    def __init__(self):
        self.clear()

    def names(self):
        """Return all the field names in the order they were first seen"""
        ret = []
        seen = set()
        for f in self:
            if f.name not in seen:
                ret.append(f.name)
                seen.add(f.name)
        return ret

    def append(self, field):
        index = len(self)
        super().append(field)
        self.field_names[field.name].append(index)
        field.modify_query()

    def get_field(self, field_name):
        """Return the last field information for field_name

        This is handy for getting set information. If you need all the set
        values for field_name then you would just use
        self.field_names[field_name]

        :param field_name: str, the field name you're looking for
        :returns: QueryField|None
        """
        try:
            return self[self.field_names[field_name][-1]]

        except KeyError:
            return None

    def __contains__(self, field_name):
        return field_name in self.field_names

    def __setitem__(self, *args, **kwargs):
        raise NotImplementedError()
    __delitem__ = __setitem__

    def clear(self):
        self.field_names = defaultdict(list)
        self.options = {}
        super().clear()

    def todict(self):
        """Returns a dict of field_name: QueryField instance"""
        ret = {}
        for f in self:
            ret[f.name] = f

        return ret


class Query(AsyncIterable):
    """Handle standard query creation and allow interface querying

    This is the glue between Orm and Interface. For the most part you will
    never interact with the Interface directly but instead use Orm and Query,
    and you won't usually ever instantiate this class directly but instead use
    the Orm.query magic class property

    :example:
        q = Query(orm_class)
        q.select_bar().select_che().eq_foo(1).desc_bar().limit(10).page(2)
        q.get()
        # SELECT
        #   bar,
        #   che
        # FROM
        #   <orm_class.table_name>
        # WHERE
        #   foo = 1
        # ORDER BY bar DESC
        # LIMIT 10 OFFSET 20
    """
    field_class = QueryField
    fields_set_class = QueryFields
    fields_select_class = QueryFields
    fields_where_class = QueryFields
    fields_sort_class = QueryFields
    bounds_class = QueryBounds
    iterator_class = Iterator

    @property
    def interface(self) -> InterfaceType|None:
        return self.orm_class.interface if self.orm_class else None

    @property
    def schema(self) -> SchemaType|None:
        return self.orm_class.schema if self.orm_class else None

    @property
    def schemas(self) -> list[SchemaType]:
        """Find and return all the schemas that are needed for this query to
        complete successfully

        Another way to put this is all the schemas this query touches

        :returns: list, a list of Schema instances
        """
        schemas = []

        s = self.schema
        if s:
            schemas.append(s)

        for q in self.subqueries:
            if s := q.schema:
                schemas.append(s)

        return schemas

    @property
    def subqueries(self) -> list[Self]:
        """Return a list of all the subqueries in the WHERE clause"""
        subqueries = []

        for f in self.fields_where:
            if isinstance(f.value, Query):
                subqueries.append(f.value)

        return subqueries

    @property
    def OR(self) -> Self:
        """Wraps left and right field statements with an OR clause, you can
        chain as many OR calls as you want to create an any length OR clause

        I don't love that I had to use OR instead of "or" but "or" is a
        reserved keyword and I thought OR was better than like _or, or_ or _or_

        :Example:
            self.eq_foo(1).OR.eq_foo(5).OR.eq_foo(10)
            # (foo=1 OR foo=5 OR foo=10)
        """
        self.fields_where[-1].or_clause = True
        return self

    @property
    def or_(self) -> Self:
        return self.OR

    @property
    def AND(self) -> Self:
        """This is just here for completeness with .OR since, by default, any
        statements will be joined by AND"""
        self.fields_where[-1].or_clause = False
        return self

    @property
    def and_(self) -> Self:
        return self.AND

    def __init__(self, orm_class=None, **kwargs):
        # needed to use the db querying methods like get(), if you just want to
        # build a query then you don't need to bother passing this in
        self.orm_class = orm_class
        self.reset()

    def reset(self):
        self._filter = None
        self.fields_set = self.fields_set_class()
        self.fields_select = self.fields_select_class()
        self.fields_where = self.fields_where_class()
        self.fields_sort = self.fields_sort_class()
        self.bounds = self.bounds_class()
        self.compounds = []

    def _find_method(self, method_name: str) -> tuple[str, str]:
        """Given a method name like <OPERATOR>_<FIELD_NAME> or 
        <FIELD_NAME>_<OPERATOR>, split those into <OPERATOR> and <FIELD_NAME>
        if there is an existing <OPERATOR>_field method that exists

        :example:
            self.gt_foo() # (<gt_field>, None, "foo")

        :returns: tuple[str, str], (<FIELD_METHOD>, <FIELD_NAME>)
        """
        # infinite recursion check, if a *_field method gets in here then it
        # doesn't exist
        if method_name.endswith("_field"):
            raise AttributeError(method_name)

        try:
            # check for <NAME>_<FIELD_NAME>
            name, field_name = method_name.split("_", 1)

        except ValueError:
            raise AttributeError("invalid potential method: {}".format(
                method_name
            ))

        else:
            if not name:
                raise AttributeError(
                    f"Could not resolve methods from {method_name}",
                )

            else:
                field_method_name = "{}_field".format(name)
                field_method = getattr(self, field_method_name, None)
                if not field_method:
                    # let's try reversing the split, so <FIELD_NAME>_<NAME>
                    field_name, name = method_name.rsplit("_", 1)
                    field_method_name = "{}_field".format(name)
                    field_method = getattr(self, field_method_name, None)

                if not field_method:
                    raise AttributeError(
                        f"No field method derived from {method_name}",
                    )

        return field_method, field_name

    def _create_field(
        self,
        field_name: str,
        field_val: Any = None,
        **kwargs,
    ) -> QueryField:
        """Creates a QueryField instance"""
        return self.field_class(self, field_name, field_val, **kwargs)

    def _append_compound(self, operator, queries, **kwargs) -> Self:
        """Internal method used by .intersect(), .union(), and .difference()"""
        for i, query in enumerate(queries):
            if self.fields_select:
                if not query.fields_select:
                    if self.schema == query.schema:
                        query.select(*self.fields_select.names())

            else:
                if i == 0 and query.fields_select:
                    self.select(*query.fields_select.names())

        self.compounds.append((operator, queries))
        return self

    def _append_operation(
        self,
        operator: str,
        field_name: str,
        field_val: Any = None,
        **kwargs,
    ) -> Self:
        """Internal method that appends an operation to the where clause"""
        kwargs["operator"] = operator
        kwargs["clause"] = "where"
        f = self._create_field(field_name, field_val, **kwargs)
        self.fields_where.append(f)
        return self

    def _append_sort(
        self,
        direction: int,
        field_name: str,
        field_val: Any = None,
        **kwargs,
    ) -> Self:
        """Internal method that appends a sort to the sort clause

        sort this query by field_name in direction

        used to be named sort_field

        :param field_name: string, the field to sort on
        :param direction: integer, negative for DESC, positive for ASC
        :param field_val: list, the order the rows should be returned in
        """
        if direction > 0:
            direction = 1

        elif direction < 0:
            direction = -1

        else:
            raise ValueError("direction {} is undefined".format(direction))

        kwargs["direction"] = direction
        kwargs["is_list"] = True
        kwargs["clause"] = "sort"
        f = self._create_field(field_name, field_val, **kwargs)
        self.fields_sort.append(f)
        return self

    def distinct(self, *field_names, **kwargs) -> Self:
        """Mark the passed in query select fields as distinct"""
        self.fields_select.options["distinct"] = True
        return self.select(*field_names, **kwargs)

    def select_field(self, field_name: str, **kwargs) -> Self:
        """set a field to be selected, this is automatically called when you do
        select_FIELDNAME(...)"""
        if field_name == "*":
            self.fields_select.options["all"] = True

        else:
            kwargs["clause"] = "select"
            field = self._create_field(field_name, **kwargs)
            self.fields_select.append(field)

        return self

    def select_ref(self, orm_class: type[OrmType]) -> Self:
        """Find the field in this query's schema that matches `orm_class`
        and select that field name
        """
        schema = self.schema

        for field_name, ref_field in schema.ref_fields.items():
            if ref_field.ref_class is orm_class:
                self.select_field(field_name)
                break

        return self

    def select_dep(self, orm_class: type[OrmType]) -> Self:
        """Select the primary key of `self` if `orm_class` matches this
        query's orm class
        """
        if dep_class := self.orm_class:
            if orm_class is dep_class:
                self.select_pk()

        return self

    def select(self, *field_names: str, **kwargs) -> Self:
        """set multiple fields to be selected, this is the many version of 
        .select_field"""
        for field_name in make_list(field_names):
            self.select_field(field_name, **kwargs)
        return self

    def set_field(self, field_name, field_val, **kwargs) -> Self:
        """Set a field into .fields_set attribute

        In insert/update queries, these are the fields that will be
        inserted/updated into the db
        """
        kwargs["clause"] = "set"
        field = self._create_field(field_name, field_val, **kwargs)
        self.fields_set.append(field)
        return self

    def set(self, fields=None, **fields_kwargs) -> Self:
        """completely replaces the current .fields with fields and
        fields_kwargs combined, this is the many version of .set_field
        """
        fields = make_dict(fields, fields_kwargs)
        for field_name, field_val in fields.items():
            self.set_field(field_name, field_val)
        return self

    def incr_field(
        self,
        field_name: str,
        increment: int = 1,
        field_val: int|None = None,
        **kwargs,
    ) -> Self:
        """Set a field to be incremented into .fields_set attribute

        In update queries, these are the fields that will be
        updated into the db using field_name = field_name + field_val
        for an atomic increment
        """
        return self.set_field(
            field_name,
            field_val,
            increment=increment,
            **kwargs
        )

    def intersect(self, *queries, **kwargs) -> Self:
        """Intersect a set of queries. Returns rows that are common in all the 
        queries.

        Compound Query instances will only return values, not Orm instances

        :Example:
            Query().intersect(
                Query().select_foo(),
                Query().select_foo(),
            ).all()

        https://www.sqlite.org/syntax/compound-operator.html

        :param *queries: two or more queries, they should all select the same
            field types, the first query sets the name of the return columns
        """
        return self._append_compound("intersect", queries, **kwargs)

    def union(self, *queries, **kwargs) -> Self:
        """Return all the rows from all the queries

        See .intersect() since this works very similar
        """
        return self._append_compound("union", queries, **kwargs)

    def difference(self, *queries, **kwargs) -> Self:
        """Return everything in the first query that isn't in any of the other
        queries.

        Return the rows with rows in the first query that are not in the other
        queries

        This is named .difference() instead of .except() because except is a
        python keyword.

        See .intersect() since this works very similar
        """
        return self._append_compound("except", queries, **kwargs)

    def eq_field(self, field_name, field_val=None, **field_kwargs) -> Self:
        """<FIELD_NAME> = <FIELD_VALUE>"""
        return self._append_operation(
            "eq",
            field_name,
            field_val,
            **field_kwargs
        )

    def ne_field(self, field_name, field_val=None, **field_kwargs) -> Self:
        """<FIELD_NAME> != <FIELD_VALUE>"""
        field_kwargs["inverse"] = True
        return self.eq_field(field_name, field_val, **field_kwargs)

    def between_field(self, field_name, low, high) -> Self:
        """<FIELD_NAME> >= low AND <FIELD_NAME> <= high"""
        self.gte_field(field_name, low)
        self.lte_field(field_name, high)
        return self

    def lte_field(self, field_name, field_val=None, **field_kwargs) -> Self:
        """<FIELD_NAME> <= <FIELD_VALUE>"""
        return self._append_operation(
            "lte",
            field_name,
            field_val,
            **field_kwargs
        )

    def lt_field(self, field_name, field_val=None, **field_kwargs) -> Self:
        """<FIELD_NAME> < <FIELD_VALUE>"""
        return self._append_operation(
            "lt",
            field_name,
            field_val,
            **field_kwargs
        )

    def gte_field(self, field_name, field_val=None, **field_kwargs) -> Self:
        """<FIELD_NAME> >= <FIELD_VALUE>"""
        return self._append_operation(
            "gte",
            field_name,
            field_val,
            **field_kwargs
        )

    def gt_field(self, field_name, field_val=None, **field_kwargs) -> Self:
        """<FIELD_NAME> > <FIELD_VALUE>"""
        return self._append_operation(
            "gt",
            field_name,
            field_val,
            **field_kwargs
        )

    def in_field(self, field_name, field_val=None, **field_kwargs) -> Self:
        """<FIELD_NAME> IN (<FIELD_VALUE>)

        :param field_val: list, a list of field_val values
        """
        field_kwargs["is_list"] = True
        return self._append_operation(
            "in",
            field_name,
            field_val,
            **field_kwargs
        )

    def nin_field(self, field_name, field_val=None, **field_kwargs) -> Self:
        """<FIELD_NAME> NOT IN (<FIELD_VALUE>)

        :param field_val: list, a list of field_val values
        """
        field_kwargs["inverse"] = True
        return self.in_field(field_name, field_val, **field_kwargs)

    def startswith_field(self, field_name, field_val, **field_kwargs) -> Self:
        """<FIELD_NAME> LIKE <FIELD_VALUE>%"""
        return self.like_field(
            field_name,
            f"{field_val}%",
            **field_kwargs,
        )

    def endswith_field(self, field_name, field_val, **field_kwargs) -> Self:
        """<FIELD_NAME> LIKE %<FIELD_VALUE>"""
        return self.like_field(
            field_name,
            f"%{field_val}",
            **field_kwargs,
        )

    def contains_field(self, field_name, field_val, **field_kwargs) -> Self:
        """<FIELD_NAME> LIKE %<FIELD_VALUE>%"""
        return self.like_field(
            field_name,
            f"%{field_val}%",
            **field_kwargs,
        )

    def like_field(self, field_name, field_val, **field_kwargs) -> Self:
        """Perform a field_name LIKE field_val query

        You should favor `.startswith_*`, `.endswith_*`, or `.contains_*`
        methods over this method, this is more of an internal method

        If you need to get the inverse (`NOT LIKE`) then pass in the `inverse`
        keyword


        :param field_name: string, the field we are filtering on
        :param field_val: string, the like query: %val, %val%, val%
        :keyword inverse: bool, set to True to inverse the matches
        :returns: self, for fluid interface
        """
        if not field_val:
            raise ValueError("Cannot LIKE nothing")

        return self._append_operation(
            "like",
            field_name,
            field_val,
            **field_kwargs,
        )

    def raw_field(self, field_name, *field_vals, **field_kwargs) -> Self:
        field_kwargs["raw"] = True
        return self._append_operation(
            "raw",
            field_name,
            field_vals,
            **field_kwargs
        )

    def asc(self, *field_names) -> Self:
        """the many version of .asc_field"""
        for field_name in field_names:
            self.asc_field(field_name)
        return self

    def asc_field(self, field_name, field_val=None) -> Self:
        """<FIELD_NAME> ASC"""
        return self._append_sort(1, field_name, field_val)

    def desc(self, *field_names) -> Self:
        """the many version of .desc_field"""
        for field_name in field_names:
            self.desc_field(field_name)
        return self

    def desc_field(self, field_name, field_val=None) -> Self:
        """<FIELD_NAME> DESC"""
        return self._append_sort(-1, field_name, field_val)

    def filter(self, predicate: Callable[[OrmType|tuple|Any], bool]) -> Self:
        """Set the predicate (callback) for an iterator returned from this
        instance

        :param predicate: callable, this will be passed to the iterator in the
            .create_iterator method
        :returns: self, for fluid interface
        """
        self._filter = predicate
        return self

    def limit(self, limit) -> Self:
        """LIMIT <limit>"""
        self.bounds.limit = limit
        return self

    def offset(self, offset) -> Self:
        """OFFSET <offset>"""
        self.bounds.offset = offset
        return self

    def page(self, page) -> Self:
        """OFFSET <PAGE_CONVERTED_TO_OFFSET>"""
        self.bounds.page = page
        return self

    def copy(self) -> Self:
        """nice handy wrapper around the deepcopy"""
        return copy.deepcopy(self)

    def __deepcopy__(self, memodict={}) -> Self:
        instance = type(self)(self.orm_class)
        ignore_keys = set(["_interface", "interface"])
        for key, val in self.__dict__.items():
            if key not in ignore_keys:
                setattr(instance, key, copy.deepcopy(val, memodict))
        return instance

    def __getattr__(self, method_name: str) -> FieldCallable|Self:
        """Allows fluid interface of .<OPERATOR>_<FIELD_NAME>(<FIELD_VALUE>)

        Also allows getting a query instance from any other orm class which
        is handy for fluid subqueries without having to import anything

        :returns:
            * callback, this will return a callback that wraps a 
                `*_field` method and passes in `<FIELD_NAME>` to the `*_field`
                method
            * Self, this will be a query instance for the requested orm
                class via `model_name`
        """
        try:
            field_method, field_name = self._find_method(method_name)

            def callback(*args, **kwargs):
                if field_method:
                    ret = field_method(field_name, *args, **kwargs)

                return ret

            return callback

        except AttributeError:
            if self.orm_class:
                try:
                    orm_classes = self.orm_class.orm_classes
                    if orm_class := orm_classes.find_class(method_name):
                        return orm_class.query

                except KeyError:
                    raise AttributeError(method_name)

            raise

    def __await__(self) -> Generator[None, None, Iterator]:
        """Returns an iterator if this query is awaited"""
        if False: yield
        return self.iterator()

    async def __aiter__(self) -> AsyncIterable[OrmType|Any]:
        """Allows fluid interface for iterating through the query

        :example:
            [r async for r in q]
        """
        async for row in await self.get():
            yield row

    def __str__(self) -> str:
        """Attempt to render the query, this should be used mainly for
        debugging and not in logs or things like that since this can be intense
        and multiline and also wrong
        """
        try:
            return self.render()

        except AttributeError:
            return super().__str__()

    def render(self, **kwargs) -> str:
        """Render the query

        :returns: string, the rendered query, this is not assured to be a valid
            query but is handy for quickly debugging what the query roughly
            looks like
        """
        return self.interface.render(self.schema, self, **kwargs)

    async def tolist(self, **kwargs) -> list[OrmType|Any]:
        """Wraps the Iterator.tolist method and does the same thing"""
        iterator = await self.get(**kwargs)
        return await iterator.tolist()

    async def raw(self, query_str, *query_args, **kwargs):
        """Send a raw query to the interface without any processing

        NOTE -- This will allow you to make any raw query and will usually
        return raw results

        :param query_str: str, the raw query for whatever the backend interface
            is
        :param query_args: list, if you have named parameters, you can pass in
            the values
        :param **kwargs: passed directly to the interface
        :returns: Any, depends on the interface and the type of query
        """
        return await self.interface.raw(query_str, *query_args, **kwargs)

    async def cursor(self, **kwargs) -> AsyncIterable[Mapping]:
        """Return a raw Interface cursor

        :param **kwargs: passed directly to the interface
        :returns: object, a cursor instance that can retrieve results from the
            interface's backend
        """
        return await self.interface.get(
            self.schema,
            self,
            cursor_result=True,
            **kwargs,
        )

    def iterator(self, **kwargs) -> Iterator:
        """Return an iterator instance that can pull rows using this query"""
        self.bounds.paginate = kwargs.pop("paginate", True)

        if self.orm_class:
            iterator = self.orm_class.iterator_class(self)

        else:
            iterator = self.iterator_class(self)

        return iterator

    async def get(self, **kwargs):
        """
        get results from the db

        :returns: Iterator
        """
        iterator = self.iterator(**kwargs)
        await iterator.load(**kwargs)
        return iterator

    async def one(self, **kwargs):
        """get one row from the db"""
        ret = None

        kwargs["paginate"] = False
        async for ret in await self.limit(1).get(**kwargs):
            break

        return ret

    async def count(self, **kwargs) -> int:
        """return the row count of the criteria

        :param **kwargs: passed through to the interface
        :returns: int, the row count
        """
        # sorting shouldn't matter for a count query
        fields_sort = self.fields_sort
        self.fields_sort = self.fields_sort_class()

        # more than one selected field will cause the count query to error out
        fields_select = self.fields_select
        self.fields_select = self.fields_select_class()

        # setting bounds causes count(*) to return 0 in both Postgres and
        # SQLite
        bounds = self.bounds
        self.bounds = self.bounds_class()

        ret = await self.interface.count(self.schema, self, **kwargs)

        # restore previous values now that count is done
        self.fields_sort = fields_sort
        self.fields_select = fields_select
        self.bounds = bounds

        # now we are going to compensate for the bounds being set
        if self.bounds:
            offset = self.bounds.offset
            ret -= offset
            if self.bounds.has_limit():
                limit = self.bounds.limit
                if ret > limit:
                    ret = limit

        return ret

    async def has(self, **kwargs) -> bool:
        """returns true if there is atleast one row in the db matching the
        query, False otherwise

        :returns: bool
        """
        return await self.interface.has(self.schema, self, **kwargs)

    async def insert(self, **kwargs) -> Mapping|None:
        """persist the .fields that were set with .set_field and .set

        :returns: The newly inserted row as stored in the db
        """
        return await self.interface.insert(
            self.schema,
            self.fields_set.todict(),
            **kwargs,
        )

    async def update(self, **kwargs) -> list[Mapping]|int|None:
        """persist the .fields set in .set and .set_field using .fields_where
        """
        return await self.interface.update(
            self.schema,
            self.fields_set.todict(),
            self,
            **kwargs,
        )

    async def upsert(
        self,
        conflict_field_names: Iterable[str]|None = None,
        **kwargs,
    ) -> Mapping|None:
        """persist the .fields set with .set and .set_field

        The insert fields are all the fields set in .fields.

        The update fields are all the fields set in .fields minus the conflict
        fields

        :param conflict_field_names: list, the list of field names that will be
            checked for conflicts and will insert if these field values don't
            already exist in the db or update if they do
        :param **kwargs: passed through to the interface upsert method
        :returns: The newly inserted/updated row as stored in the db
        """
        insert_fields = self.fields_set.todict()
        update_fields = dict(insert_fields)

        if not conflict_field_names:
            conflict_field_names = self.schema.pk_names

        for field_name in conflict_field_names:
            update_fields.pop(field_name, None)

        return await self.interface.upsert(
            self.schema,
            insert_fields,
            update_fields,
            conflict_field_names,
            **kwargs,
        )

    async def delete(self, **kwargs) -> list[Mapping]|int|None:
        """remove fields matching the where criteria"""
        return await self.interface.delete(
            self.schema,
            self,
            **kwargs,
        )

