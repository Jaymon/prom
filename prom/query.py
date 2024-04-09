# -*- coding: utf-8 -*-
"""
Classes and stuff that handle querying the interface for a passed in Orm class
"""
import copy
from collections import defaultdict
from collections.abc import AsyncIterable
import re

from datatypes import ListIterator

from .compat import *
from .utils import make_list, get_objects, make_dict


class Iterator(ListIterator, AsyncIterable):
    """The main iterator for all query methods that return iterators

    This is returned from the Query.get() method, this is also
    the Iterator class that is set in Orm.iterator_class

    fields --
        filter: callback, an iterator filter, all yielded rows will be passed
            through this callback and skipped if filter(row) returns False

    :example:
        # iterate through all the primary keys of some orm
        async for pk in SomeOrm.select_pk().query.get():
            print pk
    """
    @property
    def orm_class(self):
        return self.query.orm_class

    def __init__(self, cursor, query):
        """create an iterator for a query

        :param cursor: object, the cursor object retrieved from the interface
        :param query: Query, the query instance that produced this iterator
        """
        if query._filter:
            self.filter = query._filter

        self.query = query

        self._cursor = cursor
        self._cursor_exhausted = False
        self.field_names = self.query.fields_select.names()

    async def has_more(self):
        """Return true if there are more results for this query

        :returns: boolean, True if this query could've returned more results
        """
        ret = False
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
        await self._cursor.close()
        self._cursor_exhausted = True

    async def tolist(self):
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

    def __iter__(self):
        """Make sure no one thinks we can iterate through this syncronously"""
        raise NotImplementedError()

    async def __aiter__(self):
        """
        https://docs.python.org/3/reference/datamodel.html#object.__aiter__
        """
        if self._cursor_exhausted:
            raise ValueError("Cursor has been exhausted, rerun the query")

        cursor_i = 0
        cursor_limit = -1
        if self.query.bounds.has_more():
            cursor_limit = self.query.bounds.limit

        try:
            async for row in self._cursor:
                if cursor_limit > 0 and cursor_i >= cursor_limit:
                    break

                o = self.hydrate(row)
                if self.filter(o):
                    yield o

                cursor_i += 1

        finally:
            await self.close()

    def __len__(self):
        """Make sure no one thinks we can get count syncronously"""
        raise NotImplementedError()

    async def count(self):
        """Get the number of rows this iterator represents

        Honestly, you probably shouldn't use this and instead do a count query
        instead since that gives you way more control

        :returns: int, the rows this iterator represents
        """
        cursor = self._cursor
        count = cursor.rowcount

        if count >= 0:
            bounds = self.query.bounds
            if bounds.has_more() and bounds.limit_paginate == count:
                # compensate for having pulled one extra row
                count -= 1

        else:
            # we couldn't get the rowcount from the cursor for some reason, so
            # we will need to query for it

            # SQLite cursor's will always have rowcount=-1 until the cursor is
            # exhausted or a fetch*() method has been called, ugh.
            # https://stackoverflow.com/a/839419/5006
            count = await self.query.copy().count()

        return count

    async def __getitem__(self, i):
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
        limit = b.limit if b.has_limit() else await self.count()
        b = q.bounds_class(limit=limit, offset=q.bounds.offset)

        if isinstance(i, slice):
            if i.step:
                raise ValueError("slice stepping is not supported")

            if i.start:
                start = b.find_offset(i.start)

            else:
                start = b.offset

            if i.stop:
                stop = b.find_offset(i.stop)

            else:
                stop = b.limit

            q.limit(stop - start).offset(start)
            return type(self)(await q.cursor(), q)

        else:
            offset = b.find_offset(i)

            o = await q.offset(offset).one()
            if o is None:
                raise IndexError("Iterator index {} out of range".format(i))

            return o

    def __repr__(self):
        format_str = "[ ... {} ... ]"
        format_args = [self.__class__.__name__]

        orm_class = self.orm_class
        if orm_class:
            format_str = "[ ... {} on {} ... ]"
            format_args.append(orm_class.__name__)

        return format_str.format(*format_args)

    def filter(self, o):
        """run o through the filter, if True then orm o should be included

        NOTE -- The filter callback needs to account for non Orm instance
            values of o

        :param o: Orm|tuple[Any]|Any, usually an Orm instance but can also be
            a tuple or single value
        :returns: boolean, True if o should be filtered
        """
        return True

    def hydrate(self, d):
        """Prepare the raw dict d returned from the interface cursor to be
        returned by higher level objects, this will usually mean hydrating an
        Orm instance or stuff like that

        NOTE -- this will call Orm.hydrate and should be the only place that
            will call that method

        :param d: dict, the raw dict cursor result returned from the interface
        :returns: Orm|tuple[Any]|Any, usually an Orm instance populated with d
            but can also be a tuple if the query selected more than one field.
            If the query selected one field then just that value will be
            returned
        """
        r = None
        orm_class = self.orm_class
        if self.field_names:
            field_vals = []
            for field_name in self.field_names:
                fv = d[field_name]
                if orm_class:
                    fv = orm_class.schema.fields[field_name].iget(None, fv)
                field_vals.append(fv)
            r = field_vals if len(self.field_names) > 1 else field_vals[0]

        else:
            if orm_class:
                r = orm_class.hydrate(d)

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

    def find_offset(self, i):
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
                        "Iterator index {} out of range".format(i)
                    )

        else:
            limit = self.limit
            offset = limit + i

            if offset < 0:
                raise IndexError("Iterator index {} out of range".format(i))

        return offset

    def find_more_index(self):
        #return self.offset + self.limit_paginate
        return self.offset + self.limit


class QueryField(object):
    """Holds information for a field in the query"""
    @property
    def schema(self):
        return self.query.schema if self.query else None

    def __init__(self, query, field_name, field_val=None, **kwargs):
        self.query = query
        self.operator = kwargs.pop("operator", None)
        self.is_list = kwargs.pop("is_list", False)
        self.direction = kwargs.pop("direction", None) # 1 = ASC, -1 = DESC
        self.increment = kwargs.pop("increment", False) # Query.incr_field
        self.raw = kwargs.pop("raw", False)
        self.clause = kwargs.pop("clause", "")
        self.function_name = kwargs.pop("function_name", "")
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
        # Field.iquery method body
        self.value = field_val

        if self.is_list and not self.is_subquery():
            if field_val:
                field_val = make_list(field_val)
                for i in range(len(field_val)):
                    field_val[i] = self.iquery(field_val[i])

        else:
            field_val = self.iquery(field_val)

        self.value = field_val

    def iquery(self, field_val):
        schema = self.schema
        if schema:
            schema_field = getattr(schema, self.name)
            if ref_class := schema_field.ref_class:
                if isinstance(field_val, ref_class):
                    field_val = field_val.pk

            field_val = schema_field.iquery(self, field_val)

        return field_val

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
    def interface(self):
        return self.orm_class.interface if self.orm_class else None

    @property
    def schema(self):
        return self.orm_class.schema if self.orm_class else None

    @property
    def schemas(self):
        """Find and return all the schemas that are needed for this query to
        complete successfully

        Another way to put this is all the schemas this query touches

        :returns: list, a list of Schema instances
        """
        schemas = []
        s = self.schema
        if s:
            schemas.append(s)

        for f in self.fields_where:
            if isinstance(f.value, Query):
                s = f.value.schema
                if s:
                    schemas.append(s)

        return schemas

    @property
    def OR(self):
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
    def or_(self): return self.OR
    @property
    def _or(self): return self.OR
    @property
    def _or_(self): return self.OR

    @property
    def AND(self):
        """This is just here for completeness with .OR since, by default, any
        statements will be joined by AND"""
        self.fields_where[-1].or_clause = False
        return self

    @property
    def and_(self): return self.AND
    @property
    def _and(self): return self.AND
    @property
    def _and_(self): return self.AND

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

    def ref(self, orm_classpath):
        """
        takes a classpath to allow query-ing from another Orm class

        the reason why it takes string paths is to avoid infinite recursion
        import problems because an orm class from module A might have a ref
        from module B and sometimes it is handy to have module B be able to get
        the objects from module A that correspond to the object in module B,
        but you can't import module A into module B because module B already
        imports module A.

        :param orm_classpath: string|type, a full python class path (eg,
            foo.bar.Che) or an actual model.Orm python class
        return -- Query
        """
        # split orm from module path
        if isinstance(orm_classpath, basestring):
            orm_module, orm_class = get_objects(orm_classpath)

        else:
            orm_class = orm_classpath

        return orm_class.query

    def ref_class(self, orm_classpath):
        """alias to .ref, here to match Orm.ref_class syntax"""
        return self.ref(orm_classpath)

    def find_methods(self, method_name):
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
                    'Could not resolve methods from {}"'.format(
                        method_name
                    )
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
                        'No field method derived from {}'.format(
                            method_name
                            )
                        )

        return field_method, field_name

    def __getattr__(self, method_name):
        """Allows fluid interface of .<OPERATOR>_<FIELD_NAME>(<FIELD_VALUE>)

        :returns: callback, this will return a callback that wraps a 
            *_field method and passes in <FIELD_NAME> to the *_field method
        """
        field_method, field_name = self.find_methods(method_name)

        def callback(*args, **kwargs):
            if field_method:
                ret = field_method(field_name, *args, **kwargs)

            return ret

        return callback

    def create_field(self, field_name, field_val=None, **kwargs):
        """Creates a QueryField instance"""
        f = self.field_class(self, field_name, field_val, **kwargs)
        return f

    def create_iterator(self, cursor):
        """Creates a query.Iterator instance wrapping cursor

        :param cursor: object, this is retrieved from the Interface
        :returns: Iterator
        """
        if self.orm_class:
            iterator = self.orm_class.iterator_class(cursor, query=self)

        else:
            iterator = self.iterator_class(cursor, query=self)

        return iterator

    def append_compound(self, operator, queries, **kwargs):
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

    def append_operation(self, operator, field_name, field_val=None, **kwargs):
        """Internal method that appends an operation to the where clause"""
        kwargs["operator"] = operator
        kwargs["clause"] = "where"
        f = self.create_field(field_name, field_val, **kwargs)
        self.fields_where.append(f)
        return self

    def append_sort(self, direction, field_name, field_val=None, **kwargs):
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
        f = self.create_field(field_name, field_val, **kwargs)
        self.fields_sort.append(f)
        return self

    def distinct(self, *field_names, **kwargs):
        """Mark the passed in query select fields as distinct"""
        self.fields_select.options["distinct"] = True
        return self.select(*field_names, **kwargs)

    def select_field(self, field_name, **kwargs):
        """set a field to be selected, this is automatically called when you do
        select_FIELDNAME(...)"""
        if field_name == "*":
            self.fields_select.options["all"] = True

        else:
            kwargs["clause"] = "select"
            field = self.create_field(field_name, **kwargs)
            self.fields_select.append(field)

        return self

    def select(self, *field_names, **kwargs):
        """set multiple fields to be selected, this is the many version of 
        .select_field"""
        for field_name in make_list(field_names):
            self.select_field(field_name, **kwargs)
        return self

    def set_field(self, field_name, field_val, **kwargs):
        """Set a field into .fields_set attribute

        In insert/update queries, these are the fields that will be
        inserted/updated into the db
        """
        kwargs["clause"] = "set"
        field = self.create_field(field_name, field_val, **kwargs)
        self.fields_set.append(field)
        return self

    def set(self, fields=None, **fields_kwargs):
        """completely replaces the current .fields with fields and fields_kwargs
        combined, this is the many version of .set_field
        """
        fields = make_dict(fields, fields_kwargs)
        for field_name, field_val in fields.items():
            self.set_field(field_name, field_val)
        return self

    def incr_field(self, field_name, increment=1, field_val=None, **kwargs):
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

    def intersect(self, *queries, **kwargs):
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
        return self.append_compound("intersect", queries, **kwargs)

    def union(self, *queries, **kwargs):
        """Return all the rows from all the queries

        See .intersect() since this works very similar
        """
        return self.append_compound("union", queries, **kwargs)

    def difference(self, *queries, **kwargs):
        """Return everything in the first query that isn't in any of the other
        queries.

        Return the rows with rows in the first query that are not in the other
        queries

        This is named .difference() instead of .except() because except is a
        python keyword.

        See .intersect() since this works very similar
        """
        return self.append_compound("except", queries, **kwargs)

    def eq_field(self, field_name, field_val=None, **field_kwargs):
        """<FIELD_NAME> = <FIELD_VALUE>"""
        return self.append_operation(
            "eq",
            field_name,
            field_val,
            **field_kwargs
        )

    def ne_field(self, field_name, field_val=None, **field_kwargs):
        """<FIELD_NAME> != <FIELD_VALUE>"""
        return self.append_operation(
            "ne",
            field_name,
            field_val,
            **field_kwargs
        )

    def between_field(self, field_name, low, high):
        """<FIELD_NAME> >= low AND <FIELD_NAME> <= high"""
        self.gte_field(field_name, low)
        self.lte_field(field_name, high)
        return self

    def lte_field(self, field_name, field_val=None, **field_kwargs):
        """<FIELD_NAME> <= <FIELD_VALUE>"""
        return self.append_operation(
            "lte",
            field_name,
            field_val,
            **field_kwargs
        )

    def lt_field(self, field_name, field_val=None, **field_kwargs):
        """<FIELD_NAME> < <FIELD_VALUE>"""
        return self.append_operation(
            "lt",
            field_name,
            field_val,
            **field_kwargs
        )

    def gte_field(self, field_name, field_val=None, **field_kwargs):
        """<FIELD_NAME> >= <FIELD_VALUE>"""
        return self.append_operation(
            "gte",
            field_name,
            field_val,
            **field_kwargs
        )

    def gt_field(self, field_name, field_val=None, **field_kwargs):
        """<FIELD_NAME> > <FIELD_VALUE>"""
        return self.append_operation(
            "gt",
            field_name,
            field_val,
            **field_kwargs
        )

    def in_field(self, field_name, field_val=None, **field_kwargs):
        """<FIELD_NAME> IN (<FIELD_VALUE>)

        :param field_val: list, a list of field_val values
        """
        field_kwargs["is_list"] = True
        return self.append_operation(
            "in",
            field_name,
            field_val,
            **field_kwargs
        )

    def nin_field(self, field_name, field_val=None, **field_kwargs):
        """<FIELD_NAME> NOT IN (<FIELD_VALUE>)

        :param field_val: list, a list of field_val values
        """
        field_kwargs["is_list"] = True
        return self.append_operation(
            "nin",
            field_name,
            field_val,
            **field_kwargs
        )

    def startswith_field(self, field_name, field_val, **field_kwargs):
        """<FIELD_NAME> LIKE <FIELD_VALUE>%"""
        return self.like_field(
            field_name,
            "{}%".format(field_val),
            **field_kwargs
        )

    def endswith_field(self, field_name, field_val, **field_kwargs):
        """<FIELD_NAME> LIKE %<FIELD_VALUE>"""
        return self.like_field(
            field_name,
            "%{}".format(field_val),
            **field_kwargs
        )

    def contains_field(self, field_name, field_val, **field_kwargs):
        """<FIELD_NAME> LIKE %<FIELD_VALUE>%"""
        return self.like_field(
            field_name,
            "%{}%".format(field_val),
            **field_kwargs
        )

    def like_field(self, field_name, field_val, **field_kwargs):
        """Perform a field_name LIKE field_val query

        :param field_name: string, the field we are filtering on
        :param field_val: string, the like query: %val, %val%, val%
        :returns: self, for fluid interface
        """
        if not field_val:
            raise ValueError("Cannot LIKE nothing")

        return self.append_operation(
            "like",
            field_name,
            field_val,
            **field_kwargs
        )

    def nlike_field(self, field_name, field_val, **field_kwargs):
        """Perform a field_name NOT LIKE field_val query

        :param field_name: string, the field we are filtering on
        :param field_val: string, the like query: %val, %val%, val%
        :returns: self, for fluid interface
        """
        if not field_val:
            raise ValueError("Cannot NOT LIKE nothing")

        return self.append_operation(
            "nlike",
            field_name,
            field_val,
            **field_kwargs
        )

    def raw_field(self, field_name, *field_vals, **field_kwargs):
        field_kwargs["raw"] = True
        return self.append_operation(
            "raw",
            field_name,
            field_vals,
            **field_kwargs
        )

    def asc(self, *field_names):
        """the many version of .asc_field"""
        for field_name in field_names:
            self.asc_field(field_name)
        return self

    def asc_field(self, field_name, field_val=None):
        """<FIELD_NAME> ASC"""
        return self.append_sort(1, field_name, field_val)

    def desc(self, *field_names):
        """the many version of .desc_field"""
        for field_name in field_names:
            self.desc_field(field_name)
        return self

    def desc_field(self, field_name, field_val=None):
        """<FIELD_NAME> DESC"""
        return self.append_sort(-1, field_name, field_val)

    def filter(self, predicate):
        """Set the predicate (callback) for an iterator returned from this
        instance

        :param predicate: callable, this will be passed to the iterator in the
            .create_iterator method
        :returns: self, for fluid interface
        """
        self._filter = predicate
        return self

    def limit(self, limit):
        """LIMIT <limit>"""
        self.bounds.limit = limit
        return self

    def offset(self, offset):
        """OFFSET <offset>"""
        self.bounds.offset = offset
        return self

    def page(self, page):
        """OFFSET <PAGE_CONVERTED_TO_OFFSET>"""
        self.bounds.page = page
        return self

    def copy(self):
        """nice handy wrapper around the deepcopy"""
        return copy.deepcopy(self)

    def __deepcopy__(self, memodict={}):
        instance = type(self)(self.orm_class)
        ignore_keys = set(["_interface", "interface"])
        for key, val in self.__dict__.items():
            if key not in ignore_keys:
                setattr(instance, key, copy.deepcopy(val, memodict))
        return instance

    def __str__(self):
        """Attempt to render the query, this should be used mainly for debugging
        and not in logs or things like that since this can be intense and
        multiline and also wrong
        """
        try:
            return self.render()

        except AttributeError:
            return super().__str__()

    def render(self, **kwargs):
        """Render the query

        :returns: string, the rendered query, this is not assured to be a valid
            query but is handy for quickly debugging what the query roughly
            looks like
        """
        return self.interface.render(self.schema, self, **kwargs)

    async def __aiter__(self):
        """Allows fluid interface for iterating through the query

        :Example:
            [r async for r in q]
        """
        async for row in await self.get():
            yield row

    async def tolist(self, **kwargs):
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

    async def cursor(self, **kwargs):
        """Return a raw Interface cursor

        :param **kwargs: passed directly to the interface
        :returns: object, a cursor instance that can retrieve results from the
            interface's backend
        """
        return await self.interface.get(
            self.schema,
            self,
            cursor_result=True,
            **kwargs
        )

    async def get(self, **kwargs):
        """
        get results from the db

        :returns: Iterator
        """
        self.bounds.paginate = kwargs.pop("paginate", True)
        cursor = await self.cursor(**kwargs)
        return self.create_iterator(cursor)

    async def one(self, **kwargs):
        """get one row from the db"""
        ret = None

        kwargs["paginate"] = False
        async for ret in await self.limit(1).get(**kwargs):
            break

        return ret

    async def count(self, **kwargs):
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

    async def has(self, **kwargs):
        """returns true if there is atleast one row in the db matching the
        query, False otherwise

        :returns: bool
        """
        v = await self.one(**kwargs)
        return True if v else False

    async def insert(self, **kwargs):
        """persist the .fields that were set with .set_field and .set

        :returns: int|str|None, the primary key of the inserted row if it
            exists
        """
        return await self.interface.insert(
            self.schema,
            self.fields_set.todict(),
            **kwargs
        )

    async def update(self, **kwargs):
        """persist the .fields set in .set and .set_field using .fields_where
        """
        return await self.interface.update(
            self.schema,
            self.fields_set.todict(),
            self,
            **kwargs
        )

    async def upsert(self, conflict_field_names=None, **kwargs):
        """persist the .fields set with .set and .set_field

        The insert fields are all the fields set in .fields.

        The update fields are all the fields set in .fields minus the conflict
        fields

        :param conflict_field_names: list, the list of field names that will be
            checked for conflicts and will insert if these field values don't
            already exist in the db or update if they do
        :param **kwargs: passed through to the interface upsert method
        :returns: str|int|None, the primary key of the inserted/updated row
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
            **kwargs
        )

    async def delete(self, **kwargs):
        """remove fields matching the where criteria"""
        return await self.interface.delete(
            self.schema,
            self,
            **kwargs
        )

