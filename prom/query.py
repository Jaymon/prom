# -*- coding: utf-8 -*-
"""
Classes and stuff that handle querying the interface for a passed in Orm class
"""
from __future__ import unicode_literals, division, print_function, absolute_import
import copy
from collections import defaultdict, OrderedDict
import datetime
import logging
import os
from contextlib import contextmanager
import math
import inspect
import time
import re

from datatypes.collections import ListIterator
from datatypes import property as cachedproperty

from .utils import make_list, get_objects, make_dict, make_hash
from .interface import get_interfaces
from .compat import *


logger = logging.getLogger(__name__)


class Iterator(ListIterator):
    """The main iterator for all query methods that return iterators

    This is returned from the Query.get() and Query.all() methods, this is also
    the Iterator class that is set in Orm.iterator_class

    fields --
        ifilter -- callback -- an iterator filter, all yielded rows will be passed
            through this callback and skipped if ifilter(row) returns False

    examples --
        # iterate through all the primary keys of some orm
        for pk in SomeOrm.query.all().pk:
            print pk
    """
    @property
    def orm_class(self):
        return self.query.orm_class

    def __init__(self, query):
        """create an iterator for a query

        :param query: Query, the query instance that produced this iterator
        """
        self.query = query

        if query._ifilter:
            self.ifilter = query._ifilter # https://docs.python.org/2/library/itertools.html#itertools.ifilter

        self.reset()

    def has_more(self):
        """Return true if there are more results for this query if the query didn't
        have a LIMIT clause

        :returns: boolean, True if this query could've returned more results
        """
        ret = False
        if self.query.bounds.has_more():
            cursor = self.cursor()
            # https://www.psycopg.org/docs/cursor.html#cursor.rowcount says that
            # future versions of the spec reserve the right to return None
            if cursor.rowcount == -1 or cursor.rowcount is None:
                try:
                    if self[self.query.bounds.find_more_index()]:
                        ret = True

                except IndexError:
                    pass

            else:
                ret = self.query.bounds.limit_paginate == cursor.rowcount

        return ret

    def cursor(self):
        cursor = getattr(self, "_cursor", None)
        if not cursor:
            cursor = self.query.cursor()
            self._cursor = cursor
            self._cursor_i = 0
            self.field_names = self.query.fields_select.names()

        return cursor

    def reset(self):
        """put all the pieces together to build a generator of the results"""
        self._cursor = None
        self._cursor_i = 0

    def __iter__(self):
        self.reset()
        return self

    def next(self):
        cursor = self.cursor()
        cursor_next = cursor.__next__

        # if we have paginated the results we have to account for requesting the
        # one extra row to see if we have more results waiting
        if self.query.bounds.has_more():
            #if cursor.rownumber == self.query.bounds.limit:
            if self._cursor_i == self.query.bounds.limit:
                raise StopIteration()

        o = self.hydrate(cursor_next())
        self._cursor_i += 1
        while not self.ifilter(o):
            o = self.hydrate(cursor_next())
            self._cursor_i += 1
        return o

    def count(self):
        cursor = self.cursor()
        count = cursor.rowcount

        if count >= 0:
            # compensate for having pulled one extra row
            if self.query.bounds.has_more() and self.query.bounds.limit_paginate == count:
                count -= 1

        else:
            # we couldn't get the rowcount from the cursor for some reason, so
            # we will need to query for it

            # SQLite cursor's will always have rowcount=-1 until the cursor is
            # exhausted or a fetch*() method has been called, ugh.
            # https://stackoverflow.com/a/839419/5006
            count = self.query.copy().count()

        return count

    def __getitem__(self, i):
        it = self.copy()
        q = it.query

        b = q.bounds
        limit = b.limit if b.has_limit() else self.count()
        b = Bounds(limit=limit, offset=q.bounds.offset)

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
            it.query = q
            return it

        else:
            offset = b.find_offset(i)

            o = q.offset(offset).one()
            if o is None:
                raise IndexError("Iterator index {} out of range".format(i))

            return o

    def copy(self):
        q = self.query.copy()
        return type(self)(q)

    def reverse(self):
        for f in self.query.fields_sort:
            f.direction = -f.direction
        self.reset()

    def __getattr__(self, field_name):
        """If you have a set of results and just want to grab a certain field then
        you can do that

        :example:
            it = FooOrm.query.limit(10).get()
            it.pk # get all the primary keys from the results

        :param field_name: string, the field name you want the values of
        :returns: generator, the field_name values
        """
        it = self.copy()
        it.query.fields_select.clear()
        it.query.select_field(field_name)
        return it
        #return (getattr(o, k) for o in self)

    def __repr__(self):
        format_str = "[ ... {} ... ]"
        format_args = [self.__class__.__name__]

        orm_class = self.orm_class
        if orm_class:
            format_str = "[ ... {} on {} ... ]"
            format_args.append(orm_class.__name__)

        return format_str.format(*format_args)

    def ifilter(self, o):
        """run o through the filter, if True then orm o should be included

        NOTE -- The ifilter callback needs to account for non Orm instance values of o

        :param o: Orm|mixed, usually an Orm instance but can also be a tuple or single value
        :returns: boolean, True if o should be filtered
        """
        return o

    def hydrate(self, d):
        """Prepare the raw dict d returned from the interface cursor to be returned
        by higher level objects, this will usually mean hydrating an Orm instance
        or stuff like that

        :param d: dict, the raw dict cursor result returned from the interface
        :returns: mixed, usually an Orm instance populated with d but can also be
            a tuple if the query selected more than one field. If the query selected
            one field then just that value will be returned
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


class Bounds(object):
    @property
    def limit(self):
        l = self._limit
        #l = self.limit_paginate if self.paginate else self._limit
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
        """Returns True if the current bounds are set up to query one extra row in order
        to make pagination easier (to know if there should be a next link)

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
        """Given an index i, use the current offset and limit to find the correct
        offset i would be

        :param i: int, the index used to find the new offset
        :returns: int, the new offset
        """
        if i >= 0:
            offset = self.offset + i
            if self.has_limit():
                maximum_offset = self.limit + self.offset
                if offset > maximum_offset:
                    raise IndexError("Iterator index {} out of range".format(i))

        else:
            limit = self.limit
            offset = limit + i

            if offset < 0:
                raise IndexError("Iterator index {} out of range".format(i))

        return offset

    def find_more_index(self):
        #return self.offset + self.limit_paginate
        return self.offset + self.limit


class Field(object):
    @property
    def schema(self):
        return self.query.schema if self.query else None

    def __init__(self, query, field_name, field_val=None, **kwargs):
        self.query = query
        self.operator = kwargs.pop("operator", None)
        self.is_list = kwargs.pop("is_list", False)
        self.direction = kwargs.pop("direction", None) # 1 = ASC, -1 = DESC
        self.or_clause = False
        self.kwargs = kwargs

        self.set_name(field_name)
        self.set_value(field_val)

    def set_name(self, field_name):
        field_name, function_name = self.parse(field_name, self.schema)
        self.function_name = function_name
        self.name = field_name

    def set_value(self, field_val):
        if not isinstance(field_val, Query):
            if self.is_list:
                if field_val:
                    field_val = make_list(field_val)
                    for i in range(len(field_val)):
                        field_val[i] = self.iquery(field_val[i])

            else:
                field_val = self.iquery(field_val)

        self.value = field_val

    def iquery(self, field_val):
        query = self.query
        schema = self.schema
        if query and schema:
            schema_field = getattr(schema, self.name)
            field_val = schema_field.iquery(query, field_val)
        return field_val

    def parse(self, field_name, schema):
        function_name = ""
        if schema:
            function_name = ""
            m = re.match(r"^([^\(]+)\(([^\)]+)\)$", field_name)
            if m:
                function_name = m.group(1)
                field_name = m.group(2)

            field_name = schema.field_name(field_name)

        return field_name, function_name


class Fields(list):
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
        super(Fields, self).append(field)
        self.field_names[field.name].append(index)

    def __contains__(self, field_name):
        return field_name in self.field_names

    def __setitem__(self, *args, **kwargs):
        raise NotImplementedError()
    __delitem__ = __setitem__

    def clear(self):
        self.field_names = defaultdict(list)
        self.options = {}
        self[:]


class Query(object):
    """
    Handle standard query creation and allow interface querying

    :example:
        q = Query(orm_class)
        q.eq_foo(1).desc_bar().limit(10).page(2).get()
        # SELECT * FROM <orm_class.table_name> WHERE foo = 1 ORDER BY bar DESC LIMIT 10 OFFSET 20
    """
    field_class = Field
    fields_set_class = Fields
    fields_select_class = Fields
    fields_where_class = Fields
    fields_sort_class = Fields
    bounds_class = Bounds
    iterator_class = Iterator

    @property
    def interface(self):
        return self.orm_class.interface if self.orm_class else None

    @property
    def schema(self):
        if not self.orm_class: return None
        return self.orm_class.schema

    @property
    def schemas(self):
        """Find and return all the schemas that are needed for this query to complete
        successfully

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
        """Wraps left and right field statements with an OR clause, you can chain
        as many OR calls as you want to create an any length OR clause

        I don't love that I had to use OR instead of or but "or" is a reserved
        keyword and I thought OR was better than like _or or _or_

        :Example:
            self.eq_foo(1).OR.eq_foo(5).OR.eq_foo(10) # (foo=1 OR foo=5 OR foo=10)
        """
        self.fields_where[-1].or_clause = True
        return self

    @property
    def AND(self):
        """This is just here for completeness with .OR since, by default, any
        statements will be joined by AND"""
        self.fields_where[-1].or_clause = False
        return self

    def __init__(self, orm_class=None, **kwargs):
        # needed to use the db querying methods like get(), if you just want to build
        # a query then you don't need to bother passing this in
        self.orm_class = orm_class
        self.reset()

    def reset(self):
        #self.interface = None
        self._ifilter = None
        self.fields_set = self.fields_set_class()
        self.fields_select = self.fields_select_class()
        self.fields_where = self.fields_where_class()
        self.fields_sort = self.fields_sort_class()
        self.bounds = self.bounds_class()
        self.compounds = []

    def ref(self, orm_classpath):
        """
        takes a classpath to allow query-ing from another Orm class

        the reason why it takes string paths is to avoid infinite recursion import 
        problems because an orm class from module A might have a ref from module B
        and sometimes it is handy to have module B be able to get the objects from
        module A that correspond to the object in module B, but you can't import
        module A into module B because module B already imports module A.

        :param orm_classpath: string|type, a full python class path (eg, foo.bar.Che) or
            an actual model.Orm python class
        return -- Query()
        """
        # split orm from module path
        if isinstance(orm_classpath, basestring):
            orm_module, orm_class = get_objects(orm_classpath)
        else:
            orm_class = orm_classpath

        return orm_class.query

    def __iter__(self):
        return self.get()

    def find_methods(self, method_name):
        """Given a method name like <OPERATOR>_<FIELD_NAME> or <FIELD_NAME>_<OPERATOR>,
        split those into <OPERATOR> and <FIELD_NAME> if there is an existing
        <OPERATOR>_field method that exists

        :example:
            self.gt_foo() # (<gt_field>, None, "foo")
            self.one_pk(value) # (<eq_field>, <one>, "pk")

        :returns: tuple, (<FIELD_METHOD>, <QUERY_METHOD>, <FIELD_NAME>)
        """
        # infinite recursion check, if a *_field method gets in here then it
        # doesn't exist
        if method_name.endswith("_field"):
            raise AttributeError(method_name)

        try:
            # check for <NAME>_<FIELD_NAME>
            name, field_name = method_name.split("_", 1)

        except ValueError:
            raise AttributeError("invalid potential method: {}".format(method_name))

        else:

            if not name:
                raise AttributeError('Could not resolve methods from {}"'.format(
                    method_name
                ))

            elif name in {"one", "value"}:
                field_method = self.eq_field
                query_method = getattr(self, name)

            elif name in {"get", "values", "all", "cursor"}:
                field_method = self.in_field
                query_method = getattr(self, name)

            else:
                query_method = None
                field_method_name = "{}_field".format(name)
                field_method = getattr(self, field_method_name, None)
                if not field_method:
                    # let's try reversing the split, so <FIELD_NAME>_<NAME>
                    field_name, name = method_name.rsplit("_", 1)
                    field_method_name = "{}_field".format(name)
                    field_method = getattr(self, field_method_name, None)

                if not field_method:
                    raise AttributeError('No field method derived from {}'.format(
                        method_name
                    ))

        return field_method, query_method, field_name

    def __getattr__(self, method_name):
        field_method, query_method, field_name = self.find_methods(method_name)

        def callback(*args, **kwargs):
            #pout.v(args, kwargs, field_method, query_method, field_name)
            if field_method:
                ret = field_method(field_name, *args, **kwargs)

            if query_method:
                ret = query_method()

            return ret

        return callback

    def create_field(self, field_name, field_val=None, **kwargs):
        f = self.field_class(self, field_name, field_val, **kwargs)
        return f

    def create_iterator(self, query):
        return self.orm_class.iterator_class(query) if self.orm_class else self.iterator_class(query)

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
        kwargs["operator"] = operator
        f = self.create_field(field_name, field_val, **kwargs)
        self.fields_where.append(f)
        return self

    def append_sort(self, direction, field_name, field_val=None, **kwargs):
        """
        sort this query by field_name in directrion

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
        f = self.create_field(field_name, field_val, **kwargs)
        self.fields_sort.append(f)
        return self

    def distinct(self, *field_names):
        self.fields_select.options["distinct"] = True
        return self.select(*field_names)

    def select_field(self, field_name):
        """set a field to be selected, this is automatically called when you do select_FIELDNAME(...)"""
        if field_name == "*":
            self.fields_select.options["all"] = True
        else:
            field = self.create_field(field_name)
            self.fields_select.append(field)
        return self

    def select(self, *field_names):
        """set multiple fields to be selected"""
        for field_name in make_list(field_names):
            self.select_field(field_name)
        return self

    def set_field(self, field_name, field_val):
        """
        set a field into .fields_set attribute

        In insert/update queries, these are the fields that will be inserted/updated into the db
        """
        field = self.create_field(field_name, field_val)
        self.fields_set.append(field)
        return self

    def set(self, fields=None, **fields_kwargs):
        """
        completely replaces the current .fields with fields and fields_kwargs combined
        """
        fields = make_dict(fields, fields_kwargs)
        for field_name, field_val in fields.items():
            self.set_field(field_name, field_val)
        return self

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

        This is named .difference() instead of .except() because except is a python
        keyword.

        See .intersect() since this works very similar
        """
        return self.append_compound("except", queries, **kwargs)

    def is_field(self, field_name, field_val=None, **field_kwargs):
        return self.append_operation("eq", field_name, field_val, **field_kwargs)
    def eq_field(self, field_name, field_val=None, **field_kwargs):
        return self.is_field(field_name, field_val, **field_kwargs)

    def not_field(self, field_name, field_val=None, **field_kwargs):
        return self.append_operation("ne", field_name, field_val, **field_kwargs)
    def ne_field(self, field_name, field_val=None, **field_kwargs):
        return self.not_field(field_name, field_val, **field_kwargs)

    def between_field(self, field_name, low, high):
        self.gte_field(field_name, low)
        self.lte_field(field_name, high)
        return self

    def lte_field(self, field_name, field_val=None, **field_kwargs):
        return self.append_operation("lte", field_name, field_val, **field_kwargs)

    def lt_field(self, field_name, field_val=None, **field_kwargs):
        return self.append_operation("lt", field_name, field_val, **field_kwargs)

    def gte_field(self, field_name, field_val=None, **field_kwargs):
        return self.append_operation("gte", field_name, field_val, **field_kwargs)

    def gt_field(self, field_name, field_val=None, **field_kwargs):
        return self.append_operation("gt", field_name, field_val, **field_kwargs)

    def in_field(self, field_name, field_val=None, **field_kwargs):
        """
        :param field_val: list, a list of field_val values
        """
        field_kwargs["is_list"] = True
        return self.append_operation("in", field_name, field_val, **field_kwargs)

    def nin_field(self, field_name, field_val=None, **field_kwargs):
        """
        :param field_val: list, a list of field_val values
        """
        field_kwargs["is_list"] = True
        return self.append_operation("nin", field_name, field_val, **field_kwargs)

    def startswith_field(self, field_name, field_val, **field_kwargs):
        return self.like_field(field_name, "{}%".format(field_val), **field_kwargs)

    def endswith_field(self, field_name, field_val, **field_kwargs):
        return self.like_field(field_name, "%{}".format(field_val), **field_kwargs)

    def contains_field(self, field_name, field_val, **field_kwargs):
        return self.like_field(field_name, "%{}%".format(field_val), **field_kwargs)

    def like_field(self, field_name, field_val, **field_kwargs):
        """Perform a field_name LIKE field_val query

        :param field_name: string, the field we are filtering on
        :param field_val: string, the like query: %val, %val%, val%
        :returns: self, for fluid interface
        """
        if not field_val:
            raise ValueError("Cannot LIKE nothing")
        return self.append_operation("like", field_name, field_val, **field_kwargs)

    def nlike_field(self, field_name, field_val, **field_kwargs):
        """Perform a field_name NOT LIKE field_val query

        :param field_name: string, the field we are filtering on
        :param field_val: string, the like query: %val, %val%, val%
        :returns: self, for fluid interface
        """
        if not field_val:
            raise ValueError("Cannot NOT LIKE nothing")
        return self.append_operation("nlike", field_name, field_val, **field_kwargs)

    def asc(self, *field_names):
        for field_name in field_names:
            self.asc_field(field_name)
        return self

    def asc_field(self, field_name, field_val=None):
        return self.append_sort(1, field_name, field_val)

    def desc(self, *field_names):
        for field_name in field_names:
            self.desc_field(field_name)
        return self

    def desc_field(self, field_name, field_val=None):
        return self.append_sort(-1, field_name, field_val)

    def ifilter(self, predicate):
        """Set the predicate (callback) for an iterator returned from this instance

        https://docs.python.org/2/library/itertools.html#itertools.ifilter

        :param predicate: callable, this will be passed to the iterator in the
            .create_iterator method
        :returns: self, for fluid interface
        """
        self._ifilter = predicate
        return self

    def filter(self, predicate):
        return self.ifilter(predicate)

    def limit(self, limit):
        self.bounds.limit = limit
        return self

    def offset(self, offset):
        self.bounds.offset = offset
        return self

    def page(self, page):
        self.bounds.page = page
        return self

    def cursor(self):
        """Used by the Iterator to actually query the db"""
        return self.execute('get', cursor_result=True)

    def get(self):
        """
        get results from the db

        :returns: Iterator
        """
        self.bounds.paginate = True
        return self.create_iterator(self)

    def all(self):
        return self.get()

    def values(self):
        if not self.fields_select:
            raise ValueError("No selected fields")
        return self.get()

    def one(self):
        """get one row from the db"""
        self.limit(1)
        self.bounds.paginate = False
        it = self.create_iterator(self)
        try:
            ret = it.next()
        except StopIteration:
            ret = None
        return ret

    def value(self):
        """convenience method to just get one value or tuple of values for the query"""
        if not self.fields_select:
            raise ValueError("no selected fields")
        return self.one()

    def count(self):
        """return the count of the criteria"""

        # sorting shouldn't matter for a count query
        fields_sort = self.fields_sort
        self.fields_sort = self.fields_sort_class()

        # more than one selected field will cause the count query to error out
        fields_select = self.fields_select
        self.fields_select = self.fields_select_class()

        # setting bounds causes count(*) to return 0 in both Postgres and SQLite
        bounds = self.bounds
        self.bounds = self.bounds_class()

        ret = self.execute('count')

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

    def has(self):
        """returns true if there is atleast one row in the db matching the query, False otherwise"""
        v = self.one()
        return True if v else False

    def exists(self):
        return self.has()

    def insert(self, **kwargs):
        """persist the .fields"""
        return self.interface.insert(self.schema, self.fields_set.fields, **kwargs)

    def update(self, **kwargs):
        """persist the .fields using .fields_where"""
        return self.interface.update(
            self.schema,
            self.fields_set.fields,
            self,
            **kwargs
        )

    def upsert(self, conflict_field_names=None, **kwargs):
        """persist the .fields"""
        insert_fields = self.fields_set.fields
        update_fields = dict(insert_fields)

        if not conflict_field_names:
            conflict_field_names = self.schema.pk_names

        for field_name in conflict_field_names:
            update_fields.pop(field_name, None)

        return self.interface.upsert(
            self.schema,
            insert_fields,
            update_fields,
            conflict_field_names,
            **kwargs
        )

    def delete(self, **kwargs):
        """remove fields matching the where criteria"""
        return self.execute('delete', **kwargs)

    def render(self, **kwargs):
        """Render the query

        :returns: string, the rendered query, this is not assured to be a valid query
            but is handy for quickly debugging what the query roughly looks like
        """
        return self.interface.render(self.schema, self, **kwargs)

    def raw(self, query_str, *query_args, **query_options):
        """
        use the interface.query() method to pass in your own raw query without
        any processing

        NOTE -- This will allow you to make any raw query and will usually return
        raw results, it won't wrap those results in a prom.Orm iterator instance
        like other methods like .all() and .get()

        query_str -- string -- the raw query for whatever the backend interface is
        query_args -- list -- if you have named parameters, you can pass in the values
        **query_options -- dict -- key:val options for the backend, these are very backend specific
        return -- mixed -- depends on the backend and the type of query
        """
        i = self.interface
        return i.raw(query_str, *query_args, **query_options)

    def execute(self, method_name, **kwargs):
        i = self.interface
        s = self.schema
        return getattr(i, method_name)(s, self, **kwargs)

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
        try:
            return self.render()

        except AttributeError:
            return super().__str__()

