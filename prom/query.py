# -*- coding: utf-8 -*-
"""
Classes and stuff that handle querying the interface for a passed in Orm class
"""
from __future__ import unicode_literals, division, print_function, absolute_import
import copy
from collections import defaultdict, Mapping, OrderedDict
import datetime
import logging
import os
from contextlib import contextmanager
import math
import inspect
import time
import re
import thread

from decorators import deprecated

from . import decorators
from .utils import make_list, get_objects, make_dict, make_hash
from .interface import get_interfaces
from .compat import *


logger = logging.getLogger(__name__)


class BaseIterator(object):
    """The base interface for the iterators

    it acts as much like a list as possible to make using it as seemless as can be

    http://docs.python.org/2/library/stdtypes.html#iterator-types
    """
    def reset(self):
        raise NotImplementedError()

    def next(self):
        raise NotImplementedError()

    def __next__(self):
        """needed for py3 api compatibility"""
        return self.next()

    def values(self):
        """
        similar to the dict.values() method, this will only return the selected fields
        in a tuple

        return -- self -- each iteration will return just the field values in
            the order they were selected, if you only selected one field, than just that field
            will be returned, if you selected multiple fields than a tuple of the fields in
            the order you selected them will be returned
        """
        raise NotImplementedError()

    def __iter__(self):
        self.reset()
        return self

    def __nonzero__(self):
        return True if self.count() else False

    def __len__(self):
        return self.count()

    def count(self):
        """list interface compatibility"""
        raise NotImplementedError()

    def __getitem__(self, k):
        raise NotImplementedError()

    def pop(self, k=-1):
        """list interface compatibility"""
        raise NotImplementedError()

    def reverse(self):
        """list interface compatibility"""
        raise NotImplementedError()

    def __reversed__(self):
        self.reverse()
        return self

    def sort(self, *args, **kwargs):
        """list interface compatibility"""
        raise NotImplementedError()

    def __getattr__(self, k):
        """
        this allows you to focus in on certain fields of results

        It's just an easier way of doing: (getattr(x, k, None) for x in self)
        """
        raise NotImplementedError()

    def create_generator(self):
        """put all the pieces together to build a generator of the results"""
        raise NotImplementedError()

    def _get_result(self, d):
        raise NotImplementedError()


class Iterator(BaseIterator):
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
    def __init__(self, results):
        """
        restults -- BaseIterator|AllIterator -- this wraps another iterator and 
        adds filtering capabilities, this is here to allow the Query all() and get()
        methods to return the same base class so it can be extended

        see -- https://github.com/firstopinion/prom/issues/25
        """
        self.results = results
        self.ifilter = None # https://docs.python.org/2/library/itertools.html#itertools.ifilter
        self.reset()

    def reset(self):
        self.results.reset()

    def next(self):
        o = self.results.next()
        while not self._filtered(o):
            o = self.results.next()
        return o

    def values(self):
        return self.results.values()

    def count(self):
        return self.results.count()

    def __getitem__(self, k):
        return self.results[k]

    def pop(self, k=-1):
        return self.results.pop(k)

    def reverse(self):
        return self.results.reverse()

    def sort(self, *args, **kwargs):
        return self.results.sort()

    def __getattr__(self, k):
        return getattr(self.results, k)

    def _filtered(self, o):
        """run orm o through the filter, if True then orm o should be included"""
        return self.ifilter(o) if self.ifilter else True


class ResultsIterator(BaseIterator):
    """
    smartly iterate through a result set

    this is returned from the Query.get() and it acts as much
    like a list as possible to make using it as seemless as can be

    fields --
        has_more -- boolean -- True if there are more results in the db, false otherwise

    examples --
        # iterate through all the primary keys of some orm
        for pk in SomeOrm.query.get().pk:
            print pk
    """
    def __init__(self, results, orm_class=None, has_more=False, query=None):
        """
        create a result set iterator

        results -- list -- the list of results
        orm_class -- Orm -- the Orm class that each row in results should be wrapped with
        has_more -- boolean -- True if there are more results
        query -- Query -- the query instance that produced this iterator
        """
        self.results = results
        self.orm_class = orm_class
        self.has_more = has_more
        self.query = query.copy()
        self._values = False
        self.reset()

    def reset(self):
        self.iresults = self.create_generator()

    def next(self):
        if is_py2:
            return self.iresults.next()
        else:
            return self.iresults.__next__()

    def values(self):
        self._values = True
        self.field_names = self.query.fields_select.names()
        self.fcount = len(self.field_names)
        if not self.fcount:
            raise ValueError("no select fields were set, so cannot iterate values")

        return self

    def __iter__(self):
        self.reset()
        return self

    def __nonzero__(self):
        return True if self.count() else False

    def __len__(self):
        return self.count()

    def count(self):
        return len(self.results)

    def __getitem__(self, k):
        k = int(k)
        return self._get_result(self.results[k])

    def pop(self, k=-1):
        k = int(k)
        return self._get_result(self.results.pop(k))

    def reverse(self):
        self.results.reverse()
        self.reset()

    def __reversed__(self):
        self.reverse()
        return self

    def sort(self, *args, **kwargs):
        self.results.sort(*args, **kwargs)
        self.reset()

    def __getattr__(self, k):
        field_name = self.orm_class.schema.field_name(k)
        return (getattr(r, field_name, None) for r in self)

    def create_generator(self):
        """put all the pieces together to build a generator of the results"""
        return (self._get_result(d) for d in self.results)

    def _get_result(self, d):
        r = None
        if self._values:
            field_vals = [d.get(fn, None) for fn in self.field_names]
            r = field_vals if self.fcount > 1 else field_vals[0]

        else:
            if self.orm_class:
                r = self.orm_class.populated(d)
            else:
                r = d

        return r


class CursorIterator(ResultsIterator):
    """This is the iterator that query.cursor() uses, it is a subset of the
    functionality of the ResultsIterator but allows you to move through huge
    result sets"""
    def count(self):
        return self.results.rowcount

    def __getitem__(self, k):
        raise NotImplementedError()

    def pop(self, k=-1):
        raise NotImplementedError()

    def reverse(self):
        raise NotImplementedError()

    def sort(self, *args, **kwargs):
        raise NotImplementedError()


class AllIterator(ResultsIterator):
    """
    Similar to Iterator, but will chunk up results and make another query for the next
    chunk of results until there are no more results of the passed in Query(), so you
    can just iterate through every row of the db without worrying about pulling too
    many rows at one time
    """
    def __init__(self, query, chunk_limit=5000):

        # decide how many results we are going to iterate through
        limit, offset = query.bounds.get()
        if not limit: limit = 0
        if limit and limit < chunk_limit:
            chunk_limit = limit

        self.chunk_limit = chunk_limit
        self.limit = limit
        self.offset = offset
        self._iter_count = 0 # internal counter of how many rows iterated

        super(AllIterator, self).__init__(results=[], orm_class=query.orm_class, query=query)

    def __getitem__(self, k):
        v = None
        k = int(k)
        lower_bound = self.offset
        upper_bound = lower_bound + self.chunk_limit
        if k >= lower_bound and k < upper_bound:
            # k should be in this result set
            i = k - lower_bound
            v = self.results[i]

        else:
            limit = self.limit
            if not limit or k < limit:
                # k is not in here, so let's just grab it
                q = self.query.copy()
                orm = q.set_offset(k).get_one()
                if orm:
                    v = self._get_result(orm.fields)
                else:
                    raise IndexError("results index out of range")

            else:
                raise IndexError("results index {} out of limit {} range".format(k, limit))

        return v

    def pop(self, k=-1):
        raise NotImplementedError("{}.pop() is not supported".format(self.__class__.__name__))

    def count(self):
        ret = 0
        if self.results.has_more:
            # we need to do a count query
            q = self.query.copy()
            q.limit(0).offset(0)
            ret = q.count()
        else:
            ret = (self.offset - self.start_offset) + len(self.results)

        return ret

    def next(self):
        if self.limit and (self._iter_count >= self.limit):
            raise StopIteration("iteration exceeded limit")

        try:
            ret = self.results.next()
            self._iter_count += 1

        except StopIteration:
            if self.results.has_more:
                self.offset += self.chunk_limit
                self._set_results()
                ret = self.next()
            else:
                raise

        return ret

    def _set_results(self):
        self.results = self.query.offset(self.offset).limit(self.chunk_limit).get()
        if self._values:
            self.results = self.results.values()

    def reset(self):
        set_results = False
        if hasattr(self, 'start_offset'):
            set_results = self.offset != self.start_offset
        else:
            self.start_offset = self.offset
            set_results = True

        if set_results:
            self.offset = self.start_offset
            self._set_results()

        else:
            self.results.reset()

    def values(self):
        self.results = self.results.values()
        return super(AllIterator, self).values()







class Bounds(object):

    @property
    def limit(self):
        l = self.limit_paginate if self.paginate else self._limit
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

    def __init__(self):
        self.paginate = False
        self._limit = None
        self._offset = None
        self._page = None

    def set(self, limit=None, page=None):
        if limit is not None:
            self.limit = limit
        if page is not None:
            self.page = page

    def get(self, limit=None, page=None):
        self.set(limit, page)
        return (self.limit, self.offset)

    def __bool__(self):
        return self.limit > 0 or self.offset > 0
    __nonzero__ = __bool__ # py2

    def has(self):
        return bool(self)

    def has_limit(self):
        return self.limit > 0

    def __str__(self):
        return "limit: {}, offset: {}".format(self.limit, self.offset)






class Field(object):
    @property
    def schema(self):
        return self.query.schema if self.query else None

    def __init__(self, query, field_name, field_val=None, **kwargs):
        self.query = query
        self.operator = kwargs.pop("operator", None)
        self.is_list = kwargs.pop("is_list", False)
        self.direction = kwargs.pop("direction", None)
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
        self.field_names = defaultdict(list)
        self.options = {}

    def names(self):
        return self.field_names.keys()

    def append(self, field):
        index = len(self)
        super(Fields, self).append(field)
        self.field_names[field.name].append(index)

    def __contains__(self, field_name):
        return field_name in self.field_names

    def __setitem__(self, *args, **kwargs):
        raise NotImplementedError()
    __delitem__ = __setitem__




class Query(object):
    """
    Handle standard query creation and allow interface querying

    example --
        q = Query(orm_class)
        q.is_foo(1).desc_bar().limit(10).page(2).get()
    """
    field_class = Field
    fields_set_class = Fields
    fields_select_class = Fields
    fields_where_class = Fields
    fields_sort_class = Fields
    bounds_class = Bounds

    @property
    def interface(self):
        if not self.orm_class: return None
        interface = getattr(self, "_interface", None)
        if not interface:
            interface = self.orm_class.interface
            self._interface = interface
        return interface

    @interface.setter
    def interface(self, interface):
        self._interface = interface

    @interface.deleter
    def interface(self):
        try:
            del self._interface
        except AttributeError:
            pass

    @property
    def schema(self):
        if not self.orm_class: return None
        return self.orm_class.schema

    @property
    def iterator_class(self):
        if not self.orm_class: return None
        return self.orm_class.iterator_class if self.orm_class else Iterator

    def __init__(self, orm_class=None, **kwargs):

        # needed to use the db querying methods like get(), if you just want to build
        # a query then you don't need to bother passing this in
        self.orm_class = orm_class
        self.reset()
        self.kwargs = kwargs

    def reset(self):
        self.interface = None
        self.fields_set = self.fields_set_class()
        self.fields_select = self.fields_select_class()
        self.fields_where = self.fields_where_class()
        self.fields_sort = self.fields_sort_class()
        self.bounds = self.bounds_class()
        # the idea here is to set this to False if there is a condition that will
        # automatically cause the query to fail but not necessarily be an error, 
        # the best example is the IN (...) queries, if you do self.in_foo([]).get()
        # that will fail because the list was empty, but a value error shouldn't
        # be raised because a common case is: self.if_foo(Bar.query.is_che(True).pks).get()
        # which should result in an empty set if there are no rows where che = TRUE
        self.can_get = True

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
        #return self.all()
        #return self.get()
        # NOTE -- for some reason I need to call AllIterator.__iter__() explicitely
        # because it would call AllIterator.next() even though AllIterator.__iter__
        # returns a generator, not sure what's up
        return self.get() if self.bounds else self.all().__iter__()

    def find_operation_method(self, method_name):
        """Given a method name like <OPERATOR>_<FIELD_NAME> or <FIELD_NAME>_<OPERATOR>,
        split those into <OPERATOR> and <FIELD_NAME> if there is an existing
        <OPERATOR>_field method that exists

        So, for example, gt_foo(<VALUE>) would be split to gt <OPERATOR> and foo
        <FIELD_NAME> so self.gt_field("foo", ...) could be called

        :returns: tuple, (<OPERATOR>, <FIELD_NAME>)
        """
        # infinite recursion check, if a *_field method gets in here then it
        # doesn't exist
        if method_name.endswith("_field"):
            raise AttributeError(method_name)

        try:
            # check for <OPERATOR>_<FIELD_NAME>
            operator, field_name = method_name.split("_", 1)

        except ValueError:
            raise AttributeError("invalid operator method: {}".format(method_name))

        else:
            if not operator:
                raise AttributeError('Could not derive command from {}"'.format(
                    method_name
                ))

            operator_method_name = "{}_field".format(operator)
            operator_method = getattr(self, operator_method_name, None)
            if not operator_method:
                # let's try reversing the split, so <FIELD_NAME>_<OPERATOR>
                field_name, operator = method_name.rsplit("_", 1)
                operator_method_name = "{}_field".format(operator)
                operator_method = getattr(self, operator_method_name, None)

            if not operator_method:
                raise AttributeError('No "{}" method derived from "{}"'.format(
                    operator_method_name,
                    method_name
                ))

        return operator_method, field_name

    def create_field(self, field_name, field_val=None, **kwargs):
        f = self.field_class(self, field_name, field_val, **kwargs)
        return f

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

    def is_field(self, field_name, field_val=None, **field_kwargs):
        return self.append_operation("eq", field_name, field_val, **field_kwargs)
    def eq_field(self, field_name, field_val, **field_kwargs):
        return self.is_field(field_name, field_val, **field_kwargs)

    def not_field(self, field_name, field_val=None, **field_kwargs):
        return self.append_operation("ne", field_name, field_val, **field_kwargs)
    def ne_field(self, field_name, field_val, **field_kwargs):
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
        if not field_val and not field_kwargs:
            self.can_get = False

        field_kwargs["is_list"] = True
        return self.append_operation("in", field_name, field_val, **field_kwargs)

    def nin_field(self, field_name, field_val=None, **field_kwargs):
        """
        :param field_val: list, a list of field_val values
        """
        if not field_val:
            self.can_get = False

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

    def __getattr__(self, method_name):
        field_method, field_name = self.find_operation_method(method_name)
        def callback(*args, **kwargs):
            #pout.v(args, kwargs, field_method, field_name)
            return field_method(field_name, *args, **kwargs)
        return callback

    def limit(self, limit):
        self.bounds.limit = limit
        return self

    def offset(self, offset):
        self.bounds.offset = offset
        return self

    def page(self, page):
        self.bounds.page = page
        return self

    def cursor(self, limit=None, page=None):
        # TODO -- combine the common parts of this method and get()
        has_more = False
        self.bounds.paginate = True
        limit_paginate, offset = self.bounds.get(limit, page)
        self.default_val = []
        results = self._query('get', cursor_result=True)

        if limit_paginate:
            self.bounds.paginate = False
            if results.rowcount == limit_paginate:
                has_more = True
                # TODO -- we need to compensate for having one extra
                #results.pop(-1)

        it = CursorIterator(results, orm_class=self.orm_class, has_more=has_more, query=self)
        return self.iterator_class(it)

    def get(self, limit=None, page=None):
        """
        get results from the db

        return -- Iterator()
        """
        has_more = False
        self.bounds.paginate = True
        limit_paginate, offset = self.bounds.get(limit, page)
        self.default_val = []
        results = self._query('get')

        if limit_paginate:
            self.bounds.paginate = False
            if len(results) == limit_paginate:
                has_more = True
                results.pop(-1)

        it = ResultsIterator(results, orm_class=self.orm_class, has_more=has_more, query=self)
        return self.iterator_class(it)

    def all(self):
        """
        return every possible result for this query

        This is smart about returning results and will use the set limit (or a default if no
        limit was set) to chunk up the results, this means you can work your way through
        really big result sets without running out of memory

        return -- Iterator()
        """
        ait = AllIterator(self)
        return self.iterator_class(ait)

    def one(self): return self.get_one()
    def get_one(self):
        """get one row from the db"""
        self.default_val = None
        o = self.default_val
        d = self._query('get_one')
        if d:
            o = self.orm_class.populated(d)
        return o

    @deprecated("see list item 1 in issue 24")
    def values(self, limit=None, page=None):
        """
        convenience method to get just the values from the query (same as get().values())

        if you want to get all values, you can use: self.all().values()
        """
        return self.get(limit=limit, page=page).values()

    @deprecated("see list item 1 in issue 24")
    def value(self):
        """convenience method to just get one value or tuple of values for the query"""
        field_vals = None
        field_names = self.fields_select.names()
        fcount = len(field_names)
        if fcount:
            d = self._query('get_one')
            if d:
                field_vals = [d.get(fn, None) for fn in field_names]
                if fcount == 1:
                    field_vals = field_vals[0]

        else:
            raise ValueError("no select fields were set, so cannot return value")

        return field_vals

    @deprecated("see list item 1 in issue 24, and I don't think this is used enough to be officially supported")
    def pks(self, limit=None, page=None):
        """convenience method for setting select_pk().values() since this is so common"""
        #self.fields_set.reset()
        return self.select_pk().values(limit, page)

    @deprecated("see list item 1 in issue 24, and I don't think this is used enough to be officially supported")
    def pk(self):
        """convenience method for setting select_pk().value() since this is so common"""
        #self.fields_set.reset()
        return self.select_pk().value()

    @deprecated("see issue 112")
    def get_pks(self, field_vals):
        """convenience method for running in__id([...]).get() since this is so common"""
        field_name = self.schema.pk.name
        return self.in_field(field_name, field_vals).get()

    @deprecated("see issue 112")
    def get_pk(self, field_val):
        """convenience method for running is_pk(_id).get_one() since this is so common"""
        field_name = self.schema.pk.name
        return self.is_field(field_name, field_val).get_one()

    def first(self):
        """convenience method for running asc__id().get_one()"""
        return self.asc__id().get_one()

    def last(self):
        """convenience method for running desc__id().get_one()"""
        return self.desc__id().get_one()

    def count(self):
        """return the count of the criteria"""

        # count queries shouldn't care about sorting
        fields_sort = self.fields_sort
        self.fields_sort = self.fields_sort_class()

        self.default_val = 0
        ret = self._query('count')

        # restore previous values now that count is done
        self.fields_sort = fields_sort

        return ret

    def has(self):
        """returns true if there is atleast one row in the db matching the query, False otherwise"""
        v = self.get_one()
        return True if v else False

    def insert(self):
        """persist the .fields"""
        self.default_val = 0
        return self.interface.insert(self.schema, self.fields_set.fields)

    def update(self):
        """persist the .fields using .fields_where"""
        self.default_val = 0
        #fields = self.fields
        #fields = self.orm_class.depart(self.fields, is_update=True)
        #self.set_fields(fields)
        return self.interface.update(
            self.schema,
            self.fields_set.fields,
            self
        )
        #return self._query('update')

    def delete(self):
        """remove fields matching the where criteria"""
        self.default_val = None
        return self._query('delete')

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
        return i.query(query_str, *query_args, **query_options)

    def _query(self, method_name, **kwargs):
        if not self.can_get: return self.default_val
        i = self.interface
        s = self.schema
        return getattr(i, method_name)(s, self, **kwargs) # i.method_name(schema, query)

    def copy(self):
        """nice handy wrapper around the deepcopy"""
        return copy.deepcopy(self)

    def __deepcopy__(self, memodict={}):
        instance = type(self)(self.orm_class)
        ignore_keys = set(["_interface"])
        for key, val in self.__dict__.items():
            if key not in ignore_keys:
                setattr(instance, key, copy.deepcopy(val, memodict))
        return instance

    def __unicode__(self):
        return self.render()

    def __str__(self):
        ret = self.__unicode__()
        return ByteString(ret) if is_py2 else ret


class BaseCacheQuery(Query):
    """a standard query caching skeleton class with the idea that it would be expanded
    upon on a per project or per model basis

    by default, this is designed to call a caching method for certain queries, so
    if you only wanted to cache "get_one" type events, you could add a method
    to a child class like `cache_key_get_one` and this will call that method
    everytime a `get_one` method is invoked (this includes wrapper methods like
    `value`, `has`, and `first`).

    similar for delete, there are 3 deleting events, `insert`, `update`, and `delete`
    and so you can add a method like `cache_delete_update()` to only invalidate on
    updates but completely ignore update and delete events

    A child class will have to implement the methods that raise NotImplementedError
    in order to have a valid CacheQuery child
    """
    def cache_delete(self, method_name):
        method = getattr(self, "cache_delete_{}".format(method_name), None)
        if method:
            method()

    def cache_key(self, method_name):
        """decides if this query is cacheable, returns a key if it is, otherwise empty"""
        key = ""
        method = getattr(self, "cache_key_{}".format(method_name), None)
        if method:
            key = method()

        return key

    def cache_set(self, key, result):
        raise NotImplementedError()

    def cache_get(self, key):
        """must return a tuple (returned_value, cache_hit) where the returned value
        is what would be returned from the db and cache_hit is True if it was in
        the cache, otherwise False"""
        raise NotImplementedError()

    def _query(self, method_name):
        cache_hit = False
        cache_key = self.cache_key(method_name)
        table_name = str(self.schema)
        if cache_key:
            logger.debug("Cache check on {} for key {}".format(table_name, cache_key))
            result, cache_hit = self.cache_get(cache_key)

        if not cache_hit:
            logger.debug("Cache miss on {} for key {}".format(table_name, cache_key))
            result = super(BaseCacheQuery, self)._query(method_name)
            if cache_key:
                self.cache_set(cache_key, result)

        else:
            logger.debug("Cache hit on {} for key {}".format(table_name, cache_key))

        self.cache_hit = cache_hit
        return result

    def update(self):
        ret = super(BaseCacheQuery, self).update()
        if ret:
            logger.debug("Cache delete on {} update".format(self.schema))
            self.cache_delete("update")
        return ret

    def insert(self):
        ret = super(BaseCacheQuery, self).insert()
        if ret:
            logger.debug("Cache delete on {} insert".format(self.schema))
            self.cache_delete("insert")
        return ret

    def delete(self):
        ret = super(BaseCacheQuery, self).delete()
        if ret:
            logger.debug("Cache delete on {} delete".format(self.schema))
            self.cache_delete("delete")
        return ret


class CacheNamespace(defaultdict):
    """This is what actually does the memory processing caching of CacheQuery, it
    namespaces by process_id -> thread_id -> table, otherwise it is identical to
    any other default dict that sets default_factory to itself
    """

    @property
    def active(self):
        ret = False
        cn = self.get_process()
        if "active" in cn:
            ret = cn["active"]
        return ret

    @active.setter
    def active(self, v):
        cn = self.get_process()
        cn["active"] = bool(v)

    @active.deleter
    def active(self):
        self.get_process().pop("active", None)

    @property
    def ttl(self):
        """how long you should cache results for cacheable queries"""
        ret = 3600
        cn = self.get_process()
        if "ttl" in cn:
            ret = cn["ttl"]
        return ret

    @ttl.setter
    def ttl(self, ttl):
        cn = self.get_process()
        cn["ttl"] = int(ttl)

    @ttl.deleter
    def ttl(self):
        self.get_process().pop("ttl", None)

    @property
    def process_id(self):
        ret = ""
        if thread:
            f = getattr(os, 'getpid', None)
            if f:
                ret = str(f())
        return ret

    @property
    def thread_id(self):
        ret = ""
        if thread:
            ret = str(thread.get_ident())
        return ret

    def __init__(self):
        super(CacheNamespace, self).__init__(CacheNamespace)

    def get_process(self):
        return self[self.process_id][self.thread_id]

    def get_table(self, schema):
        return self.get_process()[str(schema)]


class CacheQuery(BaseCacheQuery):
    """a simple in-memory cache, ttls should be short since this has
    a very naive invalidation mechanism"""

    _cache_namespace = CacheNamespace()
    """store the cached values in memory"""

    @decorators.classproperty
    def cache_namespace(cls):
        return cls._cache_namespace

    @classmethod
    def cache_activate(cls, v):
        cls.cache_namespace.active = bool(v)

    @property
    def cache_table(self):
        return self.cache_namespace.get_table(self.schema)

    @classmethod
    @contextmanager
    def cache(cls, ttl=60):
        cn = cls.cache_namespace
        cn.ttl = ttl
        cls.cache_activate(True)

        try:
            yield cls

        finally:
            # cleanup
            cn.clear()
            cls.cache_activate(False)

    def cache_delete_update(self):
        self.cache_table.clear()

    def cache_delete_insert(self):
        self.cache_table.clear()

    def cache_delete_delete(self):
        self.cache_table.clear()

    def cache_hash(self, method_name):
        key = make_hash(
            method_name,
            self.fields_set,
            self.fields_where,
            self.fields_sort,
            self.bounds
        )
        return key

    def cache_key_get_one(self):
        return self.cache_hash("get_one")

    def cache_key_get(self):
        return self.cache_hash("get")

    def cache_key_count(self):
        return self.cache_hash("count")

    def cache_set(self, key, result):
        cn = self.cache_namespace
        now = datetime.datetime.utcnow()
        self.cache_table[key] = {
            "ttl": cn.ttl,
            "datetime": now,
            "result": result
        }

    def cache_get(self, key):
        result = None
        cache_hit = False
        ct = self.cache_table
        now = datetime.datetime.utcnow()
        if key in ct:
            val = ct[key]
            td = now - val["datetime"]
            if td.total_seconds() < val["ttl"]:
                cache_hit = True
                result = val["result"]

        return result, cache_hit

    def cache_key(self, method_name):
        ret = ""
        if self.cache_namespace.active:
            ret = super(CacheQuery, self).cache_key(method_name)
        return ret

