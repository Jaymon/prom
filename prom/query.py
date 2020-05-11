# -*- coding: utf-8 -*-
"""
Classes and stuff that handle querying the interface for a passed in Orm class
"""
from __future__ import unicode_literals, division, print_function, absolute_import
import copy
from collections import defaultdict, Mapping
import datetime
import logging
import os
from contextlib import contextmanager
import multiprocessing
from multiprocessing import queues
import math
import inspect
import time

import threading
try:
    import thread
except ImportError:
    thread = None

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


class Fields(object):
    def __init__(self):
        self.reset()

    def reset(self):
        self.fields = []
        self.fields_map = defaultdict(list)
        self.options = {}

    def append(self, field_name, field_args):
        index = len(self.fields)
        self.fields.append(field_args)
        self.fields_map[field_name].append(index)

    def __iter__(self):
        for field in self.fields:
            yield field

    def names(self):
        return [f[0] for f in self]

    def __nonzero__(self):
        return bool(self.fields)

    def __len__(self):
        return len(self.fields)

    def __getitem__(self, index):
        return self.fields[index]

    def has(self, field_name):
        return field_name in self.fields_map

    def __contains__(self, field_name):
        return self.has(field_name)

    def get(self, field_name):
        fields = []
        for index in self.fields_map.get(field_name, []):
            fields.append(self.fields[index])

        return fields

    def __str__(self):
        return "{}-{}".format(self.fields, self.options)


class FieldsWhere(Fields):
    """A wrapper around the Fields class that assures fields are only set once, unless
    they are less than or greater than settings"""
    def append(self, field_name, field_args):
        if field_name in self.fields_map:
            cmd_old = self.fields[self.fields_map[field_name][0]][0]
            cmd_new = field_args[0]
            if cmd_old and cmd_new:
                if not cmd_old.startswith("gt") and not cmd_old.startswith("lt"):
                    raise ValueError("Field {} has already been set".format(field_name))
                elif not cmd_new.startswith("gt") and not cmd_new.startswith("lt"):
                    raise ValueError("Field {} has already been set".format(field_name))

        return super(FieldsWhere, self).append(field_name, field_args)


class Limit(object):

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


class Query(object):
    """
    Handle standard query creation and allow interface querying

    example --
        q = Query(orm_class)
        q.is_foo(1).desc_bar().set_limit(10).set_page(2).get()
    """
    fields_set_class = Fields
    fields_where_class = FieldsWhere
    fields_sort_class = Fields
    bounds_class = Limit

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

    @property
    def fields(self):
        return dict(self.fields_set)

    @property
    def fields_select(self):
        return self.fields_set
        #return [select_field for select_field, _ in self.fields_set]

    def __init__(self, orm_class=None, *args, **kwargs):

        # needed to use the db querying methods like get(), if you just want to build
        # a query then you don't need to bother passing this in
        self.orm_class = orm_class
        self.reset()
        self.args = args
        self.kwargs = kwargs

    def reset(self):
        self.interface = None
        self.fields_set = self.fields_set_class()
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

    def unique_field(self, field_name):
        """set a unique field to be selected, this is automatically called when you do unique_FIELDNAME(...)"""
        self.fields_set.options["unique"] = True
        return self.select_field(field_name)

    def unique(self, field_name):
        return self.unique_field(field_name)

    def select_field(self, field_name):
        """set a field to be selected, this is automatically called when you do unique_FIELDNAME(...)"""
        return self.set_field(field_name, None)

    def select(self, *fields):
        """set multiple fields to be selected"""
        if fields:
            if not isinstance(fields[0], basestring): 
                fields = list(fields[0]) + list(fields)[1:]

        for field_name in fields:
            field_name = self._normalize_field_name(field_name)
            self.select_field(field_name)
        return self
    select_fields = select # DEPRECATED maybe? -- 3-10-2016 -- use select()

    def set_field(self, field_name, field_val=None):
        """
        set a field into .fields attribute

        n insert/update queries, these are the fields that will be inserted/updated into the db
        """
        field_name, field_val = self._normalize_field(field_name, field_val)
        #field_name = self._normalize_field_name(field_name)
        self.fields_set.append(field_name, [field_name, field_val])
        return self

    def set(self, fields=None, *fields_args, **fields_kwargs):
        """
        completely replaces the current .fields with fields and fields_kwargs combined
        """
        if fields_args:
            fields = [fields]
            fields.extend(fields_args)
            for field_name in fields:
                self.set_field(field_name)

        elif fields_kwargs:
            fields = make_dict(fields, fields_kwargs)
            for field_name, field_val in fields.items():
                self.set_field(field_name, field_val)

        else:
            if isinstance(fields, Mapping):
                for field_name, field_val in fields.items():
                    self.set_field(field_name, field_val)

            else:
                for field_name in fields:
                    self.set_field(field_name)

        return self

    # DEPRECATED maybe? -- 3-10-2016 -- use set()
    def set_fields(self, fields=None, *fields_args, **fields_kwargs):
        return self.set(fields, *fields_args, **fields_kwargs)

    def is_field(self, field_name, field_val=None, **field_kwargs):
        field_name, field_val = self._normalize_field(
            field_name,
            field_val=field_val,
            field_kwargs=field_kwargs,
        )
        #field_name = self._normalize_field_name(field_name)
        self.fields_where.append(field_name, ["is", field_name, field_val, field_kwargs])
        return self
    def eq_field(self, field_name, field_val, **field_kwargs):
        return self.is_field(field_name, field_val, **field_kwargs)

    def not_field(self, field_name, field_val=None, **field_kwargs):
        field_name, field_val = self._normalize_field(
            field_name,
            field_val=field_val,
            field_kwargs=field_kwargs,
        )
        #field_name = self._normalize_field_name(field_name)
        self.fields_where.append(field_name, ["not", field_name, field_val, field_kwargs])
        return self
    def ne_field(self, field_name, field_val, **field_kwargs):
        return self.not_field(field_name, field_val, **field_kwargs)

    def between_field(self, field_name, low, high):
        self.gte_field(field_name, low)
        self.lte_field(field_name, high)
        return self

    def lte_field(self, field_name, field_val=None, **field_kwargs):
        field_name, field_val = self._normalize_field(
            field_name,
            field_val=field_val,
            field_kwargs=field_kwargs,
        )
        #field_name = self._normalize_field_name(field_name)
        self.fields_where.append(field_name, ["lte", field_name, field_val, field_kwargs])
        return self

    def lt_field(self, field_name, field_val=None, **field_kwargs):
        field_name, field_val = self._normalize_field(
            field_name,
            field_val=field_val,
            field_kwargs=field_kwargs,
        )
        #field_name = self._normalize_field_name(field_name)
        self.fields_where.append(field_name, ["lt", field_name, field_val, field_kwargs])
        return self

    def gte_field(self, field_name, field_val=None, **field_kwargs):
        field_name, field_val = self._normalize_field(
            field_name,
            field_val=field_val,
            field_kwargs=field_kwargs,
        )
        #field_name = self._normalize_field_name(field_name)
        self.fields_where.append(field_name, ["gte", field_name, field_val, field_kwargs])
        return self

    def gt_field(self, field_name, field_val=None, **field_kwargs):
        field_name, field_val = self._normalize_field(
            field_name,
            field_val=field_val,
            field_kwargs=field_kwargs,
        )
        #field_name = self._normalize_field_name(field_name)
        self.fields_where.append(field_name, ["gt", field_name, field_val, field_kwargs])
        return self

    def in_field(self, field_name, field_val=None, **field_kwargs):
        """
        :param field_val: list, a list of field_val values
        """
        is_list = False
        if not isinstance(field_val, Query):
            field_val = make_list(field_val) if field_val else []
            is_list = True
        # ??? what does this do?
        if field_kwargs:
            # this normalizes the values of the kwargs so the interface can
            # treat all the values like a list regardless of you passing in
            # kwargs or field_val
            for k in field_kwargs:
                if not field_kwargs[k]:
                    raise ValueError("Cannot IN an empty list")

                field_kwargs[k] = make_list(field_kwargs[k])

        else:
            if not field_val: self.can_get = False

        field_name, field_val = self._normalize_field(
            field_name,
            field_val=field_val,
            field_kwargs=field_kwargs,
            is_list=is_list,
        )
        self.fields_where.append(field_name, ["in", field_name, field_val, field_kwargs])
        return self

    def nin_field(self, field_name, field_val=None, **field_kwargs):
        """
        :param field_val: list, a list of field_val values
        """
        if not isinstance(field_val, Query):
            field_val = make_list(field_val) if field_val else []
            is_list = True

        if field_kwargs:
            # this normalizes the values of the kwargs so the interface can
            # treat all the values like a list regardless of you passing in
            # kwargs or field_val
            for k in field_kwargs:
                if not field_kwargs[k]:
                    raise ValueError("Cannot IN an empty list")

                field_kwargs[k] = make_list(field_kwargs[k])

        else:
            if not field_val: self.can_get = False

        field_name, fv = self._normalize_field(
            field_name,
            field_val=field_val,
            field_kwargs=field_kwargs,
            is_list=is_list,
        )
        self.fields_where.append(field_name, ["nin", field_name, field_val, field_kwargs])
        return self

    def startswith_field(self, field_name, field_val, **field_kwargs):
        return self.like_field(field_name, u"{}%".format(field_val), **field_kwargs)

    def endswith_field(self, field_name, field_val, **field_kwargs):
        return self.like_field(field_name, u"%{}".format(field_val), **field_kwargs)

    def contains_field(self, field_name, field_val, **field_kwargs):
        return self.like_field(field_name, u"%{}%".format(field_val), **field_kwargs)

    def like_field(self, field_name, field_val, **field_kwargs):
        """Perform a field_name LIKE field_val query

        :param field_name: string, the field we are filtering on
        :param field_val: string, the like query: %val, %val%, val%
        :returns: self, for fluid interface
        """
        if not field_val:
            raise ValueError("Cannot LIKE nothing")
        field_name, field_val = self._normalize_field(
            field_name,
            field_val=field_val,
            field_kwargs=field_kwargs,
        )
        #field_name = self._normalize_field_name(field_name)
        self.fields_where.append(field_name, ["like", field_name, field_val, field_kwargs])
        return self

    def nlike_field(self, field_name, field_val, **field_kwargs):
        """Perform a field_name NOT LIKE field_val query

        :param field_name: string, the field we are filtering on
        :param field_val: string, the like query: %val, %val%, val%
        :returns: self, for fluid interface
        """
        if not field_val:
            raise ValueError("Cannot NOT LIKE nothing")
        field_name, field_val = self._normalize_field(
            field_name,
            field_val=field_val,
            field_kwargs=field_kwargs,
        )
        #field_name = self._normalize_field_name(field_name)
        self.fields_where.append(field_name, ["nlike", field_name, field_val, field_kwargs])
        return self

    def sort_field(self, field_name, direction, field_val=None):
        """
        sort this query by field_name in directrion

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

        field_name, field_val = self._normalize_field(
            field_name,
            field_val=list(field_val) if field_val else field_val,
        )
        self.fields_sort.append(field_name, [direction, field_name, field_val])
        return self

    def asc_field(self, field_name, field_val=None):
        self.sort_field(field_name, 1, field_val)
        return self

    def desc_field(self, field_name, field_val=None):
        self.sort_field(field_name, -1, field_val)
        return self

    def __getattr__(self, method_name):
        field_method, field_name = self._normalize_field_method(method_name)
        def callback(*args, **kwargs):
            #pout.v(args, kwargs, field_method, field_name)
            return field_method(field_name, *args, **kwargs)
        return callback

    def _normalize_field_method(self, method_name):
        # infinite recursion check, if a *_field method gets in here then it
        # doesn't exist
        if method_name.endswith("_field"):
            raise AttributeError(method_name)

        try:
            command, field_name = method_name.split("_", 1)

        except ValueError:
            raise AttributeError("invalid command_method: {}".format(method_name))

        else:
            if not command:
                raise AttributeError('Could not derive command from {}"'.format(
                    method_name
                ))

            field_method_name = "{}_field".format(command)
            field_method = getattr(self, field_method_name, None)
            if not field_method:
                raise AttributeError('No "{}" method derived from "{}"'.format(
                    field_method_name,
                    method_name
                ))

            # make sure field is legit also, this will raise an attribute error
            # if it can't find the field in the schema (and self has a schema)
            field_name = self._normalize_field_name(field_name)

        return field_method, field_name

    def _normalize_field_name(self, field_name):
        # normalize the field name if we can
        schema = self.schema
        if schema:
            field_name = schema.field_name(field_name)
        return field_name

    def _normalize_field_value(self, field_name, field_val):
        schema = self.schema
        if schema:
            field = getattr(schema, field_name)
            field_val = field.iquery(self, field_val)
        return field_val

    def _normalize_field(self, field_name, field_val, field_kwargs=None, is_list=False):
        field_name = self._normalize_field_name(field_name)

        if is_list:
            for i in range(len(field_val)):
                field_val[i] = self._normalize_field_value(field_name, field_val[i])

        else:
            field_val = self._normalize_field_value(field_name, field_val)

        return field_name, field_val

    def limit(self, limit):
        self.bounds.limit = limit
        return self
    set_limit = limit # DEPRECATED maybe? -- 3-10-2016 -- use limit()

    def offset(self, offset):
        self.bounds.offset = offset
        return self
    set_offset = offset # DEPRECATED maybe? -- 3-10-2016 -- use offset()

    def page(self, page):
        self.bounds.page = page
        return self
    set_page = page # DEPRECATED maybe? -- 3-10-2016 -- use page()

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

    def values(self, limit=None, page=None):
        """
        convenience method to get just the values from the query (same as get().values())

        if you want to get all values, you can use: self.all().values()
        """
        return self.get(limit=limit, page=page).values()

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

    def pks(self, limit=None, page=None):
        """convenience method for setting select_pk().values() since this is so common"""
        self.fields_set.reset()
        return self.select_pk().values(limit, page)

    def pk(self):
        """convenience method for setting select_pk().value() since this is so common"""
        self.fields_set.reset()
        return self.select_pk().value()

    def get_pks(self, field_vals):
        """convenience method for running in__id([...]).get() since this is so common"""
        field_name = self.schema.pk.name
        return self.in_field(field_name, field_vals).get()

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
        return self.interface.insert(self.schema, self.fields)

    def update(self):
        """persist the .fields using .fields_where"""
        self.default_val = 0
        #fields = self.fields
        #fields = self.orm_class.depart(self.fields, is_update=True)
        #self.set_fields(fields)
        return self.interface.update(
            self.schema,
            self.fields,
            self
        )
        #return self._query('update')

    def delete(self):
        """remove fields matching the where criteria"""
        self.default_val = None
        return self._query('delete')

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

    def reduce(self, target_map, target_reduce, threads=0):
        """map/reduce this query among a bunch of processes

        :param target_map: callable, this function will be called once for each 
            row this query pulls out of the db, if you want something about the row
            to be seen by the target_reduce function return that value from this function
            and it will be queued for the target_reduce function to process it
        :param target_reduce: callable, this function will be called for any non 
            None value that the target_map function returns
        :param threads: integer, if not passed in this will be pegged to how many
            cpus python detects, which is almost always what you want
        """
        if not threads:
            threads = multiprocessing.cpu_count()

        # we subtract one for the main process
        map_threads = threads - 1 if threads > 1 else 1

        q = self.copy()
        limit = q.bounds.limit
        offset = q.bounds.offset

        total_count = limit if limit else q.count()
        limit_count = int(math.ceil(float(total_count) / float(map_threads)))
        logger.info("{} processes will handle {} rows each for a total of {}".format(
            map_threads,
            limit_count,
            total_count
        ))

        queue = multiprocessing.JoinableQueue()

        # close all open db global connections just in case, because we can't be sure
        # what the target_map methods are going to do, we want them to re-open connections
        # that they need
        interfaces = get_interfaces()
        for name, inter in interfaces.items():
            inter.close()

        # just in case we also close the query connection since it can in theory
        # be non-global
        q.interface.close()

        ts = []
        for page in range(map_threads):
            q = self.copy()
            q.limit(limit_count).offset(offset + (limit_count * page))
            t = ReduceThread(
                target=target_map,
                query=q,
                queue=queue,
            )
            t.start()
            ts.append(t)

        while ts or not queue.empty():
            try:
                val = queue.get(True, 1.0)
                target_reduce(val)

            except queues.Empty:
                pass

            else:
                queue.task_done()

            # faster than using any((t.is_alive() for t in mts))
            ts = [t for t in ts if t.is_alive()]

    def watch(self, interval=60, timeout=0, cursor_field_name="pk"):
        inter = self.interface.spawn()
        start = time.time()

        # we want a new connection for this
        try:
            cursor_field_val = None
            while True:
                query = self.copy()
                query.interface = inter
                if cursor_field_val is not None:
                    query.gt_field(cursor_field_name, cursor_field_val)

                for instance in query.get():
                    yield instance
                    cursor_field_val = getattr(instance, cursor_field_name)

                time.sleep(interval)
                stop = time.time()
                if timeout:
                    if (stop - start) > timeout:
                        break

        finally:
            inter.close()

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


class ReduceThread(multiprocessing.Process):
    """Runs one of the reduce processes created in Query.reduce()

    You probably don't need to worry about this class
    """
    def __init__(self, target, query, queue):

        name = "Reduce-{}to{}".format(
            query.bounds.offset,
            query.bounds.offset + query.bounds.limit
        )

        logger.debug("Starting process: {}".format(name))

        def wrapper_target(target, query, queue):
            # create a new connection just for this thread
            #query.interface = query.interface.spawn()
            for orm in query.all():
                val = target(orm)
                if val:
                    try:
                        # queue size taps out at 32767, booooo
                        # http://stackoverflow.com/questions/5900985/multiprocessing-queue-maxsize-limit-is-32767
                        #queue.put_nowait(val)
                        queue.put(val, True, 1.0)
                    except queues.Full as e:
                        logger.exception(e)
                        #queue.close()
                        # If we ever hit a full queue you lose a ton of data but if you
                        # don't call this method then the process just hangs
                        queue.cancel_join_thread()
                        break

            query.interface.close()

        super(ReduceThread, self).__init__(target=wrapper_target, name=name, kwargs={
            "target": target,
            "query": query,
            "queue": queue,
        })


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

