"""
Classes and stuff that handle querying the interface for a passed in Orm class
"""
import types
import copy
from collections import defaultdict
import datetime
import logging
import os
from contextlib import contextmanager

try:
    import thread
    #import threading
except ImportError:
    thread = None

from . import decorators
from .utils import make_list, get_objects, make_dict, make_hash


logger = logging.getLogger(__name__)


class Iterator(object):
    """
    smartly iterate through a result set

    this is returned from the Query.get() and Query.all() methods, it acts as much
    like a list as possible to make using it as seemless as can be

    fields --
        has_more -- boolean -- True if there are more results in the db, false otherwise
        ifilter -- callback -- an iterator filter, all yielded rows will be passed
            through this callback and skipped if ifilter(row) returns True

    examples --
        # iterate through all the primary keys of some orm
        for pk in SomeOrm.query.all().pk:
            print pk

    http://docs.python.org/2/library/stdtypes.html#iterator-types
    """
    def __init__(self, results, orm=None, has_more=False, query=None):
        """
        create a result set iterator

        results -- list -- the list of results
        orm -- Orm -- the Orm class that each row in results should be wrapped with
        has_more -- boolean -- True if there are more results
        query -- Query -- the query instance that produced this iterator
        """
        self.results = results
        self.ifilter = None # https://docs.python.org/2/library/itertools.html#itertools.ifilter
        self.orm = orm # TODO -- change to orm_class to be more consistent
        self.has_more = has_more
        self.query = query.copy()
        self._values = False
        self.reset()

    def reset(self):
        #self.iresults = (self._get_result(d) for d in self.results)
        #self.iresults = self.create_generator()
        inormalize = (self._get_result(d) for d in self.results)
        self.iresults = (o for o in inormalize if self._filtered(o))

    def next(self):
        return self.iresults.next()

    def values(self):
        """
        similar to the dict.values() method, this will only return the selected fields
        in a tuple

        return -- self -- each iteration will return just the field values in
            the order they were selected, if you only selected one field, than just that field
            will be returned, if you selected multiple fields than a tuple of the fields in
            the order you selected them will be returned
        """
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
        """list interface compatibility"""
        return len(self.results)

    def __getitem__(self, k):
        k = int(k)
        return self._get_result(self.results[k])

    def pop(self, k=-1):
        """list interface compatibility"""
        k = int(k)
        return self._get_result(self.results.pop(k))

    def reverse(self):
        """list interface compatibility"""
        self.results.reverse()
        self.reset()

    def __reversed__(self):
        self.reverse()
        return self

    def sort(self, *args, **kwargs):
        """list interface compatibility"""
        self.results.sort(*args, **kwargs)
        self.reset()

    def __getattr__(self, k):
        """
        this allows you to focus in on certain fields of results

        It's just an easier way of doing: (getattr(x, k, None) for x in self)
        """
        field_name = self.orm.schema.field_name(k)
        return (getattr(r, field_name, None) for r in self)

    def create_generator(self):
        """put all the pieces together to build a generator of the results"""
        inormalize = (self._get_result(d) for d in self.results)
        return (o for o in inormalize if not self._filtered(o))

    def _get_result(self, d):
        r = None
        if self._values:
            field_vals = [d.get(fn, None) for fn in self.field_names]
            r = field_vals if self.fcount > 1 else field_vals[0]

        else:
            if self.orm:
                r = self.orm.populate(d)
            else:
                r = d

        return r

    def _filtered(self, o):
        """run orm o through the filter, if True then orm o should be included"""
        return self.ifilter(o) if self.ifilter else True


class AllIterator(Iterator):
    """
    Similar to Iterator, but will chunk up results and make another query for the next
    chunk of results until there are no more results of the passed in Query(), so you
    can just iterate through every row of the db without worrying about pulling too
    many rows at one time

    NOTE -- pop() may have unexpected results
    """
    def __init__(self, query):
        limit, offset = query.bounds.get()
        if not limit:
            limit = 5000

        self.chunk_limit = limit
        self.offset = offset
        super(AllIterator, self).__init__(results=[], orm=query.orm, query=query)

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
            # k is not in here, so let's just grab it
            q = self.query.copy()
            orm = q.set_offset(k).get_one()
            if orm:
                v = self._get_result(orm.fields)
            else:
                raise IndexError("results index out of range")

        return v

    def count(self):
        ret = 0
        if self.results.has_more:
            # we need to do a count query
            q = self.query.copy()
            q.set_limit(0).set_offset(0)
            ret = q.count()
        else:
            ret = (self.offset - self.start_offset) + len(self.results)

        return ret

    def __iter__(self):
        has_more = True
        self.reset()
        while has_more:
            has_more = self.results.has_more
            for r in self.results:
                yield r

            self.offset += self.chunk_limit
            self._set_results()

    def _set_results(self):
        self.results = self.query.set_offset(self.offset).get(self.chunk_limit)
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


class Limit(object):

    @property
    def limit(self):
        l = self.limit_paginate if self.paginate else self._limit
        return l if l else 0
        #return getattr(self, "_limit", 0)

    @limit.setter
    def limit(self, v):
        v = int(v)
        if v < 0:
            raise ValueError("Limit cannot be negative")
        self._limit = v

    @limit.deleter
    def limit(self):
        self._limit = None
#         try:
#             del self._limit
#         except AttributeError: pass

    @property
    def limit_paginate(self):
        limit = self._limit
        #limit = self.limit
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
#         try:
#             del self._offset
#         except AttributeError: pass

    @property
    def page(self):
        page = self._page
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
#         try:
#             del self._page
#         except AttributeError: pass


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

    def __nonzero__(self):
        return self.limit > 0 or self.offset > 0

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
        q = Query(orm)
        q.is_foo(1).desc_bar().set_limit(10).set_page(2).get()
    """
    fields_class = Fields

    bounds_class = Limit

    @property
    def interface(self):
        if not self.orm_class: return None
        return self.orm_class.interface

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
        self.orm = kwargs.get("orm", orm_class) # DEPRECATED -- 3-11-2016 -- switch to orm_class
        self.orm_class = orm_class if orm_class else self.orm
        self.reset()
        self.args = args
        self.kwargs = kwargs

    def reset(self):
        self.fields_set = self.fields_class()
        self.fields_where = self.fields_class()
        self.fields_sort = self.fields_class()
        self.bounds = self.bounds_class()
        # the idea here is to set this to False if there is a condition that will
        # automatically cause the query to fail but not necessarily be an error, 
        # the best example is the IN (...) queries, if you do self.in_foo([]).get()
        # that will fail because the list was empty, but a value error shouldn't
        # be raised because a common case is: self.if_foo(Bar.query.is_che(True).pks).get()
        # which should result in an empty set if there are no rows where che = TRUE
        self.can_get = True

    def ref(self, orm_classpath, cls_pk=None):
        """
        takes a classpath to allow query-ing from another Orm class

        the reason why it takes string paths is to avoid infinite recursion import 
        problems because an orm class from module A might have a ref from module B
        and sometimes it is handy to have module B be able to get the objects from
        module A that correspond to the object in module B, but you can't import
        module A into module B because module B already imports module A.

        orm_classpath -- string -- a full python class path (eg, foo.bar.Che)
        cls_pk -- mixed -- automatically set the where field of orm_classpath 
            that references self.orm_class to the value in cls_pk if present
        return -- Query()
        """
        # split orm from module path
        orm_module, orm_class = get_objects(orm_classpath)
        q = orm_class.query
        if cls_pk:
            for fn, f in orm_class.schema.fields.items():
                cls_ref_s = f.schema
                if cls_ref_s and self.schema == cls_ref_s:
                    q.is_field(fn, cls_pk)
                    break

        return q

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
        return self.select_fields(*fields)

    # DEPRECATED maybe? -- 3-10-2016 -- use select()
    def select_fields(self, *fields):
        """set multiple fields to be selected"""
        if fields:
            if not isinstance(fields[0], types.StringTypes): 
                fields = list(fields[0]) + list(fields)[1:]

        for field_name in fields:
            self.select_field(field_name)

    def set_field(self, field_name, field_val=None):
        """
        set a field into .fields attribute

        n insert/update queries, these are the fields that will be inserted/updated into the db
        """
        self.fields_set.append(field_name, [field_name, field_val])
        return self

    def set(self, fields=None, *fields_args, **fields_kwargs):
        return self.set_fields(fields, *fields_args, **fields_kwargs)

    # DEPRECATED maybe? -- 3-10-2016 -- use select()
    def set_fields(self, fields=None, *fields_args, **fields_kwargs):
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
            if isinstance(fields, (types.DictType, types.DictProxyType)):
                for field_name, field_val in fields.items():
                    self.set_field(field_name, field_val)

            else:
                for field_name in fields:
                    self.set_field(field_name)

        return self

    def is_field(self, field_name, *field_val, **field_kwargs):
        fv = field_val[0] if field_val else None
        self.fields_where.append(field_name, ["is", field_name, fv, field_kwargs])
        return self

    def not_field(self, field_name, *field_val, **field_kwargs):
        fv = field_val[0] if field_val else None
        self.fields_where.append(field_name, ["not", field_name, fv, field_kwargs])
        return self

    def between_field(self, field_name, low, high):
        self.lte_field(field_name, low)
        self.gte_field(field_name, high)
        return self

    def lte_field(self, field_name, *field_val, **field_kwargs):
        fv = field_val[0] if field_val else None
        self.fields_where.append(field_name, ["lte", field_name, fv, field_kwargs])
        return self

    def lt_field(self, field_name, *field_val, **field_kwargs):
        fv = field_val[0] if field_val else None
        self.fields_where.append(field_name, ["lt", field_name, fv, field_kwargs])
        return self

    def gte_field(self, field_name, *field_val, **field_kwargs):
        fv = field_val[0] if field_val else None
        self.fields_where.append(field_name, ["gte", field_name, fv, field_kwargs])
        return self

    def gt_field(self, field_name, *field_val, **field_kwargs):
        fv = field_val[0] if field_val else None
        self.fields_where.append(field_name, ["gt", field_name, fv, field_kwargs])
        return self

    def in_field(self, field_name, *field_vals, **field_kwargs):
        """
        field_vals -- list -- a list of field_val values
        """
        fv = make_list(field_vals[0]) if field_vals else None
        if field_kwargs:
            for k in field_kwargs:
                if not field_kwargs[k]:
                    raise ValueError("Cannot IN an empty list")

                field_kwargs[k] = make_list(field_kwargs[k])

        else:
            if not fv: self.can_get = False

        self.fields_where.append(field_name, ["in", field_name, fv, field_kwargs])
        return self

    def nin_field(self, field_name, *field_vals, **field_kwargs):
        """
        field_vals -- list -- a list of field_val values
        """
        fv = make_list(field_vals[0]) if field_vals else None
        if field_kwargs:
            for k in field_kwargs:
                if not field_kwargs[k]:
                    raise ValueError("Cannot IN an empty list")

                field_kwargs[k] = make_list(field_kwargs[k])

        else:
            if not fv: self.can_get = False

        self.fields_where.append(field_name, ["nin", field_name, fv, field_kwargs])
        return self

    def sort_field(self, field_name, direction, field_vals=None):
        """
        sort this query by field_name in directrion

        field_name -- string -- the field to sort on
        direction -- integer -- negative for DESC, positive for ASC
        field_vals -- list -- the order the rows should be returned in
        """
        if direction > 0:
            direction = 1
        elif direction < 0:
            direction = -1
        else:
            raise ValueError("direction {} is undefined".format(direction))

        self.fields_sort.append(field_name, [direction, field_name, list(field_vals) if field_vals else field_vals])
        return self

    def asc_field(self, field_name, field_vals=None):
        self.sort_field(field_name, 1, field_vals)
        return self

    def desc_field(self, field_name, field_vals=None):
        self.sort_field(field_name, -1, field_vals)
        return self

    def __getattr__(self, method_name):
        command, field_name = self._split_method(method_name)

        def callback(*args, **kwargs):
            field_method_name = "{}_field".format(command)
            command_field_method = None

            if getattr(type(self), field_method_name, None):
                command_field_method = getattr(self, field_method_name)
            else:
                raise AttributeError('No "{}" method derived from "{}"'.format(field_method_name, method_name))

            return command_field_method(field_name, *args, **kwargs)

        return callback

    def _split_method(self, method_name):
        try:
            command, field_name = method_name.split(u"_", 1)
        except ValueError:
            raise ValueError("invalid command_method: {}".format(method_name))

        # normalize the field name if we can
        schema = self.schema
        if schema:
            field_name = schema.field_name(field_name)

        return command, field_name

    def limit(self, limit):
        return self.set_limit(limit)

    # DEPRECATED maybe? -- 3-10-2016 -- use limit()
    def set_limit(self, limit):
        self.bounds.limit = limit
        return self

    def offset(self, offset):
        return self.set_offset(offset)

    # DEPRECATED maybe? -- 3-10-2016 -- use offset()
    def set_offset(self, offset):
        self.bounds.offset = offset
        return self

    def page(self, page):
        return self.set_page(page)

    # DEPRECATED maybe? -- 3-10-2016 -- use page()
    def set_page(self, page):
        self.bounds.page = page
        return self

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

        return self.iterator_class(results, orm=self.orm_class, has_more=has_more, query=self)

    def all(self):
        """
        return every possible result for this query

        This is smart about returning results and will use the set limit (or a default if no
        limit was set) to chunk up the results, this means you can work your way through
        really big result sets without running out of memory

        return -- Iterator()
        """
        return AllIterator(self)

    def get_one(self):
        """get one row from the db"""
        self.default_val = None
        o = self.default_val
        d = self._query('get_one')
        if d:
            o = self.orm_class.populate(d)
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
        self.fields_sort = self.fields_class()

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
        fields = self.orm_class.depart(self.fields, is_update=False)
        self.set_fields(fields)
        return self.interface.insert(
            self.schema,
            fields
        )

        return self.interface.insert(self.schema, self.fields)

    def update(self):
        """persist the .fields using .fields_where"""
        self.default_val = 0
        fields = self.orm_class.depart(self.fields, is_update=True)
        self.set_fields(fields)
        return self.interface.update(
            self.schema,
            fields,
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

    def _query(self, method_name):
        if not self.can_get: return self.default_val
        i = self.interface
        s = self.schema
        return getattr(i, method_name)(s, self) # i.method_name(schema, query)

    def copy(self):
        """nice handy wrapper around the deepcopy"""
        return copy.deepcopy(self)

    def __deepcopy__(self, memodict={}):
        instance = type(self)(self.orm_class)
        for key, val in self.__dict__.iteritems():
            setattr(instance, key, copy.deepcopy(val, memodict))
        return instance


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


