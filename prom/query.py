import types
"""
Classes and stuff that handle querying the interface for a passed in Orm class
"""


class Iterator(object):
    """
    smartly iterate through a result set

    this is returned from the Query.get() and Query.all() methods, it acts as much
    like a list as possible to make using it as seemless as can be

    fields --
        has_more -- boolean -- True if there are more results in the db, false otherwise

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
        self.orm = orm
        self.has_more = has_more
        self.query = query
        self.reset()
        self._values = False

    def reset(self):
        self.iresults = (self._get_result(x) for x in self.results)

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
        self.field_names = self.query.fields_select
        self.fcount = len(self.field_names)
        if not self.fcount:
            raise ValueError("no select fields were set, so cannot iterate values")

        return self

    def __iter__(self):
        self.reset()
        return self

    def __len__(self):
        return len(self.results)

    def __getitem__(self, k):
        k = int(k)
        return self._get_result(self.results[k])

    def __getattr__(self, k):
        """
        this allows you to focus in on certain fields of results

        It's just an easier way of doing: (getattr(x, k, None) for x in self)
        """
        field_name = self.orm.schema.field_name(k)
        return (getattr(r, field_name, None) for r in self)

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


class AllIterator(Iterator):
    """
    Similar to Iterator, but will chunk up results and make another query for the next
    chunk of results until there are no more results of the passed in Query(), so you
    can just iterate through every row of the db without worrying about pulling too
    many rows at one time
    """
    def __init__(self, query):
        limit, offset, _ = query.get_bounds()
        if not limit:
            limit = 5000

        self.chunk_limit = limit
        self.offset = offset
        self.start_offset = offset
        super(AllIterator, self).__init__(results=[], orm=query.orm, query=query)

    def __getitem__(self, k):
        v = None
        k = int(k)
        lower_bound = self.offset
        upper_bound = lower_bound + self.chunk_limit
        if k >= lower_bound and k < upper_bound:
            # k should be in this result set
            if not self.results:
                self._set_results()

            i = k - lower_bound
            v = self.results[i]

        else:
            # k is not in here, so let's just grab it
            orm = self.query.set_offset(k).get_one() # TODO -- clone query
            if orm:
                v = self._get_result(orm.fields)
            else:
                raise IndexError("results index out of range")

        return v

    def _set_results(self):
        self.results = self.query.set_offset(self.offset).get(self.chunk_limit)
        if self._values:
            self.results = self.results.values()

    def __iter__(self):
        has_more = True
        self.offset = self.start_offset
        while has_more:
            self._set_results()

            has_more = self.results.has_more
            for r in self.results:
                yield r

            self.offset += self.chunk_limit


class Query(object):
    """
    Handle standard query creation and allow interface querying

    example --
        q = Query(orm)
        q.is_foo(1).desc_bar().set_limit(10).set_page(2).get()
    """

    @property
    def fields(self):
        return dict(self.fields_set)

    @property
    def fields_select(self):
        return [select_field for select_field, _ in self.fields_set]

    def __init__(self, orm=None, *args, **kwargs):

        # needed to use the db querying methods like get(), if you just want to build
        # a query then you don't need to bother passing this in
        self.orm = orm

        self.fields_set = []
        self.fields_where = []
        self.fields_sort = []
        self.bounds = {}
        self.args = args
        self.kwargs = kwargs

    def __iter__(self):
        return self.get()

    def select_field(self, field_name):
        """set a field to be selected"""
        return self.set_field(field_name, None)

    def select_fields(self, *fields):
        """set multiple fields to be selected"""
        if fields:
            if not isinstance(fields[0], types.StringTypes): 
                fields = list(fields[0]) + list(fields)[1:]

        for field_name in fields:
            self.set_field(field_name)

    def set_field(self, field_name, field_val=None):
        """
        set a field into .fields attribute

        n insert/update queries, these are the fields that will be inserted/updated into the db
        """
        self.fields_set.append([field_name, field_val])
        return self

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
            if not fields: fields = {}
            if fields_kwargs:
                fields.update(fields_kwargs)
                for field_name, field_val in fields.iteritems():
                    self.set_field(field_name, field_val)

        else:
            if isinstance(fields, (types.DictType, types.DictProxyType)):
                for field_name, field_val in fields.iteritems():
                    self.set_field(field_name, field_val)

            else:
                for field_name in fields:
                    self.set_field(field_name)

        return self

    def is_field(self, field_name, field_val):
        self.fields_where.append(["is", field_name, field_val])
        return self

    def not_field(self, field_name, field_val):
        self.fields_where.append(["not", field_name, field_val])
        return self

    def between_field(self, field_name, low, high):
        self.lte_field(field_name, low)
        self.gte_field(field_name, high)
        return self

    def lte_field(self, field_name, field_val):
        self.fields_where.append(["lte", field_name, field_val])
        return self

    def lt_field(self, field_name, field_val):
        self.fields_where.append(["lt", field_name, field_val])
        return self

    def gte_field(self, field_name, field_val):
        self.fields_where.append(["gte", field_name, field_val])
        return self

    def gt_field(self, field_name, field_val):
        self.fields_where.append(["gt", field_name, field_val])
        return self

    def in_field(self, field_name, field_vals):
        """
        field_vals -- list -- a list of field_val values
        """
        assert field_vals, "Cannot IN an empty list"
        self.fields_where.append(["in", field_name, list(field_vals)])
        return self

    def nin_field(self, field_name, field_vals):
        """
        field_vals -- list -- a list of field_val values
        """
        assert field_vals, "Cannot NIN an empty list"
        self.fields_where.append(["nin", field_name, list(field_vals)])
        return self

    def sort_field(self, field_name, direction):
        if direction > 0:
            direction = 1
        elif direction < 0:
            direction = -1
        else:
            raise ValueError("direction {} is undefined".format(direction))

        self.fields_sort.append([direction, field_name])
        return self

    def asc_field(self, field_name):
        self.sort_field(field_name, 1)
        return self

    def desc_field(self, field_name):
        self.sort_field(field_name, -1)
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
        command, field_name = method_name.split(u"_", 1)
        if self.orm: field_name = self.orm.schema.field_name(field_name)

        return command, field_name

    def set_limit(self, limit):
        self.bounds['limit'] = int(limit)
        return self

    def set_offset(self, offset):
        self.bounds.pop("page", None)
        self.bounds['offset'] = int(offset)
        return self

    def set_page(self, page):
        self.bounds.pop("offset", None)
        self.bounds['page'] = int(page)
        return self

    def get_bounds(self):

        limit = offset = page = limit_paginate = 0
        if "limit" in self.bounds and self.bounds["limit"] > 0:
            limit = self.bounds["limit"]
            limit_paginate = limit + 1

        if "offset" in self.bounds:
            offset = self.bounds["offset"]
            offset = offset if offset >= 0 else 0

        else:
            if "page" in self.bounds:
                page = self.bounds["page"]
                page = page if page >= 1 else 1
                offset = (page - 1) * limit

        return (limit, offset, limit_paginate)

    def has_bounds(self):
        return len(self.bounds) > 0

    def get(self, limit=None, page=None):
        """
        get results from the db

        return -- Iterator()
        """
        if limit is not None:
            self.set_limit(limit)
        if page is not None:
            self.set_page(page)

        has_more = False
        limit, offset, limit_paginate = self.get_bounds()
        if limit_paginate:
            self.set_limit(limit_paginate)

        results = self._query('get')

        if limit_paginate:
            self.set_limit(limit)
            if len(results) == limit_paginate:
                has_more = True
                results.pop(limit)

        return Iterator(results, orm=self.orm, has_more=has_more, query=self)

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
        o = None
        d = self._query('get_one')
        if d:
            o = self.orm.populate(d)
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
        field_names = self.fields_select
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
        self.fields_set = []
        return self.select_pk().values(limit, page)

    def pk(self):
        """convenience method for setting select_pk().value() since this is so common"""
        self.fields_set = []
        return self.select_pk().value()

    def get_pks(self, field_vals):
        """convenience method for running in__id([...]).get() since this is so common"""
        field_name = self.orm.schema.pk
        return self.in_field(field_name, field_vals).get()

    def get_pk(self, field_val):
        """convenience method for running is__id(_id).get_one() since this is so common"""
        field_name = self.orm.schema.pk
        return self.is_field(field_name, field_val).get_one()

    def first(self):
        """convenience method for running asc__id().get_one()"""
        return self.asc__id().get_one()

    def last(self):
        """convenience method for running desc__id().get_one()"""
        return self.desc__id().get_one()

    def count(self):
        """return the count of the criteria"""
        return self._query('count')

    def has(self):
        """returns true if there is atleast one row in the db matching the query, False otherwise"""
        v = self.get_one()
        return True if v else False

    def set(self):
        """persist the .fields using .fields_where (if available)"""
        return self._query('set')

    def delete(self):
        """remove fields matching the where criteria"""
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
        i = self.orm.interface
        return i.query(query_str, *query_args, **query_options)

    def _query(self, method_name):
        i = self.orm.interface
        s = self.orm.schema
        return getattr(i, method_name)(s, self)

    def _create_orm(self, d):
        o = self.orm(**d)
        o.reset_modified()
        return o

