"""
Handle standard query creation

example --

    query.table("table_name").is_foo(1).desc_bar().set_limit(10).set_page(2).get()

"""
#    query.table("table_name").is_foo(1).desc_bar().limit(10).page(2).get()
#    query.table("table_name").is_foo(1).desc_bar().set_limit(10).set_page(2).get()
#    query.table("table_name").is_foo(1).desc_bar().with_limit(10).with_page(2).get()
#    query.table("table_name").is_foo(1).desc_bar().use_limit(10).use_page(2).get()
#    query.table("table_name").is_foo(1).desc_bar().limit_to(10).on_page(2).with_offset(5).get()

class Query(object):

    def __init__(self, orm=None, *args, **kwargs):

        # needed to use the db querying methods like get(), if you just want to build
        # a query then you don't need to bother passing this in
        self.orm = orm

        self.fields = []
        self.fields_where = []
        self.fields_sort = []
        self.bounds = {}
        self.args = args
        self.kwargs = kwargs

    def set_field(self, field_name, field_val=None):
        """
        set a field into .fields attribute

        this has a dual role, in select queries, these are the select fields, but in insert/update
        queries, these are the fields that will be inserted/updated into the db
        """
        self.fields.append([field_name, field_val])
        return self

    def set_fields(self, fields=None, **fields_kwargs):
        """
        completely replaces the current .fields with fields and fields_kwargs combined
        """
        if not fields: fields = {}
        fields.update(fields_kwargs)
        self.fields = fields
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

    def in_field(self, field_name, *field_vals):
        """
        NOTE -- if you have a list of values, it has to be passed in using the *, in_field("name", *list_of_values)
        """
        self.fields_where.append(["in", field_name, field_vals])
        return self

    def nin_field(self, field_name, *field_vals):
        """
        NOTE -- if you have a list of values, it has to be passed in using the *, nin_field("name", *list_of_values)
        """
        self.fields_where.append(["nin", field_name, field_vals])
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

            if field_method_name in self.__class__.__dict__:
                command_field_method = getattr(self, field_method_name)
            else:
                raise AttributeError('No "{}" method derived from "{}"'.format(field_method_name, method_name))

            return command_field_method(field_name, *args, **kwargs)

        return callback

    def _split_method(self, method_name):
        command, field_name = method_name.split(u"_", 1)
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
        if limit is not None:
            self.set_limit(limit)
        if page is not None:
            self.set_page(page)

        for d in self._query('get'):
            yield self._create_orm(d)

    def get_one(self):
        """get one row from the db"""
        o = None
        d = self._query('get_one')
        if d:
            o = self._create_orm(d)
        return o

    def get_pk(self, field_val):
        """convenience method for running is__id(_id).get_one() since this is so common"""
        field_name = self.orm.schema.pk
        return self.is_field(field_name, field_val).get_one()

    def count(self):
        """return the count of the criteria"""
        return self._query('count')

    def set(self):
        """persist the .fields using .fields_where (if available)"""
        return self._query('set')

    def delete(self):
        """remove fields matching the where criteria"""
        return self._query('delete')

    def get_query(self, query_str, *query_args, **query_options):
        """
        use the interface query method to pass in your own raw query
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

