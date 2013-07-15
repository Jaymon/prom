import types

class Schema(object):

    table = u""
    """set the table name for this schema instance"""

    fields = {}
    """all the fields this schema instance will use"""

    indexes = {}
    """all the indexes this schema will have"""

    def __init__(self, table, **fields):

        self.table = table

        self._id = long, True
        self._created = long, True
        self._updated = long, True

        self.index_updated = self._updated

        for field_name, field_val in fields.iteritems():
            setattr(self, field_name, field_val)

    def __setattr__(self, name, val):
        """
        allow schema to magically set fields and indexes by using the method name

        you can either set a field name:

            self.fieldname = <type>, <required>, <option_hash>

        or you can set a normal index:

            self.index_indexname = field1, field2, ...

        or a unique index:

            self.unique_indexname = field1, field2, ...

        example --
            # add foo and bar fields
            self.foo = int, True, dict(min_size=0, max_size=100)
            self.bar = str, False, dict(max_size=32)

            # add a normal index and a unique index
            self.index_foobar = self.foo, self.bar
            self.unique_bar = self.bar
        """

        # compensate for the special _name fields
        if name[0] != '_':
            name_bits = name.split(u'_', 1)
        else:
            name_bits = [name]

        is_field = True

        index_name = u""
        if len(name_bits) > 1: # we might have an index
            index_name = name_bits[1]
            index_types = {
                # index_type : **kwargs options
                'index': {},
                'unique': {unique=True}
            }

            if name_bits[0] in index_types:
                is_field = False
                # compensate for passing in one value instead of a tuple
                if isinstance(val, (types.DictType, types.StringType)):
                    val = (val,)

                self.set_index(index_name, val, **index_types[name_bits[0]])

        if is_field:
            # compensate for passing in one value, not a tuple
            if isinstance(val, types.TypeType):
                val = (val,)

            self.set_field(name, *val)

    def __getattr__(self, name):
        """
        this is mainly here to enable fluid defining of indexes

        example -- 
            self.foo = int, True
            self.index_foo = s.foo

        return -- string -- the string value of the attribute name, eg, self.foo returns "foo"
        """
        if not name in self.fields:
            raise AttributeError("{} is not a valid field name".format(name))

        return self.fields[name]['name']

    def set_index(self, index_name, index_fields, unique=False):
        if not index_fields:
            raise ValueError("index_fields list was empty")

        field_names = []
        for field_name in index_fields:
            field_name = str(field_name)
            if not field_name in self.fields:
                raise NameError("no field named {} so cannot set index on it".format(field_name))

            field_names.append(field_name)

        if not index_name:
            index_name = u"_".join(field_names)

        self.indexes[index_name] = {
            'name': index_name,
            'fields': field_names,
            'unique': unique
        }

        return self

    def set_field(self, field_name, field_type, required=False, options=None):
        if field_name in self.fields:
            raise ValueError("{} already exists and cannot be changed".format(field_name))

        d = {
            'name': field_name,
            'type': field_type,
            'required': required
        }

        size_a = options.get("min_size", None)
        size_b = options.get("max_size", None)
        size = options.get("size", None)
        size_min = size_max = size = None

        if size > 0:
            d['size'] = size
        else:
            if size_a > 0 and size_b == None:
                d['size'] = size_a

            elif size_a == None and size_b > 0:
                d['size'] = size_b

            elif size_a >= 0 and size_b >= 0:
                d['min_size'] = size_a
                d['max_size'] = size_b

        self.fields[field_name] = d

        return self
