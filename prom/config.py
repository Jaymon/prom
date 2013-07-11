import types

class Schema(object):

    fields = {}

    indexes = {}

    def __init__(self):

        self._id = long, True
        self._created = long, True
        self._updated = long, True

        self.index_updated = self._updated

    def __setattr__(self, name, val):

        if name[0] != '_':
            name_bits = name.split(u'_', 1)
        else:
            name_bits = [name]

        index_name = u""
        if len(name_bits) > 1: index_name = name_bits[1]

        if name_bits[0] == 'index':
            self.set_index(index_name, val)
            
        elif name_bits[0] == 'unique':
            self.set_index(index_name, val, unique=True)
        else:
            if isinstance(val, types.TypeType):
                val = (val,)

            self.set_field(name, *val)

    def __getattr__(self, name):
        return self.fields[name]

    def set_index(self, index_name, index_fields, unique=False):
        if not index_fields:
            raise ValueError("no index_fields")

        if isinstance(index_fields, (types.DictType, types.StringType)):
            index_fields = (index_fields,)

        field_names = []
        for f in index_fields:
            field_name = ""
            if isinstance(f, dict):
                field_name = f['name']
            else:
                field_name = f

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

    def set_field(self, field_name, field_type, required=False, range_a=None, range_b=None, default_val=None):

        self.fields[field_name] = {
            'name': field_name,
            'type': field_type,
            'default': default_val,
            'required': required
        }

        range_min = range_max = None

        if range_a > 0 and range_b == None:
            range_min = range_a
            range_max = range_a

        elif range_a == None and range_b > 0:
            range_min = None
            range_max = range_b

        elif range_a >= 0 and range_b >= 0:
            range_min = range_a
            range_max = range_b

        if range_min > 0:
            self.fields[field_name]['range_min'] = range_min

        if range_max > 0:
            self.fields[field_name]['range_max'] = range_max

