# stdlib
import importlib
import inspect
import sys

# first party
from .config import DsnConnection, Schema
from .query import Query
from . import decorators

__version__ = '0.8.1'

interfaces = {}
"""holds all the configured interfaces"""

def configure(dsn):
    """
    configure an interface to be used to query a backend

    you use this function to configure an Interface using a dsn, then you can get
    that interface using the get_interface() method

    dsn -- string -- a properly formatted prom dsn, see DsnConnection for how to format the dsn
    """
    global interfaces

    c = DsnConnection(dsn)
    if c.name in interfaces:
        raise ValueError('a connection named "{}" has already been configured'.format(c.name))

    interface_module_name, interface_class_name = c.interface_name.rsplit('.', 1)
    interface_module = importlib.import_module(interface_module_name)
    interface_class = getattr(interface_module, interface_class_name)

    i = interface_class(c)
    set_interface(i, c.name)
    return i

def set_interface(interface, name=''):
    """
    don't want to bother with a dsn? Use this method to make an interface available
    """
    if not interface: raise ValueError('interface is empty')

    global interfaces
    interfaces[name] = interface

def get_interface(name=''):
    """
    get an interface that was created using configure()

    name -- string -- the name of the connection for the interface to return
    """
    global interfaces
    return interfaces[name]

class Orm(object):
    """
    this is the parent class of any model Orm class you want to create that can access the db

    NOTE -- you must set the schema class as a class property (not an instance property)

    example -- create a user class

        import prom

        class User(prom.Orm):

            schema = prom.Schema(
                "user_table_name",
                username=(str, True),
                password=(str, True)
                email=(str, True)
                unique_user=('username') # set a unique index on user
                index_email=('email') # set a normal index on email
            )

        # create a user
        u = User(username='foo', password='awesome_and_secure_pw_hash', email='foo@bar.com')
        u.set()

        # query for our new user
        u = User.query.is_username('foo').get_one()
        print u.username # foo
    """

    connection_name = ""
    """the name of the connection to use to retrieve the interface"""

    schema = None
    """the Schema() instance that this class will derive all its db info from"""

    @decorators.cachedclassproperty
    def interface(cls):
        """
        return an Interface instance that can be used to access the db

        this will do the work required to find the interface instance, and then set this
        property to that interface instance, so all subsequent calls to this property
        will return the Interface() directly without running this method again

        return -- Interface() -- the interface instance this Orm will use
        """
        i = get_interface(cls.connection_name)
        return i

    @decorators.cachedclassproperty
    def query_class(cls):
        """
        return the Query class this class will use to create Query instances to actually query the db

        To be as magical and helpful as possible, this will start at the child class and look
        for a ChildClassNameQuery defined in the same module, it will then move down through
        each parent class looking for a matching <ParentClassName>Query class defined in the 
        same module as the parent class. If you want to short circuit the auto-finding, you 
        can just set the query_class in the child class

        example -- set the Query class manually

            class Foo(Orm):
                query_class = module.Query

        like .interface, this is cached

        return -- Query -- the query class, not an instance, but the actaul class object, Query if
            a custom one isn't found
        """
        query_class = Query
        parents = inspect.getmro(cls)
        for parent in parents:
            parent_module_name = parent.__module__
            parent_class_name = '{}Query'.format(parent.__name__)
            if parent_module_name in sys.modules:
                parent_class = getattr(sys.modules[parent_module_name], parent_class_name, None)
                if parent_class:
                    query_class = parent_class
                    break

        return query_class

    @decorators.classproperty
    def query(cls):
        """
        return a new Query instance ready to make a db call using the child class

        example -- fluid interface

            results = Orm.query.is_foo('value').desc_bar().get()

        return -- Query() -- every time this is called a new query instance is created using cls.query_class
        """
        query_class = cls.query_class
        return query_class(orm=cls)

    @property
    def pk(self):
        """wrapper method to return the primary key, None if the primary key is not set"""
        return getattr(self, self.schema.pk, None)

    @property
    def created(self):
        """wrapper property method to return the created timestamp"""
        return getattr(self, self.schema._created, None)

    @property
    def updated(self):
        """wrapper property method to return the updated timestamp"""
        return getattr(self, self.schema._updated, None)

    def __init__(self, **fields):

        self.reset_modified()
        self.modify(fields)


    @classmethod
    def normalize(cls, d):
        """
        return only fields in d that are also in schema

        you can override this method to do some sanity checking of the fields

        d -- dict -- a dict of field/values
        return -- dict -- the field/values that are in cls.schema
        """
        rd = {}
        s = cls.schema
        for field_name, field_val in d.iteritems():
            if field_name in s.fields:
                rd[field_name] = field_val

        return rd

    def __setattr__(self, field_name, field_val):
        s = self.schema
        if field_name in s.fields:
            self.modified_fields.add(field_name)

        self.__dict__[field_name] = field_val

    def insert(self):
        """persist the field values of this orm"""
        fields = self.get_modified()
        q = self.query
        q.set_fields(fields)
        fields = q.set()
        self.modify(fields)
        self.reset_modified()
        return True

    def update(self):
        """re-persist the updated field values of this orm that has a primary key"""
        fields = self.get_modified()

        q = self.query
        _id_name = self.schema.pk
        _id = self.pk
        if _id:
            q.is_field(_id_name, _id)

        q.set_fields(fields)
        fields = q.set()
        self.modify(fields)
        self.reset_modified()
        return True

    def set(self):
        """
        persist the fields in this object into the db, this will update if _id is set, otherwise
        it will insert

        see also -- .insert(), .update()
        """
        ret = False

        _id_name = self.schema.pk
        _id = self.pk
        if _id:
            ret = self.update()
        else:
            ret = self.insert()

        return ret

    def delete(self):
        """delete the object from the db if _id is set"""
        ret = False
        q = self.query
        _id = self.pk
        _id_name = self.schema.pk
        if _id:
            self.query.is_field(_id_name, _id).delete()
            # get rid of _id
            delattr(self, _id_name)

            # mark all the fields that still exist as modified
            self.reset_modified()
            for field_name in self.schema.fields:
                if hasattr(self, field_name):
                    self.modified_fields.add(field_name)

            ret = True

        return ret

    def get_modified(self):
        """return the modified fields and their new values"""
        fields = {}
        for field_name in self.modified_fields:
            fields[field_name] = getattr(self, field_name)

        return fields

    def modify(self, fields):
        """update the fields of this instance with the values in dict fields"""
        for field_name, field_val in fields.iteritems():
            setattr(self, field_name, field_val)

    def is_modified(self):
        """true if a field has been changed from its original value, false otherwise"""
        return len(self.modified_fields) > 0

    def reset_modified(self):
        """
        reset field modification tracking

        this is handy for when you are loading a new Orm with the results from a query and
        you don't want set() to do anything, you can Orm(**fields) and then orm.reset_modified() to
        clear all the passed in fields from the modified list
        """
        self.modified_fields = set()

    @classmethod
    def install(cls):
        """install the Orm's table using the Orm's schema"""
        return cls.interface.set_table(cls.schema)

