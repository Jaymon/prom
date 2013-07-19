# stdlib
import importlib
import inspect
import sys

# first party
from .config import DsnConnection, Schema
from .query import Query
# this needs psycopg2 to be installed to import, so I think it is better to make
# it only available in longform so there are no unneeded dependencies to import prom
#from .interface.postgres import Interface as PostgresInterface
from . import decorators

__version__ = '0.4.3'

_interfaces = {}
"""holds all the configured interfaces"""

def configure(dsn):
    """
    configure an interface to be used to query a backend

    you use this function to configure an Interface using a dsn, then you can get
    that interface using the get_interface() method

    dsn -- string -- a properly formatted prom dsn, see DsnConnection for how to format the dsn
    """
    c = DsnConnection(dsn)
    if c.name in _interfaces:
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
    _interfaces[name] = interface

def get_interface(name=''):
    """
    get an interface that was created using configure()

    name -- string -- the name of the connection for the interface to return
    """
    return _interfaces[name]

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
        return the Query class this class will use create Query instances to actually query the db

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
            #pout.v(parent_module_name, parent_class_name)
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

    def __init__(self, **fields):

        self.reset_modified()

        # go through and set all the values for 
        for field_name, field_val in fields.iteritems():
            setattr(self, field_name, field_val)

    def __setattr__(self, field_name, field_val):
        s = self.schema
        if field_name in s.fields:
            self.modified_fields.add(field_name)

        self.__dict__[field_name] = field_val

    def set(self):
        """
        persist the fields in this object into the db, this will update if _id is set, otherwise
        it will insert
        """
        ret = False
        d = {}

        # get all the modified fields
        for field_name in self.modified_fields:
            d[field_name] = getattr(self, field_name)

        if d:
            q = self.query
            # check if we should update, or insert
            _id_name = self.schema.pk
            _id = self.pk
            if _id:
                q.is_field(_id_name, _id)

            q.set_fields(d)
            d = q.set()

            for field_name, field_val in d.iteritems():
                d[field_name] = setattr(self, field_name, field_val)

            # orm values have now been persisted, so reset
            self.modified_fields = set()
            ret = True

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
#        if b:
#            if len(self.modified_fields) == 0:
#                for field_name in self.schema.fields:
#                    if not field_name.startwith('_'): # ignore the magic fields
#                        self.modified_fields.add(field_name)
#        else:
        self.modified_fields = set()

