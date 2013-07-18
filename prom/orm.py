"""
this holds orm specific stuff
"""
import inspect
import sys

from .query import Query
from . import get_interface
from .utils import classproperty, cachedclassproperty

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

    @cachedclassproperty
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

    @cachedclassproperty
    def query_class(cls):
        """
        return the Query instance this class will use create Query instances to actually
        query the db

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

    @classproperty
    def query(cls):
        """
        return a new Query instance ready to make a db call using the child class

        example -- fluid interface

            results = Orm.query.is_foo('value').desc_bar().get()

        return -- Query() -- every time this is called a new query instance is created using cls.query_class
        """
        query_class = cls.query_class
        return query_class(orm=cls)

    def __init__(self, **fields):

        self.modified_fields = set()

        # go through and set all the values for 
        for field_name, field_val in fields.iteritems():
            setattr(self, field_name, field_val)


#        s = self.schema
#        for field_name in s.fields:
#            if field_name in fields:
#                setattr(self, field_name, fields[field_name])


    def __setattr__(self, field_name, field_val):

#        if 'modified_fields' not in self.__dict__:
#            self.__dict__['modified_fields'] = set()

        s = self.schema
        if field_name in s.fields:
            pout.v(field_name, field_val)
            self.modified_fields.add(field_name)

        self.__dict__[field_name] = field_val

    def set(self):

        d = {}

        # get all the modified fields
        for field_name in self.modified_fields:
            d[field_name] = getattr(self, field_name)


        _id = getattr(self, s.schema._id, None)
        if _id:
            # insert

        else
