
class classproperty(property):
    """
    allow a class property to exist on the Orm

    NOTE -- this is read only, you can't write to the property

    example --

        class Foo(object):
            @classproperty
            def bar(cls):
                return 42

        Foo.bar # 42

    http://stackoverflow.com/questions/128573/using-property-on-classmethods
    http://stackoverflow.com/questions/5189699/how-can-i-make-a-class-property-in-python
    http://docs.python.org/2/reference/datamodel.html#object.__setattr__
    """
    def __get__(self, instance, cls):
        return self.fget(cls)

class cachedclassproperty(classproperty):
    """
    a memoized class property, it will do the calculation the first time and set the property
    to the calculated value

    NOTE -- this is read only, you can't write to the property

    example --

        class Foo(object):
            @cachedclassproperty
            def bar(cls):
                # do lots of calculations that take a long time :)
                result = 42
                return result

        Foo.bar # 42, but calculated
        Foo.bar # 42, but really fast

    http://stackoverflow.com/questions/128573/using-property-on-classmethods
    http://www.reddit.com/r/Python/comments/ejp25/cached_property_decorator_that_is_memory_friendly/
    """
    def __get__(self, instance, cls):
        v = self.fget(cls)
        # http://stackoverflow.com/questions/432786/how-can-i-assign-a-new-class-attribute-via-dict-in-python
        setattr(cls, self.fget.__name__, v)
        return v

