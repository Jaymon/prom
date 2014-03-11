import importlib
import types

def get_objects(classpath):
    """
    given a full classpath like foo.bar.Baz return module foo.bar and class Baz
    objects

    classpath -- string -- the full python class path (inludes modules)
    return -- tuple -- (module, class)
    """
    module_name, class_name = classpath.rsplit('.', 1)
    module = importlib.import_module(module_name)
    klass = getattr(module, class_name)
    return module, klass


def make_list(val):
    """make val a list, no matter what"""
    try:
        r = list(val)
    except TypeError:
        r = [val]

    return r

