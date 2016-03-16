import importlib
import hashlib


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


def make_dict(fields, fields_kwargs):
    """lot's of methods take a dict or kwargs, this combines those

    Basically, we do a lot of def method(fields, **kwargs) and we want to merge
    those into one super dict with kwargs taking precedence, this does that

    fields -- dict -- a passed in dict
    fields_kwargs -- dict -- usually a **kwargs dict from another function

    return -- dict -- a merged fields and fields_kwargs
    """
    ret = {}
    if fields:
        ret.update(fields)

    if fields_kwargs:
        ret.update(fields_kwargs)

    return ret

def make_hash(*mixed):
    s = ""
    for m in mixed:
        s += str(m)
    # http://stackoverflow.com/questions/5297448/how-to-get-md5-sum-of-a-string
    return hashlib.md5(s).hexdigest()
