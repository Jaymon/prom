# -*- coding: utf-8 -*-
import importlib

from .compat import *


def get_objects(classpath, calling_classpath=""):
    """
    given a classpath like foo.bar.Baz return module foo.bar and class Baz
    objects

    .. seealso::
        https://docs.python.org/2.5/whatsnew/pep-328.html
        https://www.python.org/dev/peps/pep-0328/

    :param classpath: string, the full python class path (includes modules), a
        classpath is something like foo.bar.Che where Che is the class definied
        in the foo.bar module
    :param calling_classpath: string, if classpath is relative (eg, ..foo.Bar)
        then this is needed to resolve the relative classpath, it is usually the
        path of the module that is calling get_objects()
    :returns: tuple, (module, class)
    """
    if ":" in classpath:
        module_name, class_name = classpath.rsplit(":", 1)

    else:
        module_name, class_name = classpath.rsplit(".", 1)

    module = importlib.import_module(module_name, calling_classpath)
    try:
        klass = getattr(module, class_name)

    except AttributeError:
        raise AttributeError("module {} has no attribute {} parsing {}".format(
            module.__name__,
            class_name,
            classpath
        ))

    return module, klass


def make_list(vals):
    """make vals a list, no matter what

    :param vals: list, vals should always be a list or tuple (eg, *args passed
        as args)
    """
    ret = []
    if isinstance(vals, basestring):
        ret.append(vals)

    else:
        try:
            for val in vals:
                if isinstance(val, basestring):
                    ret.append(val)

                elif isinstance(val, (list, tuple)):
                    ret.extend(val)

                else:
                    try:
                        r = list(val)
                        ret.extend(r)

                    except TypeError:
                        ret.append(val)

        except TypeError:
            # TypeError: * is not iterable
            ret.append(vals)

    return ret


def make_dict(*fields):
    """lot's of methods take a dict or kwargs, this combines those

    Basically, we do a lot of def method(fields, **kwargs) and we want to merge
    those into one super dict with kwargs taking precedence, this does that

    fields -- dict -- a passed in dict
    fields_kwargs -- dict -- usually a **kwargs dict from another function

    return -- dict -- a merged fields and fields_kwargs
    """
    ret = {}

    for d in fields:
        if d:
            ret.update(d)

    return ret

