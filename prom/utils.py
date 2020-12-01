# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import importlib
import heapq
import itertools
import os
import sys
import codecs
from contextlib import contextmanager

from .compat import *


class Stream(object):
    """In the CLI we either want to print to stdout or write to a file, this wrapper
    does that, if path is empty then anything you write to it will write to stdout
    """
    def __init__(self, path=""):
        self.path = path
        if path:
            self.path = os.path.abspath(os.path.expanduser(str(path)))
            self.stream = codecs.open(self.path, encoding='utf-8', mode='w+')
        else:
            self.stream = sys.stdout

    def __getattr__(self, k):
        return getattr(self.stream, k)

    @contextmanager
    def open(self):
        yield self
        if self.path:
            self.stream.close()

    def write_line(self, line, count=1):
        """writes the line and count newlines after the line"""
        self.write(line)
        self.write_newlines(count)

    def write_newlines(self, count=1):
        """writes count newlines"""
        for c in range(count):
            self.write("\n")


def get_objects(classpath, calling_classpath=""):
    """
    given a classpath like foo.bar.Baz return module foo.bar and class Baz
    objects

    .. seealso::
        https://docs.python.org/2.5/whatsnew/pep-328.html
        https://www.python.org/dev/peps/pep-0328/

    :param classpath: string, the full python class path (includes modules), a classpath
        is something like foo.bar.Che where Che is the class definied in the foo.bar
        module
    :param calling_classpath: string, if classpath is relative (eg, ..foo.Bar) then
        this is needed to resolve the relative classpath, it is usually the path of
        the module that is calling get_objects()
    :returns: tuple, (module, class)
    """
#     if classpath.startswith("."):
#         rel_count = len(re.match("^\.+", classpath).group(0))
#         if calling_classpath:
#             calling_count = calling_classpath.count(".")
#             if rel_count > calling_count:
#                 raise ValueError(
#                     "Attempting relative import passed calling_classpath {}".format(
#                         calling_classpath
#                     )
#                 )
# 
#             bits = calling_classpath.rsplit('.', rel_count)
#             parent_classpath = bits[0]
#             classpath = ".".join([parent_classpath, classpath[rel_count:]])
# 
#         else:
#             raise ValueError("Attempting relative import without calling_classpath")
# 
    module_name, class_name = classpath.rsplit('.', 1)
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

    :param vals: list, vals should always be a list or tuple (eg, *args passed as args)
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
        s += String(m)
    # http://stackoverflow.com/questions/5297448/how-to-get-md5-sum-of-a-string
    return String(s).md5()

