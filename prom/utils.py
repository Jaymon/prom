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


class Pool(dict):
    """Generic pool of some values bounded by size, this means when size is reached
    then the least used item will be silently dropped from the pool.

    In order to use this class you must extend it and implement the create_value
    method 

    see -- model.OrmPool
    """
    def __init__(self, size=0):
        super(Pool, self).__init__()
        self.pq = PriorityQueue(size)

    def __getitem__(self, key):
        shuffle = key in self
        val = super(Pool, self).__getitem__(key)

        # since this is being accessed again, we move it to the end
        if shuffle:
            self.pq.add(key, val)
            #pout.v("shuffling {}".format(key))

        return val

    def __setitem__(self, key, val):
        super(Pool, self).__setitem__(key, val)
        try:
            self.pq.add(key, val)
        except OverflowError:
            self.popitem()
            self.pq.add(key, val)

    def popitem(self):
        dead_key, dead_val, dead_priority = self.pq.popitem()
        del self[dead_key]
        return dead_key, dead_val

    def __missing__(self, key):
        #pout.v("missing {}".format(key))
        val = self.create_value(key)
        self[key] = val
        return val

    def create_value(self, key):
        raise NotImplementedError()


class PriorityQueue(object):
    """A semi-generic priority queue, if you never pass in priorities it defaults to
    a FIFO queue

    This is basically an implementation of the example on this page:
    https://docs.python.org/2/library/heapq.html#priority-queue-implementation-notes
    """
    def __init__(self, size=0):
        """create an instance

        size -- int -- 0 means the queue is unbounded, otherwise it will raise an
        OverflowError when you try and add more than size vals"""
        self.pq = []
        self.item_finder = {}
        self.counter = itertools.count()
        self.size = size
        self.removed_count = 0

    def add(self, key, val, priority=None):
        """add a value to the queue with priority, using the key to know uniqueness

        key -- str -- this is used to determine if val already exists in the queue,
            if key is already in the queue, then the val will be replaced in the
            queue with the new priority
        val -- mixed -- the value to add to the queue
        priority -- int -- the priority of val
        """

        if key in self.item_finder:
            self.remove(key)

        else:
            # keep the queue contained
            if self.full():
                raise OverflowError("Queue is full")

        if priority is None:
            priority = next(self.counter)

        item = [priority, key, val]
        self.item_finder[key] = item
        heapq.heappush(self.pq, item)

    def remove(self, key):
        """remove the value found at key from the queue"""
        item = self.item_finder.pop(key)
        item[-1] = None
        self.removed_count += 1

    def popitem(self):
        """remove the next prioritized [key, val, priority] and return it"""
        pq = self.pq
        while pq:
            priority, key, val = heapq.heappop(pq)
            if val is None:
                self.removed_count -= 1

            else:
                del self.item_finder[key]
                return key, val, priority

        raise KeyError("pop from an empty priority queue")

    def pop(self):
        """remove the next prioritized val and return it"""
        key, val, priority = self.popitem()
        return val

    def full(self):
        """Return True if the queue is full"""
        if not self.size: return False
        return len(self.pq) == (self.size + self.removed_count)

    def keys(self):
        """return the keys in the order they are in the queue"""
        return [x[1] for x in self.pq if x[2] is not None]

    def values(self):
        """return the vals in the order they are in the queue"""
        return [x[2] for x in self.pq if x[2] is not None]

    def __contains__(self, key):
        return key in self.item_finder


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
    return md5(s)

