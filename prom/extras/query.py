# -*- coding: utf-8 -*-
"""
Classes and stuff that handle querying the interface for a passed in Orm class
"""
from __future__ import unicode_literals, division, print_function, absolute_import
import copy
from collections import defaultdict, Mapping, OrderedDict
import datetime
import logging
import os
from contextlib import contextmanager
import math
import inspect
import time
import re
import threading

from ..compat import *
from ..utils import make_hash
from ..query import Query
from .. import decorators


logger = logging.getLogger(__name__)


class BaseCacheQuery(Query):
    """a standard query caching skeleton class with the idea that it would be expanded
    upon on a per project or per model basis

    by default, this is designed to call a caching method for certain queries, so
    if you only wanted to cache "get_one" type events, you could add a method
    to a child class like `cache_key_get_one` and this will call that method
    everytime a `get_one` method is invoked (this includes wrapper methods like
    `value`, `has`, and `first`).

    similar for delete, there are 3 deleting events, `insert`, `update`, and `delete`
    and so you can add a method like `cache_delete_update()` to only invalidate on
    updates but completely ignore update and delete events

    A child class will have to implement the methods that raise NotImplementedError
    in order to have a valid CacheQuery child
    """
    def cache_delete(self, method_name):
        method = getattr(self, "cache_delete_{}".format(method_name), None)
        if method:
            method()

    def cache_key(self, method_name):
        """decides if this query is cacheable, returns a key if it is, otherwise empty"""
        key = ""
        method = getattr(self, "cache_key_{}".format(method_name), None)
        if method:
            key = method()

        return key

    def cache_set(self, key, result):
        raise NotImplementedError()

    def cache_get(self, key):
        """must return a tuple (returned_value, cache_hit) where the returned value
        is what would be returned from the db and cache_hit is True if it was in
        the cache, otherwise False"""
        raise NotImplementedError()

    def _query(self, method_name):
        cache_hit = False
        cache_key = self.cache_key(method_name)
        table_name = str(self.schema)
        if cache_key:
            logger.debug("Cache check on {} for key {}".format(table_name, cache_key))
            result, cache_hit = self.cache_get(cache_key)

        if not cache_hit:
            logger.debug("Cache miss on {} for key {}".format(table_name, cache_key))
            result = super(BaseCacheQuery, self)._query(method_name)
            if cache_key:
                self.cache_set(cache_key, result)

        else:
            logger.debug("Cache hit on {} for key {}".format(table_name, cache_key))

        self.cache_hit = cache_hit
        return result

    def update(self):
        ret = super(BaseCacheQuery, self).update()
        if ret:
            logger.debug("Cache delete on {} update".format(self.schema))
            self.cache_delete("update")
        return ret

    def insert(self):
        ret = super(BaseCacheQuery, self).insert()
        if ret:
            logger.debug("Cache delete on {} insert".format(self.schema))
            self.cache_delete("insert")
        return ret

    def delete(self):
        ret = super(BaseCacheQuery, self).delete()
        if ret:
            logger.debug("Cache delete on {} delete".format(self.schema))
            self.cache_delete("delete")
        return ret


class CacheNamespace(defaultdict):
    """This is what actually does the memory processing caching of CacheQuery, it
    namespaces by process_id -> thread_id -> table, otherwise it is identical to
    any other default dict that sets default_factory to itself
    """

    @property
    def active(self):
        ret = False
        cn = self.get_process()
        if "active" in cn:
            ret = cn["active"]
        return ret

    @active.setter
    def active(self, v):
        cn = self.get_process()
        cn["active"] = bool(v)

    @active.deleter
    def active(self):
        self.get_process().pop("active", None)

    @property
    def ttl(self):
        """how long you should cache results for cacheable queries"""
        ret = 3600
        cn = self.get_process()
        if "ttl" in cn:
            ret = cn["ttl"]
        return ret

    @ttl.setter
    def ttl(self, ttl):
        cn = self.get_process()
        cn["ttl"] = int(ttl)

    @ttl.deleter
    def ttl(self):
        self.get_process().pop("ttl", None)

    @property
    def process_id(self):
        ret = ""
        if threading:
            f = getattr(os, 'getpid', None)
            if f:
                ret = str(f())
        return ret

    @property
    def thread_id(self):
        ret = ""
        if threading:
            ret = str(threading.current_thread().ident)
        return ret

    def __init__(self):
        super(CacheNamespace, self).__init__(CacheNamespace)

    def get_process(self):
        return self[self.process_id][self.thread_id]

    def get_table(self, schema):
        return self.get_process()[str(schema)]


class CacheQuery(BaseCacheQuery):
    """a simple in-memory cache, ttls should be short since this has
    a very naive invalidation mechanism"""

    _cache_namespace = CacheNamespace()
    """store the cached values in memory"""

    @decorators.classproperty
    def cache_namespace(cls):
        return cls._cache_namespace

    @classmethod
    def cache_activate(cls, v):
        cls.cache_namespace.active = bool(v)

    @property
    def cache_table(self):
        return self.cache_namespace.get_table(self.schema)

    @classmethod
    @contextmanager
    def cache(cls, ttl=60):
        cn = cls.cache_namespace
        cn.ttl = ttl
        cls.cache_activate(True)

        try:
            yield cls

        finally:
            # cleanup
            cn.clear()
            cls.cache_activate(False)

    def cache_delete_update(self):
        self.cache_table.clear()

    def cache_delete_insert(self):
        self.cache_table.clear()

    def cache_delete_delete(self):
        self.cache_table.clear()

    def cache_hash(self, method_name):
        key = make_hash(
            method_name,
            self.fields_set,
            self.fields_where,
            self.fields_sort,
            self.bounds
        )
        return key

    def cache_key_get_one(self):
        return self.cache_hash("get_one")

    def cache_key_get(self):
        return self.cache_hash("get")

    def cache_key_count(self):
        return self.cache_hash("count")

    def cache_set(self, key, result):
        cn = self.cache_namespace
        now = datetime.datetime.utcnow()
        self.cache_table[key] = {
            "ttl": cn.ttl,
            "datetime": now,
            "result": result
        }

    def cache_get(self, key):
        result = None
        cache_hit = False
        ct = self.cache_table
        now = datetime.datetime.utcnow()
        if key in ct:
            val = ct[key]
            td = now - val["datetime"]
            if td.total_seconds() < val["ttl"]:
                cache_hit = True
                result = val["result"]

        return result, cache_hit

    def cache_key(self, method_name):
        ret = ""
        if self.cache_namespace.active:
            ret = super(CacheQuery, self).cache_key(method_name)
        return ret

