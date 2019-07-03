# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import

import sys
import hashlib

try:
    import cPickle as pickle
except ImportError:
    import pickle

# shamelessly ripped from https://github.com/kennethreitz/requests/blob/master/requests/compat.py
# Syntax sugar.
_ver = sys.version_info
is_py2 = (_ver[0] == 2)
is_py3 = (_ver[0] == 3)

if is_py2:
    basestring = basestring
    unicode = unicode
    range = xrange # range is now always an iterator

    import Queue as queue
    import thread as _thread
    try:
        from cStringIO import StringIO
    except ImportError:
        from StringIO import StringIO

    # shamelously ripped from six https://bitbucket.org/gutworth/six
    exec("""def reraise(tp, value, tb=None):
        try:
            raise tp, value, tb
        finally:
            tb = None
    """)

    from SimpleHTTPServer import SimpleHTTPRequestHandler
    from BaseHTTPServer import HTTPServer
    import Cookie as cookies
    import urlparse
    import __builtin__ as builtins

    # http://stackoverflow.com/a/5297483/5006
    def md5(text):
        return hashlib.md5(text).hexdigest()


elif is_py3:
    basestring = (str, bytes)
    unicode = str
    long = int

    #class long(int): pass

    import queue
    import _thread
    from io import StringIO
    from http.server import HTTPServer, SimpleHTTPRequestHandler
    from http import cookies
    from urllib import parse as urlparse
    import builtins

    def md5(text):
        return hashlib.md5(text.encode('utf-8')).hexdigest()

    # ripped from six https://bitbucket.org/gutworth/six
    def reraise(tp, value, tb=None):
        try:
            if value is None:
                value = tp()
            if value.__traceback__ is not tb:
                raise value.with_traceback(tb)
            raise value
        finally:
            value = None
            tb = None


