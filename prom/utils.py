import types
import os
import importlib
import fnmatch
import inspect
from contextlib import contextmanager
import time


class Attempts(object):
    def __init__(self, callback, count, backoff=0):
        self.callback = callback
        self.count = count
        self.backoff = backoff

    def __call__(self, *args, **kwargs):
        for attempt in range(0, self.count):
            pout.b("attempt {}".format(attempt))
            try:
                time.sleep(attempt * self.backoff)
                return self.callback(*args, **kwargs)

            except Exception:
                pout.v("attempt is handling failure {}".format(attempt))
                if attempt == (self.count - 1):
                    raise




class attempts3(object):
    def __init__(self, count, backoff=0):
        self.count = count
        self.backoff = backoff

    def __enter__(self):
        for attempt in range(0, self.count):
            pout.v("attempt {}".format(attempt))
            try:
                time.sleep(attempt * self.backoff)
                pout.v("after backoff {}".format(attempt))
                return attempt

            except Exception:
                pout.v("attempt is handling failure {}".format(attempt))
                if attempt == (self.count - 1):
                    raise


    def __exit__(self, type, value, traceback):
        #pout.v(type, value, traceback)
        return False


@contextmanager
def attempts2(count, backoff=0):
    """this will attempt to do whatever is inside the with statement count times,
    retrying on any raised exception. If backup is set then it will backoff that
    many seconds x count between each attempt

    count -- integer -- how many attempts you want to make
    backoff -- integer -- how many seconds (times iteration) you want to wait before
        trying again
    """
    try:
        for attempt in range(0, count):
            pout.v("attempt {}".format(attempt))
            try:
                time.sleep(attempt * backoff)
                pout.v("after backoff {}".format(attempt))
                yield attempt
                raise ValueError()

            except ValueError:
                raise

            except Exception:
                pout.v("attempt is handling failure {}".format(attempt))
                if attempt == (count - 1):
                    raise

    except ValueError:
        pass

def get_subclasses(modulepath, parent_class):
    """given a module return all the parent_class subclasses that are found in
    that module and any submodules.

    You probably will never need this method, but I did and it didn't seem useful
    to keep this method buried in our proprietary codebase even if it isn't actually
    used anywhere in the prom codebase, I used it specifically for finding all
    the orm classes, this could probably also live in Endpoints since a bunch
    of this code was adapted from endpoint's methods

    modulepath -- string -- a path like foo.bar.che
    parent_class -- object -- the class whose children you are looking for
    """
    modules = []
    module = importlib.import_module(modulepath)
    basedir = os.path.dirname(module.__file__)
    for dirpath, dirnames, filenames in os.walk(basedir):
        #for f in filenames:
        #    if fnmatch.fnmatch(f, '*.py'):
        module_name = dirpath.replace(basedir, '', 1)
        module_name = [modulepath] + filter(None, module_name.split('/'))
        for f in fnmatch.filter(filenames, '*.py'):
            if f.startswith('__init__'):
                modules.append('.'.join(module_name))

            else:
                file_name = os.path.splitext(f)[0]
                modules.append('.'.join(module_name + [file_name]))

    # I could combine these loops but I'm more interested in readability over speed
    classes = []
    for mpath in modules:
        m = importlib.import_module(mpath)
        cs = inspect.getmembers(m, inspect.isclass)
        for c in cs:
            if not issubclass(c[1], parent_class): continue
            classes.append(c[1])

    return classes


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

