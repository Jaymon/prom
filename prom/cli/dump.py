# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import

import os
import sys
import importlib
import fnmatch
import inspect
import pkgutil
import subprocess
from types import ModuleType

from captain import exit as console, echo
from captain.decorators import arg

from ..compat import *
from ..utils import get_objects
from ..model import Orm
from ..interface import get_interface


def get_modules(modulepath):
    """return all found modules at modulepath (eg, foo.bar) including modulepath module"""
    m = importlib.import_module(modulepath)
    mpath = m.__file__
    ret = set([m])

    if "__init__." in mpath.lower():
        mpath = os.path.dirname(mpath)

        # https://docs.python.org/2/library/pkgutil.html#pkgutil.iter_modules
        for module_info in pkgutil.iter_modules([mpath]):
            submodulepath = ".".join([modulepath, module_info[1]])
            if module_info[2]:
                # module is a package
                submodules = get_modules(submodulepath)
                ret.update(submodules)
            else:
                ret.add(importlib.import_module(submodulepath))

    return ret


def get_subclasses(modulepath, parent_class):
    """given a module return all the parent_class subclasses that are found in
    that module and any submodules.

    :param modulepath: string, a path like foo.bar.che
    :param parent_class: object, the class whose children you are looking for
    :returns: set, all the found child classes in modulepath of parent_class
    """
    if isinstance(modulepath, ModuleType):
        modules = get_modules(modulepath.__name__)
    else:
        modules = get_modules(modulepath)

    ret = set()
    for m in modules:
        cs = inspect.getmembers(m, lambda v: inspect.isclass(v) and issubclass(v, parent_class))
        for class_name, klass in cs:
            ret.add(klass)

    return ret


def build_dump_order(orm_class, orm_classes):
    """pass in an array, when you encounter a ref, call this method again with the array
    when something has no more refs, then it gets appended to the array and returns, each
    time something gets through the list they are added, but before they are added to the
    list it is checked to see if it is already in the listt"""
    if orm_class in orm_classes: return

    for field_name, field_val in orm_class.schema.fields.items():
        if field_val.is_ref():
            build_dump_order(field_val.schema.orm_class, orm_classes)

    if orm_class not in orm_classes:
        orm_classes.append(orm_class)


def get_orm_classes(path):
    """this will return prom.Orm classes found in the given path (classpath or modulepath)"""
    ret = set()
    try:
        m = importlib.import_module(path)

    except ImportError:
        # we have a classpath
        m, klass = get_objects(path)
        if issubclass(klass, Orm):
            ret.add(klass)

    else:
        ret.update(get_subclasses(m, Orm))

    return ret


def get_table_map(paths):
    ret = {}
    orm_classes = set()
    dump_orm_classes = []
    for p in paths:
        orm_classes.update(get_orm_classes(p))

    for orm_class in orm_classes:
        build_dump_order(orm_class, dump_orm_classes)

    try:
        for orm_class in dump_orm_classes:
            inter = orm_class.interface
            conn_name = inter.connection_config.name
            ret.setdefault(conn_name, {"interface": inter, "table_names": []})
            ret[conn_name]["table_names"].append(orm_class.table_name)

    except RuntimeError:
        pass

    return ret


def run_cmd(cmd):
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        if is_py2:
            for line in iter(process.stdout.readline, ""):
                sys.stdout.write(line)
                sys.stdout.flush()
        else:
            for line in iter(process.stdout.readline, b""):
                line = line.decode("utf-8")
                sys.stdout.write(line)
                sys.stdout.flush()

        process.wait()

    except subprocess.CalledProcessError as e:
        raise RuntimeError("dump failed with code {} and output: {}".format(e.returncode, e.output))

    except OSError as e:
        if e.errno == 2:
            echo.err("dump is not installed, you need to run `pip install dump`")
        raise


def get_base_cmd(action, inter, directory):

    conn = inter.connection_config

    if not "postgres" in conn.interface_name.lower():
        raise RuntimeError("Dump only works with Postgres databases")

    cmd = [
        "dump",
        action,
        "--dbname",
        conn.database,
        "--username",
        conn.username,
        "--password",
        conn.password,
        "--host",
        conn.host,
        "--directory",
        directory,
    ]

    if conn.port:
        cmd.extend(["--port", str(conn.port)])

    return cmd


@arg("-D", "--dir", "--directory", dest="directory", help="directory where the backup files should go")
@arg("--dry-run", dest="dry_run", action="store_true", help="act like you are going to do everything but do nothing")
@arg("paths", nargs="+", help="module or class paths (eg, foo.bar or foo.bar.Che) where prom Orm classes are defined")
def main_dump(paths, directory, dry_run):
    """dump all or part of the prom data, currently only works on Postgres databases

    basically just a wrapper around `dump backup` https://github.com/Jaymon/dump
    """
    table_map = get_table_map(paths)

    for conn_name, conn_info in table_map.items():
        inter = conn_info["interface"]
        conn = inter.connection_config
        table_names = conn_info["table_names"]

        cmd = get_base_cmd("backup", inter, directory)
        cmd.extend(table_names)

        if dry_run:
            echo.out(" ".join(cmd))

        else:
            run_cmd(cmd)


@arg("-D", "--dir", "--directory",
     dest="directory",
     help="directory where the backup files from a previous prom dump are located")
@arg("--connection-name", "-c",
    dest="conn_name",
    default="",
    help="the connection name (from prom dsn) you want to restore")
def main_restore(directory, conn_name):
    """Restore your database dumped with the dump command

    just a wrapper around `dump restore` https://github.com/Jaymon/dump
    """
    inter = get_interface(conn_name)
    conn = inter.connection_config
    cmd = get_base_cmd("restore", inter, directory)
    run_cmd(cmd)

