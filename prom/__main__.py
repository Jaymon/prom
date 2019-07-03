# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import

try:
    from captain import exit
    from captain.decorators import arg

except ImportError:
    print(" ".join([
        "To use the prom cli commands you need to install captain, usually this is",
        "as easy as `pip install captain`"
    ]))
    exit(1)

from prom.interface import get_interface
#from prom.cli import run_cmd
from prom.cli.generate import main_generate
from prom.cli.dump import main_dump
from prom.cli.dump import main_restore


def console():
    exit(__name__)


if __name__ == "__main__":
    console()

