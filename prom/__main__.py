# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import

try:
    from captain import exit as console
    from captain.decorators import arg

except ImportError:
    print(" ".join([
        "To use the prom cli commands you need to install captain, usually this is",
        "as easy as `pip install captain`"
    ]))
    exit()

from prom.interface import get_interface
#from prom.cli import run_cmd
from prom.cli.generate import main_generate
from prom.cli.dump import main_dump
from prom.cli.dump import main_restore


# @arg("--connection-name", "-c",
#     dest="conn_name",
#     default="",
#     help="the connection name (from prom dsn) you want to restore")
# def main_shell(conn_name):
#     """quick way to get into the db shell using prom dsn"""
#     inter = get_interface(conn_name)
#     conn = inter.connection_config
# 
#     if "postgres" in conn.interface_name.lower():
#         # TODO -- need to write password to pgpass file and set environment variable
#         # and call psql, I don't think you can put the password on the cli
#         cmd = [
#             "psql",
#             "--dbname",
#             conn.database,
#             "--username",
#             conn.username,
#             "--password",
#             conn.password,
#             "--host",
#             conn.host,
#             "--port",
#             str(conn.port),
#         ]
# 
#     elif "sqlite" in conn.interface_name.lower():
#         cmd = [
#             "sqlite3",
#             conn.database
#         ]
#     else:
#         raise RuntimeError("Unsupported interface")
# 
#     import subprocess
#     try:
#         process = subprocess.Popen(
#             cmd,
#             stdout=subprocess.PIPE,
#             stderr=subprocess.PIPE,
#             stdin=subprocess.PIPE,
#         )
# 
#         c = ""
#         while True:
#             process.stdin.write(c)
#             r = process.stdout.readline()
#             sys.stdout.write(r)
#             sys.stdout.flush()
# 
# 
#             #r = process.communicate(c)
#             #print 'gnugo says: ' + str(r)
#             c = raw_input()
#             c += "\n"
# 
# 
# 
# #         for line in iter(process.stdout.readline, ""):
# #             #echo.out(line)
# #             sys.stdout.write(line)
# #             sys.stdout.flush()
# #         process.wait()
# 
#     except subprocess.CalledProcessError as e:
#         raise RuntimeError("dump failed with code {} and output: {}".format(e.returncode, e.output))
# 


if __name__ == "__main__":
    console()

