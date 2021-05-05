#!/usr/bin/env python
# http://docs.python.org/distutils/setupscript.html
# http://docs.python.org/2/distutils/examples.html
# https://packaging.python.org/
# https://packaging.python.org/tutorials/distributing-packages/
from setuptools import setup, find_packages
import re
import os
from codecs import open


name = "prom"
kwargs = dict(
    name=name,
    description='A sensible orm for PostgreSQL or SQLite',
    author='Jay Marcyes',
    author_email='jay@marcyes.com',
    url='http://github.com/jaymon/{}'.format(name),
    license="MIT",
    classifiers=[ # https://pypi.python.org/pypi?:action=list_classifiers
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries',
        'Topic :: Utilities',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
    ],
#     entry_points = {
#         'console_scripts': [
#             '{} = {}.__main__:console'.format(name, name),
#         ],
#     }

)

kwargs["tests_require"] = ['testdata']
kwargs["install_requires"] = ['dsnparse', 'datatypes']
kwargs["extras_require"] = {
    'postgres': ["psycopg", "psycogreen", "gevent"],
}


def read(path):
    if os.path.isfile(path):
        with open(path, encoding='utf-8') as f:
            return f.read()
    return ""


vpath = os.path.join(name, "__init__.py")
if os.path.isfile(vpath):
    kwargs["packages"] = find_packages(exclude=["tests", "tests.*", "*_test*", "example*"])

    dpath = os.path.join(name, "data")
    if os.path.isdir(dpath):
        # https://docs.python.org/3/distutils/setupscript.html#installing-package-data
        kwargs["package_data"] = {name: ['data/*']} 

else:
    vpath = "{}.py".format(name)
    kwargs["py_modules"] = [name]

kwargs["version"] = re.search(r"^__version__\s*=\s*[\'\"]([^\'\"]+)", read(vpath), flags=re.I | re.M).group(1)

# https://pypi.org/help/#description-content-type
kwargs["long_description"] = read('README.md')
kwargs["long_description_content_type"] = "text/markdown"


setup(**kwargs)

