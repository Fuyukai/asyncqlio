import re
import sys
from distutils.core import setup

if sys.version_info[0:2] < (3, 5):
    raise RuntimeError("This package requires Python 3.5+.")

with open("asyncqlio/__init__.py") as f:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]', f.read(), re.MULTILINE).group(1)

setup(
    name='asyncqlio',
    version=version,
    packages=[
        'asyncqlio',
        'asyncqlio.orm',
        'asyncqlio.orm.schema',
        # namespace packages yay
        'asyncqlio.backends',
        # postgres backend
        'asyncqlio.backends.postgresql',
        'asyncqlio.backends.postgresql.asyncpg'
    ],
    url='https://github.com/SunDwarf/asyncqlio',
    license='MIT',
    author='Laura Dickinson',
    author_email='l@veriny.tf',
    description='An asyncio ORM for Python 3.5',
    install_requires=[
        "cached_property==1.3.0"
    ]
)
