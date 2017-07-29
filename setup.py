import sys
from pathlib import Path

from setuptools import setup

if sys.version_info[0:2] < (3, 5):
    raise RuntimeError("This package requires Python 3.5+.")

setup(
    name="asyncqlio",
    use_scm_version={
        "version_scheme": "guess-next-dev",
        "local_scheme": "dirty-tag"
    },
    packages=[
        "asyncqlio",
        "asyncqlio.orm",
        "asyncqlio.orm.schema",
        "asyncqlio.orm.ddl",
        # namespace packages yay
        "asyncqlio.backends",
        # postgres backend
        "asyncqlio.backends.postgresql",
        # mysql backend
        "asyncqlio.backends.mysql",
        # sqlite3 backend
        "asyncqlio.backends.sqlite3"
    ],
    url="https://github.com/SunDwarf/asyncqlio",
    license="MIT",
    author="Laura Dickinson",
    author_email="l@veriny.tf",
    description="An asyncio ORM for Python 3.5+",
    long_description=Path(__file__).with_name("README.rst").read_text(encoding="utf-8"),
    setup_requires=[
        "setuptools_scm",
        "pytest-runner"
    ],
    install_requires=[
        "cached_property==1.3.0",
        "asyncio_extras==1.3.0"
    ],
    extras_require={
        "docs": [
            "sphinx>=1.5.0",
            "sphinxcontrib-asyncio",
            "guzzle_sphinx_theme"
        ],
        "postgresql": [
            "asyncpg>=0.12.0"
        ],
        "mysql": [
            "aiomysql>=0.0.9",
        ]
    },
    test_requires=[
        "pytest",
        "pytest-asyncio",
        "pytest-cov"
    ],
    python_requires=">=3.5.2",
    entry_points={
        "console_scripts": ["asql-migrate=asyncqlio.orm.ddl.migration_tool:cli"]
    }
)
