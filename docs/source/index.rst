.. asyncqlio documentation master file, created by
   sphinx-quickstart on Wed Apr 26 07:56:32 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to asyncqlio's documentation!
=====================================

**asyncqlio** is a Python 3.5+ :ref:`ORM` for SQL Relational Databases, that
uses :mod:`asyncio` for an async interface.

You can install the latest version of asyncqlio from PyPI with pip:

.. code-block:: bash

   $ pip install asyncqlio

You can also install the development version of asyncqlio from Git:

.. code-block:: bash

   $ pip install git+https://github.com/SunDwarf/asyncqlio.git

**The development version is NOT guarenteed to be stable, or even working.**  
Of course, asyncqlio by itself is useless without a driver to connect to a
database. asyncqlio comes with modules that use other libraries as backend
drivers to connect to these servers:

   - PostgreSQL (asyncpg, aiopg): :ref:`driver-psql`
   - MySQL (aiomysql): :ref:`driver-mysql`
   - SQLite3 (sqlite3): :ref:`driver-sqlite3`

Other databases may be supported in the future. 

Contents
========

.. toctree::
   :maxdepth: 2
   :caption: Getting Started

       gs/connecting

.. toctree::
   :maxdepth: 2
   :caption: Low-level API

   lowlvl/basics

.. toctree::
   :maxdepth: 2
   :caption: High-level API


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
