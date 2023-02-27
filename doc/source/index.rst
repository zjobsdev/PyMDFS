.. PyMDFS documentation master file, created by
   sphinx-quickstart on Sun Feb 26 11:27:14 2023.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. image::  _static/distribution_128px.ico

Welcome to PyMDFS's documentation!
==================================

A high level and easy-to-use Micaps MDFS data online reader package.

It contains main features as following,

#. Online client to read data from GDS server
#. Read Micaps diamond (write support) and Micaps 4 Grid/Stations files.
#. Read satellite product data file (AWX)
#. Read weather radar mosaic product file (.LATLON)
#. Filter stations data or clip grid data
#. Major data structures are pandas.DataFrame/xarray.DataArray

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   install
   quick_start
   scripts
   datasource
   api


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
