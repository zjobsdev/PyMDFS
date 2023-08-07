

PyMDFS
======

A high level and easy-to-use Micaps MDFS data online reader package.

It contains main features as following,

#. Online client to read data from GDS server
#. Read Micaps diamond (write support) and Micaps 4 Grid/Stations files.
#. Read satellite product data file (AWX)
#. Read weather radar mosaic product file (.LATLON)
#. Filter stations data or clip grid data
#. Major data structures are pandas.DataFrame/xarray.DataArray

README
^^^^^^

- `简体中文 <https://github.com/zjobsdev/pymdfs/blob/master/README.rst>`_

Install
^^^^^^^

use pip install pymdfs

.. code:: shell

    pip install pymdfs


Quick Start
^^^^^^^^^^^

Read data from Micaps GDS server
------------------------------------

The most useful class in pymdfs is MdfsClient, you can use it to fetch data
from GDS server, clip longitude and latitude extent.

**Key Point**

- The first argument of **MdfsClient** is GDS server address and port.
- **MdfsClient.sel** is the frontend interface to fetch data in GDS,
  using several arguments,

  - `datasource`, top directory name in GDS server
  - `inittime`, initial datetime of model or observation datetime,
  - `fh`, forecast hour of model, only valid for model data
  - `varname`, variable name, / joined middle directories
  - `level`, model pressure level, only valid for model data
  - `lat`, slice extent for latitude
  - `lon`, slice extent for longitude
  - `wildcard`, file name wildcard, runtime can be speedup if offered

Following is an example to fetch 0.125x0.125 ECMWF forecasted relative humidity field,
initial at 2023-02-20 20:00 (BT) and lead at 24 hours later.

.. code:: python

    from datetime import datetime
    from pymdfs import MdfsClient

    gds = MdfsClient('xxx.xxx.xxx.xxx:xxxx')
    dar = gds.sel('ECMWF_HR', datetime(2023, 2, 20, 20), fh=24, varname='RH',
                  level=850, lat=slice(20, 40), lon=slice(110, 130))
    print(dar)


Following is an example to fetch observational 24-hour station rainfall at 2023-02-20 20:00 (BT),
within the extent of 20N-40N,110E-130E.

.. code:: python

    from datetime import datetime
    from pymdfs import MdfsClient

    gds = MdfsClient('xxx.xxx.xxx.xxx:xxxx')
    df = gds.sel('SURFACE', datetime(2023, 2, 20, 20), varname='RAIN24_ALL_STATION',
                 lat=slice(20, 40), lon=slice(110, 130))
    print(df)


Command line procedures
^^^^^^^^^^^^^^^^^^^^^^^^^

1. client_query
----------------

usage:
    mdfs_query [-h] [-s SERVER] [-o LOGLEVEL] datasource

MDFS Data Query

positional arguments:
  datasource,            data source name

optional arguments:
    +----------------------------------+---------------------------------+
    | arguments                        | Description                     |
    +==================================+=================================+
    | -h, --help                       | show this help message and exit |
    +----------------------------------+---------------------------------+
    | -s SERVER, --server SERVER       | GDS server address              |
    +----------------------------------+---------------------------------+
    | -o LOGLEVEL, --loglevel LOGLEVEL | loglevel: 10, 20, 30, 40, 50    |
    +----------------------------------+---------------------------------+


Example:

.. code:: python

    client_query ECMWF_HR

2. client_dump
----------------

usage:
    mdfs_dump [-h] [-f FH] [-e OUTFILE] [-c COMPLEVEL] [-v VARNAME] [-x LON] [-y LAT] [-p LEVEL] [-t OFFSET_INITTIME] [--name_map NAME_MAP] [-s SERVER] [-o LOGLEVEL] datasource inittime

MDFS Data Dumper

positional arguments:
    +-------------+------------------------------------------------+
    | arguments   | Description                                    |
    +=============+================================================+
    | datasource  | data source name                               |
    +-------------+------------------------------------------------+
    | inittime    | model initial datetime or observation datetime |
    +-------------+------------------------------------------------+

optional arguments:
    +-------------------------------------------------------+-------------------------------------+
    | arguments                                             | Description                         |
    +=======================================================+=====================================+
    | -h, --help                                            | show this help message and exit     |
    +-------------------------------------------------------+-------------------------------------+
    | -f FH, --fh FH                                        | model forecast hour                 |
    +-------------------------------------------------------+-------------------------------------+
    | -e OUTFILE, --outfile OUTFILE                         | output netcdf file name             |
    +-------------------------------------------------------+-------------------------------------+
    | -c COMPLEVEL, --complevel COMPLEVEL                   | output netcdf4 compress level       |
    +-------------------------------------------------------+-------------------------------------+
    | -v VARNAME, --varname VARNAME                         | model variable names                |
    +-------------------------------------------------------+-------------------------------------+
    | -x LON, --lon LON                                     | longitude point or range            |
    +-------------------------------------------------------+-------------------------------------+
    | -y LAT, --lat LAT                                     | latitude point or range             |
    +-------------------------------------------------------+-------------------------------------+
    | -p LEVEL, --level LEVEL                               | pressure level point or range       |
    +-------------------------------------------------------+-------------------------------------+
    | -t OFFSET_INITTIME, --offset-inittime OFFSET_INITTIME | offset inittime (hours) to variable |
    +-------------------------------------------------------+-------------------------------------+
    | --name_map NAME_MAP                                   | map variable name to new            |
    +-------------------------------------------------------+-------------------------------------+
    | -s SERVER, --server SERVER                            | GDS server address                  |
    +-------------------------------------------------------+-------------------------------------+
    | -o LOGLEVEL, --loglevel LOGLEVEL                      | logger level in number              |
    +-------------------------------------------------------+-------------------------------------+


Example:

.. code:: shell

     client_dump ECMWF_HR 2023021920 -f 24 --level 500 -v RH,UGRD,VGRD,TMP,HGT -e ECMWF_HR.2023021920.nc


More details and features please go to the docs hosted at `readthedocs <www.pymdfs.readthedocs.org>`_ .
