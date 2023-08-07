Command line procedures
=======================

1. mdfs_query
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

    mdfs_query ECMWF_HR

2. mdfs_dump
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

     mdfs_dump ECMWF_HR 2023021920 -f 24 --level 500 -v RH,UGRD,VGRD,TMP,HGT -e ECMWF_HR.2023021920.nc