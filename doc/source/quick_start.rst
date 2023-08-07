Quick Start
===========

Read data from Micaps GDS server
--------------------------------

The most useful class in pymdfs is MdfsClient, you can use it to fetch data
from GDS server, clip longitude and latitude extent.

**Key Point**

- The first argument of `MdfsClient`_ is GDS server address and port.
- `MdfsClient.sel`_ is the frontend interface to fetch data in GDS,
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

    gds = MdfsClient('xx.xxx.xxx.xxx:xxxx')
    dar = gds.sel('ECMWF_HR', datetime(2023, 2, 20, 20), fh=24, varname='RH',
                  level=850, lat=slice(20, 40), lon=slice(110, 130))
    print(dar)


Following is an example to fetch observational 24-hour station rainfall at 2023-02-20 20:00 (BT),
within the extent of 20N-40N,110E-130E.

.. code:: python

    from datetime import datetime
    from pymdfs import MdfsClient

    gds = MdfsClient('xx.xxx.xxx.xxx:xxxx')
    df = gds.sel('SURFACE', datetime(2023, 2, 20, 20), varname='RAIN24_ALL_STATION',
                 lat=slice(20, 40), lon=slice(110, 130))
    print(df)
