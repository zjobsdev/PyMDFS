# -*- coding: utf-8 -*-
# @Author: wqshen
# @Email: wqshen91@gmail.com
# @Date: 2021/2/3 17:26
# @Last Modified by: wqshen

import os
import numpy as np
import pandas as pd
import xarray as xr
from typing import Optional, Union, cast
from datetime import datetime, timedelta
from dataclasses import dataclass, fields, astuple


@dataclass
class Diamond4Head:
    diamond: str
    dtype: int
    description: str
    year: int
    month: int
    day: int
    hour: int
    duration: int
    level: float
    xinterval: float
    yinterval: float
    startlon: float
    endlon: float
    startlat: float
    endlat: float
    xsize: int
    ysize: int
    lineinteravl: float
    startvalue: float
    endvalue: float
    smooth: float = 1
    boldvalue: float = 0

    def __post_init__(self):
        self._cast_fields_types()

    def _cast_fields_types(self):
        for field in fields(cast(dataclass, self)):
            field_value = getattr(self, field.name)
            setattr(self, field.name, field.type(field_value))


@dataclass
class Diamond4:
    """Micaps Diamond Data class

    From this class, one can read and write Micaps Diamond4 file

    Read Diamond4
    --------------
    >>> m4 = Diamond4(pathfile='../../sample_data/diamond_4/21032920.024')
    >>> m4.read()
    >>> print(m4)
    >>> print(m4.data.shape)
    >>> print(m4.to_xarray())

    Write Diamond4
    --------------
    >>> m4 = Diamond4()
    >>> m4.head = Diamond4Head('diamond', 4, '20年11月27日T63_500hPa温度120小时预报', 2020, 11, 27, 20, 120, 500,
    ...                         1.875, -1.875, 0, 180, 90, 0, 97, 49, 20, -300, 300, 1, 0)
    >>> m4.data = np.random.randint(-50, 10, size=(97, 49))
    >>> m4.write("20201127.120", fmt='%.2f')
    """
    head: Optional[Diamond4Head] = None
    data: Optional[np.ndarray] = None
    pathfile: Optional[str] = None
    missing_value: Optional[float] = None
    _ds: Optional[xr.DataArray] = None

    def __post_init__(self):
        if self.pathfile is not None:
            self.read(self.pathfile)

    def sel(self, **kwargs):
        """interface to call `xarray.DataArray.sel`_ method

        Parameters
        ----------
        kwargs (dict): other parameters used to process xarray.DataArray variable

        Returns
        -------
        dar (xarray.DataArray): sel variable
        """
        if self._ds.indexes['lat'].is_monotonic_decreasing:
            if 'lat' in kwargs and isinstance(kwargs.get('lat'), slice):
                lat = kwargs.get('lat')
                kwargs['lat'] = slice(lat.stop, lat.start, lat.step)
        dar = self._ds.sel(**kwargs)
        return dar

    def read(self, path_or_string: Union[str, bytes], encoding='GBK'):
        """Read micaps diamond 4 file

        Parameters
        ----------
        path_or_string : Union[str, bytes]
            file content str, bytes or path to file name
        encoding : str, Optional, default: 'GBK'
            encoding of output file
        """
        if isinstance(path_or_string, bytes):
            data = path_or_string.decode(encoding).split()
        elif os.path.isfile(path_or_string):
            with open(path_or_string, 'r', encoding=encoding) as f:
                data = f.read().split()
        else:
            data = path_or_string.split()
        headsize = len(fields(Diamond4Head))
        self.head = Diamond4Head(*data[:headsize])
        self.data = np.asfarray(data[headsize:]).reshape((self.head.ysize, self.head.xsize))
        self._ds = self.to_xarray(name='var')

    def write(self, pathfile: str, encoding: str = 'GBK', fmt: str = '%.2f'):
        """Write data into micaps diamond4 text file

        Parameters
        ----------
        pathfile : str
            path to file name
        encoding : str, Optional, default: 'GBK'
            encoding of output file
        fmt : Optional[str], default: '.5f'
            float format to save data
        """
        with open(pathfile, 'w', encoding=encoding) as f:
            head = tuple(map(str, astuple(self.head)))
            f.write("\t".join(head[:3]) + "\n")
            f.write("\t".join(head[3:9]) + "\n")
            f.write("\t".join(head[9:22]) + "\n")

            np.savetxt(f, self.data, fmt=fmt, delimiter='\t')

    def to_netcdf(self, pathfile: str, name: str, complevel: int = 5):
        """write data into NetCDF4 file

        Parameters
        ----------
        pathfile : str
            path to file name
        name : str
            variable name when pack data into NetCDF4 Variable
        complevel : Optional[int], default: 5
            Compress level when pack data into NetCDF4 dataset
        """
        data = self.to_xarray()
        # convert to dataset and save to netcdf file with encoding
        comp = dict(zlib=True, complevel=complevel)
        ds = data.to_dataset(name=name)
        encoding = {var: comp for var in ds.data_vars}
        ds.to_netcdf(pathfile, encoding=encoding)

    def from_xarray(self, darray: xr.DataArray):
        """convert xarray into Diamond4"""
        desc = getattr(darray, 'long_name', 'Unknown')
        time = getattr(darray, 'time', datetime(1900, 1, 1, 0))
        it = pd.to_datetime(time)
        duration = getattr(darray, 'step', 0)
        level = getattr(darray, 'level', 0)
        xinterval = darray.lon[1] - darray.lon[0]
        yinterval = darray.lat[1] - darray.lat[0]
        startlon = darray.lon.min()
        startlat = darray.lat.min()
        endlon = darray.lon.max()
        endlat = darray.lat.max()
        xsize, ysize = darray.shape[-1], darray.shape[-2]
        startvalue = darray.min().floor()
        endvalue = darray.max().ceil()
        lineinteravl = (endvalue - startvalue) / 20.
        self.head = Diamond4Head('diamond', 4, desc, it.year, it.month, it.day, it.hour, duration,
                                 level, xinterval, yinterval, startlon, endlon, startlat, endlat,
                                 xsize, ysize, lineinteravl, startvalue, endvalue, )
        self.data = darray.values
        self._ds = darray

    def to_xarray(self, name=None):
        """Convert instance into xarray.DataArray

        Returns
        -------
        data: xr.DataArray
            converted data
        """
        lons = np.linspace(self.head.startlon, self.head.endlon, self.head.xsize)
        lats = np.linspace(self.head.startlat, self.head.endlat, self.head.ysize)

        if self.head.year < 50:
            inittime = datetime(2000 + self.head.year, self.head.month,
                                self.head.day, self.head.hour)
        elif self.head.year < 100:
            inittime = datetime(1900 + self.head.year, self.head.month,
                                self.head.day, self.head.hour)
        else:
            inittime = datetime(self.head.year, self.head.month,
                                self.head.day, self.head.hour)
        fh = self.head.duration
        leadtime = inittime + timedelta(hours=fh)
        data = xr.DataArray(self.data[None, ...], dims=('time', 'lat', 'lon'),
                            coords={'time': [leadtime], 'lat': lats, 'lon': lons},
                            name=name)
        # To mask potential missing values
        if self.missing_value is not None:
            data = data.where(data == self.missing_value)
        # Append extra attributes to netcdf varibale
        data.attrs['description'] = self.head.description
        data.attrs['inittime'] = inittime
        data.attrs['fh'] = fh

        if self.pathfile is not None:
            data.attrs['raw'] = self.pathfile
        return data

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close open dataset"""
        del self.pathfile, self.data, self.head, self._ds

    def __repr__(self):
        """print"""
        return self.head.__repr__()
