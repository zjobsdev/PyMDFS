# -*- coding: utf-8 -*-
# @Author: wqshen
# @Email: wqshen91@gmail.com
# @Date: 2021/2/3 17:26
# @Last Modified by: wqshen

import os
import numpy as np
import xarray as xr
from typing import Optional, Union, cast
from datetime import datetime
from dataclasses import dataclass, fields, astuple


@dataclass
class Diamond11Head:
    diamond: str
    dtype: int
    description: str
    year: int
    month: int
    day: int
    hour: int
    duration: int
    level: int
    xinterval: float
    yinterval: float
    startlon: float
    endlon: float
    startlat: float
    endlat: float
    xsize: int
    ysize: int

    def __post_init__(self):
        self._cast_fields_types()

    def _cast_fields_types(self):
        for field in fields(cast(dataclass, self)):
            field_value = getattr(self, field.name)
            setattr(self, field.name, field.type(field_value))


@dataclass
class Diamond11:
    """Micaps Diamond Data class

    From this class, one can read and write Micaps Diamond11 file

    Read Diamond11
    --------------
    >>> m11 = Diamond11(pathfile='../../sample_data/diamond_11/21032920.024')
    >>> m11.read()
    >>> print(m11)
    >>> print(m11.data.shape)
    >>> print(m11.to_xarray())

    Write Diamond11
    --------------
    >>> m11 = Diamond11()
    >>> m11.head = Diamond11Head('diamond', 11, '20年11月27日T63_500hPa风场120小时预报',
    ...                          2020, 11, 27, 20, 120, 500,
    ...                          1.875, -1.875, 0, 180, 90, 0, 97, 49, 20, -300, 300, 1, 0)
    >>> m11.data = np.random.randint(-50, 10, size=(97, 49))
    >>> m11.write("20201127.120", fmt='%.5f')
    """
    head: Optional[Diamond11Head] = None
    data: Optional[np.ndarray] = None
    pathfile: Optional[str] = None
    missing_value: Optional[float] = None
    _ds: Optional[Union[xr.DataArray, xr.Dataset]] = None

    def __post_init__(self):
        if self.pathfile is not None:
            self.read(self.pathfile)

    def sel(self, **kwargs):
        """interface to call `xarray.Dataset.sel`_ method

        Parameters
        ----------
        kwargs (dict): other parameters used to process xarray.Dataset variable

        Returns
        -------
        dar (xarray.Dataset): sel dataset
        """
        if self._ds.indexes['lat'].is_monotonic_decreasing:
            if 'lat' in kwargs and isinstance(kwargs.get('lat'), slice):
                lat = kwargs.get('lat')
                kwargs['lat'] = slice(lat.stop, lat.start, lat.step)
        dar = self._ds.sel(**kwargs)
        return dar

    def read(self, path_or_string: Union[str, bytes], encoding='GBK'):
        """Read micaps diamond 11 file

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

        headsize = len(fields(Diamond11Head))
        self.head = Diamond11Head(*data[:headsize])
        self.data = np.asfarray(data[headsize:]).reshape((2, self.head.ysize, self.head.xsize))
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
            f.write("\t".join(head[9:]) + "\n")

            np.savetxt(f, self.data[0], fmt=fmt, delimiter='\t')
            np.savetxt(f, self.data[1], fmt=fmt, delimiter='\t')

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

    def to_xarray(self, name=('u', 'v')):
        """Convert instance into xarray.DataArray

        Returns
        -------
        data: xr.DataArray
            converted data
        """
        lons = np.linspace(self.head.startlon, self.head.endlon, self.head.xsize)
        lats = np.linspace(self.head.startlat, self.head.endlat, self.head.ysize)
        var_x = xr.DataArray(self.data[0], dims=('lat', 'lon'), coords={'lat': lats, 'lon': lons},
                             name=name[0])
        var_y = xr.DataArray(self.data[1], dims=('lat', 'lon'), coords={'lat': lats, 'lon': lons},
                             name=name[1])

        # To mask potential missing values
        if self.missing_value is not None:
            var_x = var_x.where(var_x == self.missing_value)
            var_y = var_y.where(var_y == self.missing_value)
        # Append extra attributes to netcdf varibale
        var_x.attrs['description'] = self.head.description + "_" + name[0]
        var_y.attrs['description'] = self.head.description + "_" + name[1]
        var_x.attrs['inittime'] = datetime(self.head.year, self.head.month, self.head.day,
                                           self.head.hour)
        var_y.attrs['inittime'] = datetime(self.head.year, self.head.month, self.head.day,
                                           self.head.hour)
        var_x.attrs['fh'] = self.head.duration
        var_y.attrs['fh'] = self.head.duration

        if self.pathfile is not None:
            var_x.attrs['raw'] = self.pathfile
            var_y.attrs['raw'] = self.pathfile
        var = xr.merge([var_x, var_y])
        return var

    def __repr__(self):
        """print"""
        return self.head.__repr__()
