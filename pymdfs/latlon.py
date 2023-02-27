# -*- coding: utf-8 -*-
# @Author: wqshen
# @Email: wqshen91@gmail.com
# @Date: 2023/2/9 22:07
# @Last Modified by: wqshen

import numpy as np
import xarray as xr
from typing import Optional, Union, cast
from struct import unpack, calcsize
from datetime import datetime, timedelta
from dataclasses import dataclass, fields


@dataclass
class LatLonHead:
    dataname: str
    varname: str
    units: str
    label: int
    unitlen: int
    slat: float
    wlon: float
    nlat: float
    elon: float
    clat: float
    clon: float
    rows: int
    cols: int
    dlat: float
    dlon: float
    nodata: float
    levelbytes: int
    levelnum: int
    amp: int
    compmode: int
    dates: int
    seconds: int
    min_value: int
    max_value: int
    reserved: list

    @property
    def dtype(self) -> str:
        return '=128s32s16sHhffffffiifffihhhHihh6h'

    def __post_init__(self):
        self._cast_fields_types()

    def _cast_fields_types(self):
        for field in fields(cast(dataclass, self)):
            field_value = getattr(self, field.name)
            if isinstance(field_value, bytes):
                field_value = field_value.rstrip(b'\x00\xfe').decode('utf8')
            setattr(self, field.name, field.type(field_value))


@dataclass
class LatLon(object):
    head: Optional[LatLonHead] = None
    data: Optional[Union[np.ndarray, dict]] = None
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

    def read(self, path_or_bytes: Union[str, bytes]):
        """read LatLon file or bytes

        Parameters
        ----------
        path_or_bytes: path to latlon file or data in bytes
        """
        if isinstance(path_or_bytes, str):
            with open(path_or_bytes, 'rb') as f:
                bytes_array = f.read()
        else:
            bytes_array = path_or_bytes
            print(type(path_or_bytes))
        head_type = '=128s32s16sHhffffffiifffihhhHihh6h'
        p = calcsize(head_type)
        head_data = unpack(head_type, bytes_array[:p])
        self.head = LatLonHead(*head_data[:-6], list(head_data[-6:]))

        body_type = f'{self.head.levelbytes // 2}h'
        body_data = np.frombuffer(bytes_array[p:p + calcsize(body_type)], dtype=body_type)[0]
        if self.head.compmode != 0:
            data = self.decompress(body_data)
        else:
            data = np.array(body_data).reshape(self.head.rows, self.head.cols)
        data[data >= self.head.min_value] *= 1.0 / self.head.amp
        data[data < 0] = np.nan
        self.data = data
        self._ds = self.to_xarray()

    def decompress(self, buf: np.ndarray) -> np.ndarray:
        """decompress data

        Parameters
        ----------
        buf: np.ndarray
            compressed data

        Returns
        -------
        data: np.ndarray
            decompressed data
        """
        head = self.head
        data = np.ones((head.rows, head.cols))
        data *= int(head.nodata * head.amp + 0.5)
        y, x, n = buf[:3]
        buf = buf[3:]
        while y >= 0 and x >= 0 and n > 0:
            data[y, x:x + n] = buf[:n]
            buf = buf[n:]
            y, x, n = buf[:3]
            buf = buf[3:]
        return data

    def to_xarray(self) -> xr.DataArray:
        head = self.head
        lons = np.linspace(head.wlon, head.elon - head.dlon, head.cols)
        lats = np.linspace(head.slat + head.dlat, head.nlat, head.rows)
        time = datetime(1970, 1, 1) + timedelta(days=int(head.dates) - 1, seconds=int(head.seconds))
        attrs = {
            k: getattr(head, k)
            for k in ('dataname', 'varname', 'units', 'label',
                      'unitlen', 'dates', 'seconds', 'levelnum')
        }
        if self.pathfile is not None:
            attrs['raw'] = self.pathfile

        name = head.varname
        dims = ('time', 'lat', 'lon')
        coords = {'time': [time], 'lat': lats, 'lon': lons}
        data = xr.DataArray(self.data[None, ...], dims=dims, coords=coords, name=name, attrs=attrs)

        return data

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close open dataset"""
        del self.pathfile, self.data, self.head, self._ds

    def __repr__(self) -> str:
        """print"""
        return self.head.__repr__()
