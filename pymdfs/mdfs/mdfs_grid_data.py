# -*- coding: utf-8 -*-
# @Author: wqshen
# @Email: wqshen91@gmail.com
# @Date: 2023/2/9 22:07
# @Last Modified by: wqshen

import numpy as np
import xarray as xr
from typing import Optional, Union, cast
from struct import unpack
from datetime import datetime, timedelta
from dataclasses import dataclass, fields


@dataclass
class MdfsGridHead:
    discriminator: str
    dtype: int
    modelName: str
    element: str
    description: str
    level: float
    year: int
    month: int
    day: int
    hour: int
    timezone: int
    period: int
    startLongitude: float
    endLongitude: float
    longitudeGridSpace: float
    latitudeGridNumber: int
    startLatitude: float
    endLatitude: float
    latitudeGridSpace: float
    longitudeGridNumber: int
    isolineStartValue: float
    isolineEndValue: float
    isolineSpace: float
    Extent: str

    def __post_init__(self):
        self._cast_fields_types()

    def _cast_fields_types(self):
        for field in fields(cast(dataclass, self)):
            field_value = getattr(self, field.name)
            if isinstance(field_value, bytes):
                field_value = field_value.decode('GBK').rstrip('\x00')
            setattr(self, field.name, field.type(field_value))


@dataclass
class MdfsGridData(object):
    head: Optional[MdfsGridHead] = None
    data: Optional[Union[np.ndarray, dict]] = None
    pathfile: Optional[str] = None
    missing_value: Optional[float] = None
    _ds: Optional[Union[xr.DataArray, xr.Dataset]] = None

    def __post_init__(self):
        if self.pathfile is not None:
            self.read(self.pathfile)

    def sel(self, **kwargs):
        """interface to call xarray.DataArray/xarray.Dataset sel method

        Parameters
        ----------
        kwargs (dict): other parameters used to process xarray.DataArray/xarray.Dataset variable

        Returns
        -------
        dar (xarray.DataArray, xarray.Dataset): sel variable
        """
        if self._ds.indexes['lat'].is_monotonic_decreasing:
            if 'lat' in kwargs and isinstance(kwargs.get('lat'), slice):
                lat = kwargs.get('lat')
                kwargs['lat'] = slice(lat.stop, lat.start, lat.step)
        dar = self._ds.sel(**kwargs)
        return dar

    def read(self, path_or_bytes: Union[str, bytes]):
        def read_block(bytes_arr, p: int = 0):
            head_data = unpack('=4sh20s50s30sfiiiiiifffifffifff100s', bytes_arr[p:p + 278])
            head = MdfsGridHead(*head_data)
            n_points = head.latitudeGridNumber * head.longitudeGridNumber
            body_data = np.frombuffer(bytes_arr[p + 278:p + 278 + n_points * 4],
                                      '{}f'.format(n_points))
            data = np.array(body_data).reshape(head.longitudeGridNumber,
                                               head.latitudeGridNumber)
            if head.dtype == 11:
                p = 278 + data.size * 4
                block_size = p + data.size * 4
                body_data_angle = unpack('{}f'.format(data.size), bytes_array[p:block_size])
                data_angle = np.array(body_data_angle).reshape(head.longitudeGridNumber,
                                                               head.latitudeGridNumber)
                data = {'magnitude': data, 'angle': data_angle}
            return head, data

        if isinstance(path_or_bytes, str):
            with open(path_or_bytes, 'rb') as f:
                bytes_array = f.read()
        else:
            bytes_array = path_or_bytes
        self.head, self.data = read_block(bytes_array)
        nbytes = len(bytes_array)
        size = self.head.latitudeGridNumber * self.head.longitudeGridNumber
        block_size = 278 + size * 4 * (2 if self.head.dtype == 11 else 1)
        if nbytes > block_size and nbytes % block_size == 0:
            heads = [self.head]
            datas = [self.data]
            nblocks = int(nbytes / block_size)
            _ds = [self.to_xarray(name=self.head.element)]
            for i in range(1, nblocks):
                self.head, self.data = read_block(bytes_array[block_size * i:block_size * (i + 1)])
                heads.append(self.head)
                datas.append(self.data)
                _ds.append(self.to_xarray(name=self.head.element))
            self.data = datas
            self.head = heads
            self._ds = xr.concat(_ds, dim=xr.DataArray(range(len(_ds)), dims='number')).transpose('time', ...)
        else:
            self._ds = self.to_xarray(name=self.head.element)
        return self.data

    def to_xarray(self, name=None):
        def set_mask_and_attrs(data):
            # To mask potential missing values
            if self.missing_value is not None:
                data = data.where(data == self.missing_value)

            # Append extra attributes to netcdf varibale
            data = data.assign_coords(inittime=xr.DataArray([inittime], dims='time'))
            data.attrs['model'] = self.head.modelName
            data.attrs['description'] = self.head.description
            # data.attrs['inittime'] = f"{inittime:%Y-%m-%d %H:%M}"
            data.attrs['fh'] = fh

            if self.pathfile is not None:
                data.attrs['raw'] = self.pathfile
            return data

        head = self.head
        lons = np.linspace(head.startLongitude, head.endLongitude, head.latitudeGridNumber)
        lats = np.linspace(head.startLatitude, head.endLatitude, head.longitudeGridNumber)

        inittime = datetime(head.year, head.month, head.day, head.hour)
        fh = self.head.period
        leadtime = inittime + timedelta(hours=fh)
        dims = ('time', 'lat', 'lon')
        coords = {'time': [leadtime], 'lat': lats, 'lon': lons}
        if isinstance(self.data, dict):
            data_u = self.data['magnitude'] * np.cos(np.deg2rad(self.data['angle']))
            data_v = self.data['magnitude'] * np.sin(np.deg2rad(self.data['angle']))
            data_u = xr.DataArray(data_u[None, ...], dims=dims, coords=coords, name=name + '_U')
            data_v = xr.DataArray(data_v[None, ...], dims=dims, coords=coords, name=name + '_V')
            data = xr.merge([set_mask_and_attrs(data_u), set_mask_and_attrs(data_v)])
        else:
            data = xr.DataArray(self.data[None, ...], dims=dims, coords=coords, name=name)
            data = set_mask_and_attrs(data)

        return data

    def to_diamond(self, pathfile: str):
        from ..diamond.diamond4 import Diamond4, Diamond4Head

        head = self.head
        m4 = Diamond4()
        m4.head = Diamond4Head('diamond', head.dtype,
                               head.description, head.year, head.month,
                               head.day, head.hour, head.period, head.level,
                               head.longitudeGridSpace, head.latitudeGridSpace,
                               head.startLongitude, head.endLongitude,
                               head.startLatitude, head.endLatitude,
                               head.latitudeGridNumber, head.longitudeGridNumber,
                               20, -300, 300, 1, 0)
        m4.data = self._ds.squeeze().values
        m4.write(pathfile, fmt='%.2f')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close open dataset"""
        del self.pathfile, self.data, self.head, self._ds

    def __repr__(self) -> str:
        """print"""
        return self.head.__repr__()
