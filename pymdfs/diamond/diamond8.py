# -*- coding: utf-8 -*-
# @Author: wqshen
# @Email: wqshen91@gmail.com
# @Date: 2019/1/16 12:06
# @Last Modified by: wqshen

import os
import numpy as np
import pandas as pd
from typing import Optional, Union, Any, cast
from dataclasses import dataclass, fields, astuple
from numpy.lib.recfunctions import unstructured_to_structured


@dataclass
class Diamond8Head:
    diamond: str
    dtype: int
    description: str
    year: int
    month: int
    day: int
    hour: int
    duration: int
    nrec: int

    def __post_init__(self):
        self._cast_fields_types()

    def _cast_fields_types(self):
        for field in fields(cast(dataclass, self)):
            field_value = getattr(self, field.name)
            setattr(self, field.name, field.type(field_value))


@dataclass
class Diamond8:
    """Micaps Diamond 8 Data class

    From this class, one can read and write Micaps Diamond 8 file

    Read Diamond 8
    --------------
    >>> m8 = Diamond8(pathfile='../../sample_data/diamond_1/18091608.000')
    >>> m8.read()
    >>> print(m8)
    >>> print(m8.data.shape)
    >>> print(m8.to_frame())

    Write Diamond 8
    ---------------
    >>> m8 = Diamond8()
    >>> m8.head = Diamond8Head('diamond', 8, '18年09月16日08时48小时城市预报', 2018, 9, 16, 8, 48, 4)
    >>> m8.data = np.array([
    ...    [50953, 126.77, 45.75, 143, 0, 9999, 0, 16, 4],
    ...    [54161, 125.22, 43.90, 238, 0, 9999, 0, 13, 3],
    ...    [54342, 123.43, 41.77, 43, 0, 9999, 0, 12, 1],
    ...    [54527, 117.17, 39.09, 5, 0, 9999, 0, 5, 4]
    ...             ])
    >>> m8.write("18091608.048.000")
    """
    head: Optional[Diamond8Head] = None
    data: Optional[np.ndarray] = None
    pathfile: Optional[str] = None
    missing_value: Optional[float] = None
    encoding: Optional[str] = 'GBK'
    _ds: Optional[pd.DataFrame] = None

    def __post_init__(self):
        if self.pathfile is not None:
            self.read(self.pathfile)

    def sel(self, element: Any = None, stid: Any = None, lon: Any = None, lat: Any = None,
            height: Any = None, query: str = None):
        """interface to select data

        Parameters
        ----------
        element: Any
            select element in columns
        stid: Any
            select data at given station or station list
        lon: Any
            select data within given longitude range
        lat: Any
            select data within given latitude range
        height: Any
            select data within given height range
        query: str
            pandas.DataFrame.query string to filter data

        Returns
        -------
        df (pd.DataFrame): selected data
        """
        df = self._ds
        if element is not None:
            if isinstance(element, str):
                df = df[[*self.index_cols, element]]
            elif isinstance(element, (list, tuple, np.ndarray, pd.Series)):
                df = df[[*self.index_cols, *element]]

        for slice_, ele_ in zip((stid, lon, lat, height), self.index_cols):
            if slice_ is not None:
                if isinstance(slice_, slice):
                    df = df[(df[ele_] >= slice_.start) & (df[ele_] <= slice_.stop)]
                elif isinstance(slice_, (list, tuple, np.ndarray, pd.Series)):
                    df = df[df[ele_].isin(slice_)]
                elif isinstance(slice_, (str, int, float)):
                    df = df[df[ele_] == slice_]
                else:
                    raise NotImplementedError(f"{ele_} must be type of int, float, str, list, "
                                              f"tuple, numpy.ndarray, pandas.Series")

        if query is not None:
            df = df.query(query)
        return df

    def read(self, path_or_string: Union[str, bytes], encoding='GBK'):
        """Read micaps diamond 8 file

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

        headsize = len(fields(Diamond8Head))
        self.head = Diamond8Head(*data[:headsize])

        ncols = len(data[headsize:]) / self.head.nrec
        if not ncols.is_integer():
            raise Exception("Data can't be splitted into integer columns.")
        self.data = np.asfarray(data[headsize:]).reshape((self.head.nrec, int(ncols)))
        self._ds = self.to_frame()

    def write(self, pathfile: str, fmt: str = '%5i%8.2f%8.2f%5i%5i%5i%5i%5i%5i%5i%5i%5i'):
        """Write data into micaps diamond4 text file

        Parameters
        ----------
        pathfile : str
            path to file name
        fmt : Optional[str], default: '%.2f'
            float format to save data
        """
        with open(pathfile, 'w', encoding=self.encoding) as f:
            head = tuple(map(str, astuple(self.head)))
            f.write("\t".join(head[:3]) + "\n")
            f.write("\t".join(head[3:]) + "\n")

            data = self.to_numpy()
            if len(fmt.split('%')) == data.shape[-1] - 3:
                fmt = '%'.join(fmt.split('%')[:-3])
            np.savetxt(f, data, fmt=fmt)

    @property
    def dtype(self):
        """column data type"""
        dtypes = [('stid', 'i4'), ('lon', 'f4'), ('lat', 'f4'), ('height', 'f4'),
                  ('ww1', 'i2'), ('windr1', 'i2'), ('winds1', 'i2'),
                  ('tmin', 'i2'), ('tmax', 'i2'),
                  ('ww2', 'i2'), ('windr2', 'i2'), ('winds2', 'i2')]

        if self.data.shape[-1] == len(dtypes) - 3:
            dtypes = dtypes[:-3]
        return np.dtype(dtypes)

    @property
    def names(self):
        """column-name dictionary"""
        names = [('stid', '区站号'), ('lon', '经度'), ('lat', '纬度'), ('height', '拔海高度'),
                 ('ww1', '天气现象1'), ('windr1', '风向'), ('winds1', '风速'),
                 ('tmin', '最低温度'), ('tmax', '最高温度'),
                 ('ww2', '天气现象2'), ('windr2', '风向'), ('winds2', '风速')]
        return dict(names)

    @property
    def index_cols(self):
        """base index columns, keep in frame when select data"""
        return ['stid', 'lon', 'lat', 'height']

    def to_numpy(self):
        """Convert instance into numpy array with dtypes

        Returns
        -------
        data: np.ndarray
            converted data
        """
        return unstructured_to_structured(self.data, dtype=self.dtype)

    def to_frame(self):
        """Convert instance into pandas.DataFrame

        Returns
        -------
        data: xr.DataArray
            converted data
        """
        data = pd.DataFrame.from_records(self.to_numpy())
        return data

    def __repr__(self):
        """print"""
        return self.head.__repr__()

    def __getitem__(self, stid):
        """get data of station"""
        if isinstance(stid, str):
            return self.data[stid]
        elif isinstance(stid, (tuple, list, np.ndarray)):
            return self.data[np.isin(self.data['stid'], stid)]
        else:
            TypeError("stid must be type of str, tuple or list")
