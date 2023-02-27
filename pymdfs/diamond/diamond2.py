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
class Diamond2Head:
    diamond: str
    dtype: int
    description: str
    year: int
    month: int
    day: int
    hour: int
    level: int
    nrec: int

    def __post_init__(self):
        self._cast_fields_types()

    def _cast_fields_types(self):
        for field in fields(cast(dataclass, self)):
            field_value = getattr(self, field.name)
            setattr(self, field.name, field.type(field_value))


@dataclass
class Diamond2:
    """Micaps Diamond 2 (高空全要素填图) Data class

    From this class, one can read and write Micaps Diamond4 file

    Read Diamond2
    --------------
    >>> m2 = Diamond2(pathfile='../../sample_data/diamond_2/18091608.000')
    >>> m2.read()
    >>> print(m2)
    >>> print(m2.data.shape)
    >>> print(m2.to_frame())

    Write Diamond2
    --------------
    >>> m2 = Diamond2()
    >>> m2.head = Diamond2Head(['diamond', 2, '21年03月07日20时500百帕高空观测', 2021, 3, 7, 20, 500, 2])
    >>> m2.data = [[3005, -1.17, 60.13, 84, 1, 547, -29, 16, 310, 13],
    ...            [3808, -5.31, 50.22, 88, 1, 557, -26, 22,   5, 10]]
    >>> m2.write("21030720.000")
    """
    head: Optional[Diamond2Head] = None
    data: Optional[np.ndarray] = None
    pathfile: Optional[str] = None
    missing_value: Optional[float] = None
    _ds: Optional[pd.DataFrame] = None

    def __post_init__(self):
        if self.pathfile is not None:
            self.read(self.pathfile)

    def sel(self, element: Any = None, stid: Any = None, lon: Any = None, lat: Any = None,
            height: Any = None, level: Any = None, query: str = None):
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
        level: Any
            select data within given level range
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

        for slice_, ele_ in zip((stid, lon, lat, height, level), self.index_cols):
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
        """Read micaps diamond 2 file

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

        headsize = len(fields(Diamond2Head))
        self.head = Diamond2Head(*data[:headsize])
        ncols = len(data[headsize:]) / self.head.nrec
        if not ncols.is_integer():
            raise Exception("Data can't be splitted into integer columns.")
        self.data = np.asfarray(data[headsize:]).reshape((self.head.nrec, int(ncols)))
        self._ds = self.to_frame()

    def write(self, pathfile: str, encoding: str = 'GBK',
              fmt: str = '%6i%8.2f%8.2f%5i%5i%5i%5i%5i%5i%5i'):
        """Write data into micaps diamond4 text file

        Parameters
        ----------
        pathfile : str
            path to file name
        encoding : str, Optional, default: 'GBK'
            encoding of output file
        fmt : Optional[str], default: '%.2f'
            float format to save data
        """
        with open(pathfile, 'w', encoding=encoding) as f:
            head = tuple(map(str, astuple(self.head)))
            f.write("\t".join(head[:3]) + "\n")
            f.write("\t".join(head[3:]) + "\n")

            np.savetxt(f, self.to_numpy(), fmt=fmt)

    @property
    def dtype(self):
        """column data type"""
        dtypes = [('stid', 'i4'), ('lon', 'f4'), ('lat', 'f4'), ('height', 'f4'), ('level', 'i1'),
                  ('h', 'i2'), ('t', 'i2'), ('ttd', 'i2'), ('wd', 'i2'), ('ws', 'i2')]
        return np.dtype(dtypes)

    @property
    def names(self):
        """column-name dictionary"""
        names = [('stid', '区站号'), ('lon', '经度'), ('lat', '纬度'), ('height', '拔海高度'),
                 ('level', '站点级别'), ('h', '高度'), ('t', '温度'), ('ttd', '露点温度差'),
                 ('wd', '风向'), ('ws', '风速')]
        return dict(names)

    @property
    def index_cols(self):
        """base index columns, keep in frame when select data"""
        return ['stid', 'lon', 'lat', 'height', 'level']

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

    def sel(self, **kwargs):
        """interface to select variable from file by given more filter

        Parameters
        ----------
        kwargs (dict): other parameters used to filter data

        Returns
        -------
        var (pandas.DataFrame): Readed variable in pandas.DataFrame
        """
        if len(kwargs) > 0:
            conditions = True
            for k, v in kwargs.items():
                if k in ('stid', 'level'):
                    conditions = conditions & (self._ds[k] == v)
                elif k in ('lat', 'lon', 'height'):
                    conditions = conditions & (self._ds[k].between(v.start, v.stop))
            return self._ds.loc[conditions]
        else:
            return self._ds