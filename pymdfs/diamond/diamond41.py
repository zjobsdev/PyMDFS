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
class Diamond41Head:
    diamond: str
    dtype: int
    description: str
    year: int
    month: int
    day: int
    hour: int
    nrec: int

    def __post_init__(self):
        self._cast_fields_types()

    def _cast_fields_types(self):
        for field in fields(cast(dataclass, self)):
            field_value = getattr(self, field.name)
            setattr(self, field.name, field.type(field_value))


@dataclass
class Diamond41:
    """Micaps Diamond 41 Data class

    From this class, one can read and write Micaps Diamond 41 file

    Read Diamond 41
    ---------------
    >>> m41 = Diamond41(pathfile='../../sample_data/diamond_41/LIGHT_MICAPS3_2008062417_COLLECT.TXT')
    >>> m41.read()
    >>> print(m41)
    >>> print(m41.data.shape)
    >>> print(m41.to_frame())

    Write Diamond 41
    ----------------
    >>> m41 = Diamond41()
    >>> m41.head = Diamond41Head('diamond', 41, '2008年06月24日17时闪电监测资料', 2008, 1, 8, 8)
    >>> m41.data = np.array([
    ...    [1, 200806241711032294853, 9999, 9999, 114.8664, 26.57132, 9999, 9999, 9999, 9999, -29.16124, 9999, 74.63686, 6, -7.178152],
    ...    [2, 200806241711036436922, 9999, 9999, 106.4083, 23.33219, 9999, 9999, 9999, 9999, -51.30987, 9999, 75.52758, 6, -9.658328],
    ...    [3, 200806241711045918992, 9999, 9999, 121.0393, 28.38015, 9999, 9999, 9999, 9999, -28.92107, 9999, 74.09919, 6, -6.855365],
    ...    [4, 200806241711049273112, 9999, 9999, 102.8865, 25.18426, 9999, 9999, 9999, 9999, -205.6933, 9999, 0, 5, -33.32752],
    ...    [5, 20080624171105456599, 9999, 9999, 110.0666, 24.17627, 9999, 9999, 9999, 9999, -46.67589, 9999, 0, 2, -13.2767],
    ...    [6, 200806241711053232880, 9999, 9999, 109.5917, 24.25497, 9999, 9999, 9999, 9999, -31.43308, 9999, 0, 2, -10.58799],
    ...             ])
    >>> m41.write("LIGHT_MICAPS3_2008010808_COLLECT.TXT")
    """
    head: Optional[Diamond41Head] = None
    data: Optional[np.ndarray] = None
    pathfile: Optional[str] = None
    missing_value: Optional[float] = None
    _ds: Optional[pd.DataFrame] = None

    def __post_init__(self):
        if self.pathfile is not None:
            self.read(self.pathfile)

    def sel(self, element: Any = None, lon: Any = None, lat: Any = None, height: Any = None,
            query: str = None):
        """interface to select data

        Parameters
        ----------
        element: Any
            select element in columns
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

        for slice_, ele_ in zip((lon, lat, height), ('lon', 'lat', 'height')):
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
        """Read micaps diamond 5 file

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

        headsize = len(fields(Diamond41Head))
        self.head = Diamond41Head(*data[:headsize])

        ncols = len(data[headsize:]) / self.head.nrec
        if not ncols.is_integer():
            raise Exception("Data can't be splitted into integer columns.")
        self.data = np.asarray(data[headsize:]).reshape((self.head.nrec, int(ncols)))
        self._ds = self.to_frame()

    def write(self, pathfile: str, encoding: str = 'GBK',
              fmt: str = '%5i %22s %5i %5i %9.4f %9.4f %9.4f %5i %9.4f %9.4f %9.5f %9.4f %9.5f %5i %9.5f'):
        """Write data into micaps diamond4 text file

        Parameters
        ----------
        pathfile : str
            path to file name
        encoding : str, Optional, default: 'GBK'
            encoding of output file
        fmt : Optional[str]
            float format to save data
        """
        with open(pathfile, 'w', encoding=encoding) as f:
            head = tuple(map(str, astuple(self.head)))
            f.write("\t".join(head[:3]) + "\n")
            f.write("\t".join(head[3:8]) + "\n")

            data = self.to_numpy()
            np.savetxt(f, data, fmt=fmt)

    @property
    def dtype(self):
        """column data type"""
        dtypes = [('sn', 'i4'), ('datetime', 'U21'), ('unit_code', 'i4'), ('category', 'i1'),
                  ('lon', 'f8'), ('lat', 'f8'), ('height', 'f4'),
                  ('return_stroke', 'i4'), ('rise_time', 'f4'), ('decay_time', 'f4'),
                  ('intensity', 'f4'), ('energy', 'f4'), ('error', 'f4'),
                  ('algorithm', 'i4'), ('steepness', 'f4')]
        return np.dtype(dtypes)

    @property
    def names(self):
        """column-name dictionary"""
        names = [('sn', '闪电个数的序号'), ('datetime', '日期时间'), ('unit_code', '单位代码'),
                 ('category', '闪电的种类'), ('lon', '闪电位置的经度'), ('lat', '闪电位置的纬度'),
                 ('height', '闪电位置的高度'),
                 ('return_stroke', '闪电回击数'), ('rise_time', '上升时间'),
                 ('decay_time', '衰减时间'),
                 ('intensity', '闪电归一化电流强度值'), ('energy', '闪电能量'), ('error', '误差'),
                 ('algorithm', '算法代码'), ('steepness', '陡度')]
        return dict(names)

    @property
    def index_cols(self):
        """base index columns, keep in frame when select data"""
        return ['sn', 'datetime', 'unit_code', 'category', 'lon', 'lat', 'height']

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

    def __getitem__(self, sn):
        """get data of station"""
        if isinstance(sn, str):
            return self.data[sn]
        elif isinstance(sn, (tuple, list, np.ndarray)):
            return self.data[np.isin(self.data['sn'], sn)]
        else:
            TypeError("stid must be type of str, tuple or list")
