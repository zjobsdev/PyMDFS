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
class Diamond1Head:
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
class Diamond1:
    """Micaps Diamond 1 (地面全要素填图数据) Data class

    From this class, one can read and write Micaps Diamond 1 file

    Read Diamond1
    --------------
    >>> m1 = Diamond1(pathfile='../../sample_data/diamond_1/18091608.000')
    >>> m1.read()
    >>> print(m1)
    >>> print(m1.data.shape)
    >>> print(m1.to_frame())

    Write Diamond1
    --------------
    >>> m1 = Diamond1()
    >>> m1.head = Diamond1Head('diamond', 1, '18年09月16日08时地面填图', 2018, 9, 16, 8, 2)
    >>> m1.data = np.array([
    ...    [67297, 34.90, -19.79, 16, 32, 4, 60, 2, 142, 9999,
    ...     0, 0, 0, 32, 4, 300, 22.5, 20.0, 0, 24.2, 20, 10, 0, 0],
    ...    [96315, 114.93, 4.93, 29, 32, 7, 230, 1, 88, 9999,
    ...     2, 2, 0, 38, 1, 300, 24.8, 8.0, 5, 26.5, 24, 18]
    ...             ])
    >>> m1.write("18091608.000")
    """
    head: Optional[Diamond1Head] = None
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
        """Read micaps diamond 1 file

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
        headsize = len(fields(Diamond1Head))
        self.head = Diamond1Head(*data[:headsize])
        ncols = len(data[headsize:]) / self.head.nrec
        if not ncols.is_integer():
            raise Exception("Data can't be splitted into integer columns.")
        self.data = np.asarray(data[headsize:]).reshape((self.head.nrec, int(ncols)))
        self._ds = self.to_frame()

    def write(self, pathfile: str, encoding: str = 'GBK',
              fmt: str = '%5i%8.2f%8.2f%5i%5i%5i%5i%5i%5i%5i%5i%5i%5i'
                         '%5i%5i%5i%7.1f%7.1f%5i%7.1f%5i%5i%5i%5i%5i%5i'):
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

            data = self.to_numpy()
            if len(fmt.split('%')) == data.shape[-1] - 2:
                fmt = '%'.join(fmt.split('%')[:-2])
            np.savetxt(f, data, fmt=fmt)

    @property
    def index_cols(self):
        """base index columns, keep in frame when select data"""
        return ['stid', 'lon', 'lat', 'height', 'level']

    @property
    def dtype(self):
        """column data type"""
        dtypes = [('stid', 'i4'), ('lon', 'f4'), ('lat', 'f4'), ('height', 'f4'), ('level', 'i1'),
                  ('tcc', 'i2'), ('windr', 'i2'), ('winds', 'i2'), ('slp', 'i2'), ('dp3', 'i2'),
                  ('ww1', 'i2'), ('ww2', 'i2'), ('pr6', 'f4'), ('lcs', 'i2'), ('lcc', 'i2'),
                  ('lch', 'i2'), ('dew', 'f4'), ('vis', 'f4'), ('ww', 'i2'), ('t', 'f4'),
                  ('mcs', 'i2'), ('hcs', 'i2'), ('identifier1', 'i2'), ('identifier2', 'i2'),
                  ('dt24', 'f4'), ('dp24', 'f4')]
        if self.data.shape[-1] == len(dtypes) - 2:
            dtypes = dtypes[:-2]
        return np.dtype(dtypes)

    @property
    def names(self):
        """column-name dictionary"""
        names = [('stid', '区站号'), ('lon', '经度'), ('lat', '纬度'), ('height', '拔海高度'),
                 ('level', '站点级别'), ('tcc', '总云量'), ('windr', '风向'), ('winds', '风速'),
                 ('slp', '气压'), ('dp3', '3小时变压'), ('ww1', '过去天气1'), ('ww2', '过去天气2'),
                 ('pr6', '6小时降水'), ('lcs', '低云状'), ('lcc', '低云量'), ('lch', '低云高'),
                 ('dew', '露点'), ('vis', '能见度'), ('ww', '现在天气'), ('t', '温度'),
                 ('mcs', '中云状'), ('hcs', '高云状'),
                 ('identifier1', '标志1'), ('identifier2', '标志2'),
                 ('dt24', '24小时变温'), ('dp24', '24小时变压')]
        return dict(names)

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
