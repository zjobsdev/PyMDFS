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
class Diamond16Head:
    diamond: str
    dtype: int
    description: str
    nrec: int

    def __post_init__(self):
        self._cast_fields_types()

    def _cast_fields_types(self):
        for field in fields(cast(dataclass, self)):
            field_value = getattr(self, field.name)
            setattr(self, field.name, field.type(field_value))


@dataclass
class Diamond16:
    """Micaps Diamond 16 Data class

    From this class, one can read and write Micaps Diamond 16 file

    Read Diamond 16
    ---------------
    >>> m1 = Diamond16(pathfile='../../sample_data/diamond_16/18091608.000')
    >>> m1.read()
    >>> print(m1)
    >>> print(m1.data.shape)
    >>> print(m1.to_frame())

    Write Diamond 16
    ----------------
    >>> m1 = Diamond16()
    >>> m1.head = Diamond16Head('diamond', 16, '18年09月16日08时地面填图', 2018, 9, 16, 8, 6)
    >>> m1.data = np.array([
    ...    [50136 5328 12222 0],
    ...    [50246 5219 12443 0],
    ...    [50353 5143 12639 0],
    ...    [50434 5029 12141 0],
    ...    [50442 5024 12407 0],
    ...    [50468 5015 12727 0],
    ...             ])
    >>> m1.write("18091608.000")
    """
    head: Optional[Diamond16Head] = None
    data: Optional[np.ndarray] = None
    pathfile: Optional[str] = None
    missing_value: Optional[float] = None
    _ds: Optional[pd.DataFrame] = None

    def __post_init__(self):
        if self.pathfile is not None:
            self.read(self.pathfile)

    def sel(self, stid: Any = None, lon: Any = None, lat: Any = None,
            level: Any = None, query: str = None):
        """interface to select data

        Parameters
        ----------
        stid: Any
            select data at given station or station list
        lon: Any
            select data within given longitude range
        lat: Any
            select data within given latitude range
        level: Any
            select data within given height range
        query: str
            pandas.DataFrame.query string to filter data

        Returns
        -------
        df (pd.DataFrame): selected data
        """
        df = self._ds
        for slice_, ele_ in zip((stid, lon, lat, level), self.index_cols):
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

        headsize = len(fields(Diamond16Head))
        self.head = Diamond16Head(*data[:headsize])
        ncols = len(data[headsize:]) / self.head.nrec
        if not ncols.is_integer():
            raise Exception("Data can't be splitted into integer columns.")
        self.data = np.asfarray(data[headsize:]).reshape((self.head.nrec, int(ncols)))
        self._ds = self.to_frame()

    def write(self, pathfile: str, encoding: str = 'GBK', fmt: str = '%5i%8i%8i%4i'):
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
            f.write("\t".join(head) + "\n")

            data = self.to_numpy()
            np.savetxt(f, data, fmt=fmt)

    @property
    def dtype(self):
        """column data type"""
        dtypes = [('stid', 'i4'), ('lon', 'f4'), ('lat', 'f4'), ('level', 'i1')]
        return np.dtype(dtypes)

    @property
    def names(self):
        """column-name dictionary"""
        names = [('stid', '区站号'), ('lon', '经度'), ('lat', '纬度'), ('level', '站点级别')]
        return dict(names)

    @property
    def index_cols(self):
        """base index columns, keep in frame when select data"""
        return ['stid', 'lon', 'lat', 'level']

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
