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
class Diamond3Head:
    diamond: str
    dtype: int
    description: str
    year: int
    month: int
    day: int
    hour: int
    level: int

    # if nlines is 0, you should set lines=[]
    nlines: int
    lines: list
    smooth: float
    boldvalue: float

    # if nedgepoints is 0, you should set edgepoints=[]
    nedgepoints: int
    edgepoints: list

    nele: int
    nrec: int

    def __post_init__(self):
        self._cast_fields_types()

    def _cast_fields_types(self):
        for field in fields(cast(dataclass, self)):
            field_value = getattr(self, field.name)
            setattr(self, field.name, field.type(field_value))


@dataclass
class Diamond3:
    """Micaps Diamond 3 (通用填图和离散点等值线) Data class

    From this class, one can read and write Micaps Diamond 3 file

    Read Diamond 3
    --------------
    >>> m3 = Diamond3(pathfile='../../sample_data/diamond_3/18091608.000')
    >>> m3.read()
    >>> print(m3)
    >>> print(m3.data.shape)
    >>> print(m3.to_frame())

    Write Diamond 3
    ---------------
    >>> m3 = Diamond3()
    >>> m3.head = Diamond3Head('diamond', 3, '高空700hPa变温_20210725200000',
    ...                         2021, 7, 25, 20, 700, 0, [], 0, 0, 0, [], 1, 487)
    >>> m3.data = np.array([
    ...    [47104, 128.85, 37.8, 80, 0.6],
    ...    [83971, -51.18, -30, 3, 4.6],
    ...    [29698, 99.03, 54.88, 411, 0.0],
    ...    [76805, -99.75, 16.75, 3, 0.2],
    ...    [36872, 77, 43.35, 662, 0.4],
    ...             ])
    >>> m3.write("2021072520.000")
    """
    head: Optional[Diamond3Head] = None
    data: Optional[np.ndarray] = None
    pathfile: Optional[str] = None
    missing_value: Optional[float] = None
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
        """Read micaps diamond 3 file

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
        headsize = len(fields(Diamond3Head))
        head = data[:9]
        nlines = int(head[-1])
        head += [data[9:9 + nlines]]
        head += data[9 + nlines:12 + nlines]
        nedgepoints = int(head[-1])
        head += [data[12 + nlines:12 + nlines + nedgepoints]]
        head += data[12 + nlines + nedgepoints:14 + nlines + nedgepoints]
        self.head = Diamond3Head(*head)

        dynamic_headsize = headsize + (nlines - 1) + (nedgepoints - 1)
        ncols = len(data[dynamic_headsize:]) / self.head.nrec
        if not ncols.is_integer():
            raise Exception("Data can't be splitted into integer columns.")
        self.data = np.asfarray(data[dynamic_headsize:]).reshape((self.head.nrec, int(ncols)))
        self._ds = self.to_frame()

    def write(self, pathfile: str, encoding: str = 'GBK', fmt: str = '%5i%8.2f%8.2f%5i%8.1f'):
        """Write data into micaps diamond4 text file

        Parameters
        ----------
        pathfile : str
            path to file name
        encoding : str, Optional, default: 'GBK'
            encoding of output file
        fmt : Optional[str], default: '%5i%8.2f%8.2f%5i%8.1f'
            format to save numpy data
        """
        with open(pathfile, 'w', encoding=encoding) as f:
            lines, edgepoints = map(str, self.head.lines), map(str, self.head.edgepoints)
            head = tuple(map(str, astuple(self.head)))
            f.write("\t".join(head[:8]) + "\n")
            f.write(head[8] + "\t" + "\t".join(lines) + "\t" + "\t".join(head[10:12]) + "\n")
            f.write(head[12] + "\t" + "\t".join(edgepoints) + "\n")
            f.write("\t".join(head[-2:]) + "\n")

            data = self.to_numpy()
            ffields = fmt.split('%')
            if self.data.shape[-1] > len(ffields):
                fmt += ffields[-1] * (self.data.shape[-1] - len(ffields))
            np.savetxt(f, data, fmt=fmt)

    @property
    def dtype(self):
        """column data type"""
        ncols = self.data.shape[-1]
        dtypes = [('stid', 'i4'), ('lon', 'f4'), ('lat', 'f4'), ('height', 'f4'), ('ele1', 'f4')]
        if ncols > len(dtypes):
            dtypes += [(f'ele{i + 2}', str) for i in range(ncols - len(dtypes))]
        return np.dtype(dtypes)

    @property
    def names(self):
        """column-name dictionary"""
        names = [('stid', '区站号'), ('lon', '经度'), ('lat', '纬度'), ('height', '拔海高度'),
                 ('ele1', '要素值1')]
        ncols = self.data.shape[-1]
        if ncols > len(names):
            names += [(f'ele{i + 2}', f'要素值{i + 2}') for i in range(ncols - len(names))]
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
        data: pd.DataFrame
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
                if k in ('stid',):
                    conditions = conditions & (self._ds[k] == v)
                elif k in ('lat', 'lon', 'height'):
                    conditions = conditions & (self._ds[k].between(v.start, v.stop))
            return self._ds.loc[conditions]
        else:
            return self._ds
