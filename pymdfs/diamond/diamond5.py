# -*- coding: utf-8 -*-
# @Author: wqshen
# @Email: wqshen91@gmail.com
# @Date: 2021/12/17 10:25
# @Last Modified by: wqshen


import os
import numpy as np
import pandas as pd
from typing import Optional, Union, Any, cast
from dataclasses import dataclass, fields, astuple
from numpy.lib.recfunctions import unstructured_to_structured


@dataclass
class Diamond5Head:
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
class Diamond5Index:
    stid: str
    lon: float
    lat: float
    height: float
    nrec: int

    def __post_init__(self):
        self._cast_fields_types()

    def _cast_fields_types(self):
        for field in fields(cast(dataclass, self)):
            field_value = getattr(self, field.name)
            setattr(self, field.name, field.type(field_value))


@dataclass
class Diamond5:
    """Micaps Diamond 5 Data class

    From this class, one can read and write Micaps Diamond 5 file

    Read Diamond5
    --------------
    >>> m5 = Diamond5(pathfile='../../sample_data/diamond_5/18091608.000')
    >>> m5.read()
    >>> print(m5)
    >>> print(m5.data)
    >>> print(m5.to_frame())

    Write Diamond5
    --------------
    >>> m5 = Diamond5()
    >>> m5.head = Diamond5Head('diamond', 5, '18年09月16日08时温度对数压力图', 18, 09, 16, 8, 519)
    >>> m5.data = np.array([
    ...    [67297, 34.90, -19.79, 16, 32, 4, 60, 2, 142, 9999, 0, 0, 0, 32, 4, 300, 22.5, 20.0, 0, 24.2, 20, 10, 0, 0],
    ...    [96315, 114.93, 4.93, 29, 32, 7, 230, 1, 88, 9999, 2, 2, 0, 38, 1, 300, 24.8, 8.0, 5, 26.5, 24, 18]
    ...             ])
    >>> m5.write("18091608.000")
    """
    head: Optional[Diamond5Head] = None
    index: Optional[pd.DataFrame] = None
    data: Optional[list] = None
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
                idx = df.index.get_level_values(ele_)
                if isinstance(slice_, slice):
                    df = df[(idx >= slice_.start) & (idx <= slice_.stop)]
                elif isinstance(slice_, (list, tuple, np.ndarray, pd.Series)):
                    df = df[idx.isin(slice_)]
                elif isinstance(slice_, (str, int, float)):
                    df = df[idx == slice_]
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

        headsize = len(fields(Diamond5Head))
        self.head = Diamond5Head(*data[:headsize])

        irec, p, indices, records = 0, headsize, [], []
        while irec < self.head.nrec:
            rec_head = data[p:p + 5]
            nrec_irec = int(rec_head[-1])
            p += 5
            record = data[p:p + nrec_irec]
            p += nrec_irec
            irec += 1
            indices.append(rec_head)
            record = np.array(record).reshape(-1, len(self.dtype))
            record = unstructured_to_structured(record, dtype=self.dtype)
            records.append(record)
        index = unstructured_to_structured(np.array(indices), dtype=self.dtype_index)
        self.index = pd.DataFrame.from_records(index)
        self.data = records
        self._ds = self.to_frame()

    def write(self, pathfile: str, encoding: str = 'GBK',
              fmt='%6d %6d %4d %4d %4d %4d'):
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
            f.write("\t".join(head[:3]) + "\n" + "\t".join(head[3:]) + '\n')
            for i, (head, track) in enumerate(zip(self.index.values, self.data)):
                f.write("{:6s}\t{:.2f}\t{:.2f}\t{:4.0f}\t{:4d}".format(*head) + "\n")
                np.savetxt(f, track, fmt=fmt)

    @property
    def dtype(self):
        dtypes = [('pressure', 'f4'), ('height', 'f4'), ('temperature', 'f4'),
                  ('dewpoint', 'f4'), ('windr', 'f4'), ('winds', 'f4')]
        return np.dtype(dtypes)

    @property
    def dtype_index(self):
        dtypes = [('stid', 'U6'), ('lon', 'f4'), ('lat', 'f4'), ('height', 'f4'), ('nrec', 'i4')]
        return np.dtype(dtypes)

    @property
    def index_cols(self):
        """base index columns, keep in frame when select data"""
        return ['stid', 'lon', 'lat', 'height']

    def to_numpy(self):
        """Convert instance into numpy array with dtypes

        Returns
        -------
        data: list
            converted data
        """
        return [unstructured_to_structured(np.asarray(d).reshape(-1, len(self.dtype)),
                                           dtype=self.dtype)
                for d in self.data]

    def to_frame(self):
        """Convert instance into pandas.DataFrame

        Returns
        -------
        data: xr.DataFrame
            converted data
        """
        nrecs = self.index.nrec // len(self.dtype)
        multi_index = self.index.loc[self.index.index.repeat(nrecs)]
        multi_index = pd.MultiIndex.from_frame(multi_index[self.index_cols])
        data = pd.DataFrame.from_records(np.hstack(self.data), index=multi_index)
        return data

    def __repr__(self):
        """print"""
        return self.head.__repr__()

    def __getitem__(self, stid: str):
        """get data of station"""
        if isinstance(stid, str):
            return self._ds[self._ds.stid == stid]
        elif isinstance(stid, (tuple, list, np.ndarray)):
            return self._ds[self._ds.stid.isin(stid)]
        else:
            TypeError("stid must be type of str")
