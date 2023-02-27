# -*- coding: utf-8 -*-
# @Author: wqshen
# @Email: wqshen91@gmail.com
# @Date: 2021/8/4 20:20
# @Last Modified by: wqshen

import os
import numpy as np
import pandas as pd
from typing import Optional, Union, Any, cast
from dataclasses import dataclass, fields, astuple
from numpy.lib.recfunctions import unstructured_to_structured


@dataclass
class Diamond7Head:
    diamond: str
    dtype: int
    description: str

    def __post_init__(self):
        self._cast_fields_types()

    def _cast_fields_types(self):
        for field in fields(cast(dataclass, self)):
            field_value = getattr(self, field.name)
            setattr(self, field.name, field.type(field_value))


@dataclass
class Diamond7Index:
    name: str
    sn: str
    center: str
    nrec: int

    def __post_init__(self):
        self._cast_fields_types()

    def _cast_fields_types(self):
        for field in fields(cast(dataclass, self)):
            field_value = getattr(self, field.name)
            setattr(self, field.name, field.type(field_value))


@dataclass
class Diamond7:
    """Micaps Diamond 7 Data class

    From this class, one can read and write Micaps Diamond7 file

    Read Diamond5
    --------------
    >>> m7 = Diamond7(pathfile="./micaps/typhoon/ECEP/ECEP14W0408.dat")
    >>> m7.read()
    >>> print(m7)
    >>> print(m7.to_frame())

    Write Diamond5
    --------------
    >>> m5 = Diamond7()
    >>> m5.head = Diamond7Head('diamond', 7, '9714号台风路径(主观预报)', 'NAMELESS', '0615', '28', 3)
    >>> m5.data = np.array([
    ...    [2006, 09, 24, 08, 00, 111.3, 15.9, 995, 18, 100.0, 0.0, 292.5, 15.0],
    ...    [2006, 09, 24, 08, 24, 107.4, 17.3, 985, 23, 0.0, 0.0, 0.0, 0.0]
    ...             ])
    >>> m5.write("18091608.000")
    """
    head: Optional[Diamond7Head] = None
    index: Optional[pd.DataFrame] = None
    data: Optional[list] = None
    pathfile: Optional[str] = None
    missing_value: Optional[float] = 9999
    _ds: Optional[pd.DataFrame] = None

    def __post_init__(self):
        if self.pathfile is not None:
            self.read(self.pathfile)

    def sel(self, element: Any = None, name: Any = None, sn: Any = None, center: Any = None,
            query: str = None):
        """interface to select data

        Parameters
        ----------
        element: Any
            select element in columns
        name: Any
            select data at given station or station list
        sn: Any
            select data within given longitude range
        center: Any
            select data within given latitude range
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

        for slice_, ele_ in zip((name, sn, center), self.index_cols):
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
        """Read micaps diamond 7 file

        path_or_string : Union[str, bytes]
            file content str, bytes or path to file name
        encoding : str, Optional, default: 'GBK'
            encoding of output file

        Notes
        -----
        Before call this method, ensure you have set the `pathfile`
        """
        if isinstance(path_or_string, bytes):
            data = path_or_string.decode(encoding).split('\n')
        elif os.path.isfile(path_or_string):
            with open(path_or_string, 'r', encoding=encoding) as f:
                data = f.readlines()
        else:
            data = path_or_string.split('\n')

        indices, tracks, track = [], [], []
        headsize = len(fields(Diamond7Head))
        head = data[0].split()
        if len(head) != headsize:
            raise ValueError("Field in first line has inconsistent size to HEAD.")
        else:
            self.head = Diamond7Head(*head)

        for row in data[1:]:
            row_data = row.split()
            if row_data == ['0']:
                tracks.append(track)
                track = []
            else:
                track.append(tuple(row_data))
        del track
        if len(tracks) == 0:
            print("Warning: Failed to find any track in file.")
            return
        else:
            index = unstructured_to_structured(np.array([track[0] for track in tracks]),
                                               dtype=self.dtype_index)
            self.index = pd.DataFrame.from_records(index)
            self.data = [np.asarray(track[1:], dtype=self.dtype) for track in tracks]
        self._ds = self.to_frame()

    def write(self, pathfile: str, encoding: str = 'GBK',
              fmt='%4i %02i %02i %02i %03i %6.1f %5.1f %4.0f %3.0f %5.0f %5.0f %5.0f %5.0f'):
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
            for i, (head, track) in enumerate(zip(self.index.values, self.data)):
                if i > 0:
                    f.write('0\n')
                f.write("\t".join(map(str, head)) + "\n")
                np.savetxt(f, track, fmt=fmt)

    @property
    def dtype(self):
        return np.dtype({'names': ['year', 'month', 'day', 'hour', 'fh',
                                   'lon', 'lat', 'wind', 'pres',
                                   'r7', 'r10', 'dir', 'spd'],
                         'formats': ['i2', 'i1', 'i1', 'i1', 'i2', 'f4',
                                     'f4', 'f4', 'f4', 'f4', 'f4', 'f4', 'f4']})

    @property
    def dtype_index(self):
        return np.dtype({'names': ['name', 'sn', 'center', 'nrec'],
                         'formats': ['U50', 'U4', 'U4', 'i1']})

    @property
    def index_cols(self):
        """base index columns, keep in frame when select data"""
        return ['name', 'sn', 'center']

    def to_frame(self):
        """Convert instance into pandas.DataFrame

        Returns
        -------
        data: xr.DataArray
            converted data
        """
        nrecs = self.index.nrec
        multi_index = self.index.loc[self.index.index.repeat(nrecs)]
        multi_index = pd.MultiIndex.from_frame(multi_index[self.index_cols])
        data = pd.DataFrame.from_records(np.hstack(self.data), index=multi_index)
        return data

    def __repr__(self):
        """print"""
        return self.head.__repr__()

    def __getitem__(self, sn):
        """get data of station"""
        if isinstance(sn, str):
            return self._ds[self._ds.sn == sn]
        elif isinstance(sn, (tuple, list, np.ndarray)):
            return self._ds[self._ds.sn.isin(sn)]
        else:
            TypeError("stid must be type of str, tuple or list")
