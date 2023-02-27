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
class Diamond42Head:
    diamond: str
    dtype: int
    description: str
    year: int
    month: int
    day: int
    hour: int
    minute: int
    nele: int
    nrec: int

    def __post_init__(self):
        self._cast_fields_types()

    def _cast_fields_types(self):
        for field in fields(cast(dataclass, self)):
            field_value = getattr(self, field.name)
            setattr(self, field.name, field.type(field_value))


@dataclass
class Diamond42:
    """Micaps Diamond 1 Data class

    From this class, one can read and write Micaps Diamond4 file

    Read Diamond42
    --------------
    >>> m42 = Diamond42(pathfile='../../sample_data/diamond_42/GPSG_200809162300.000')
    >>> m42.read()
    >>> print(m42)
    >>> print(m42.data.shape)
    >>> print(m42.to_frame())

    Write Diamond42
    --------------
    >>> m42 = Diamond42()
    >>> m42.head = Diamond42Head('diamond', 42, '08年07月29日20时GPS/MET数据', 2008, 7, 29, 20, 0, 8, 3)
    >>> m42.data = np.array([['ahhs', 58531, 29.714, 118.284, 142.3, 2.6846, 978.8, 23.6, 97.0,   72.7, 999999],
    ...                      ['ahma', 58336, 31.701, 118.516,  22.8, 2.6943, 993.6, 26.3, 92.0,   69.4, 999999],
    ...                      ['bais', 59211, 23.903, 106.606, 159.3, 999999, 980.6, 25.1, 84.0, 999999, 999999]
    ...             ])
    >>> m42.write("GPSG_200809162300.000")
    """
    head: Optional[Diamond42Head] = None
    data: Optional[np.ndarray] = None
    pathfile: Optional[str] = None
    missing_value: Optional[float] = None

    # _ds: Optional[pd.DataFrame] = None

    def __post_init__(self):
        if self.pathfile is not None:
            self.read(self.pathfile)

    def sel(self, element: Any = None, code: Any = None, stid: Any = None, lon: Any = None,
            lat: Any = None, height: Any = None, query: str = None):
        """interface to select data

        Parameters
        ----------
        element: Any
            select element in columns
        code: Any
            select data within given code range
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

        for slice_, ele_ in zip((code, stid, lon, lat, height), self.index_cols):
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

        headsize = len(fields(Diamond42Head))
        self.head = Diamond42Head(*data[:headsize])
        ncols = len(data[headsize:]) / self.head.nrec
        if not ncols.is_integer():
            raise Exception("Data can't be splitted into integer columns.")
        self.data = np.asarray(data[headsize:]).reshape((self.head.nrec, int(ncols)))
        self._ds = self.to_frame()
    #
    def write(self, pathfile: str, encoding: str = 'GBK',
              fmt='%4s %5s %7.3f %7.3f %7.1f %6.4f %6.1f %6.1f %6.1f %6.1f %6.1f'):
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
            np.savetxt(f, data, fmt=fmt)

    @property
    def dtype(self):
        dtypes = [('code', 'U4'), ('stid', 'U5'), ('lon', 'f4'), ('lat', 'f4'), ('height', 'f4'),
                  ('ztd', 'f4'), ('p', 'f4'), ('t', 'f4'), ('rh', 'f4'), ('pwv', 'f4'),
                  ('elec', 'f4')]
        return np.dtype(dtypes)

    @property
    def index_cols(self):
        """base index columns, keep in frame when select data"""
        return ['code', 'stid', 'lon', 'lat', 'height']

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
        import pandas as pd

        data = pd.DataFrame.from_records(self.to_numpy())
        return data

    def __repr__(self):
        """print"""
        return self.head.__repr__()


if __name__ == "__main__":
    m42 = Diamond42()
    m42.head = Diamond42Head('diamond', 42, '08年07月29日20时GPS/MET数据', 2008, 7, 29, 20, 0, 8, 3)
    m42.data = np.array(
        [['ahhs', 58531, 29.714, 118.284, 142.3, 2.6846, 978.8, 23.6, 97.0, 72.7, 999999],
         ['ahma', 58336, 31.701, 118.516, 22.8, 2.6943, 993.6, 26.3, 92.0, 69.4, 999999],
         ['bais', 59211, 23.903, 106.606, 159.3, 999999, 980.6, 25.1, 84.0, 999999, 999999]
         ])
    m42.write("GPSG_200809162300.000")
