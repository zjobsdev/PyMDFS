# -*- coding: utf-8 -*-
# @Author: wqshen
# @Email: wqshen91@gmail.com
# @Date: 2019/1/16 12:06
# @Last Modified by: wqshen

import os.path
import numpy as np
import pandas as pd
from io import StringIO
from typing import Optional, Union, Any, cast
from dataclasses import dataclass, fields, astuple


@dataclass
class Diamond31Head:
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
class Diamond31:
    """Micaps Diamond 31 Data class

    From this class, one can read and write Micaps Diamond 31 file

    Read Diamond 31
    ---------------
    >>> m1 = Diamond31(pathfile='../../sample_data/diamond_31/18091608.000')
    >>> m1.read()
    >>> print(m1)
    >>> print(m1.data.shape)

    Write Diamond 31
    ----------------
    >>> m1 = Diamond31()
    >>> m1.head = Diamond31Head('diamond', 31, '18年09月16日08时地面填图', 2018, 9, 16, 8, 2)
    >>> m1.data = np.array([
    ...    [67297, 34.90, -19.79, 16, 32, 4, 60, 2, 142, 9999, 0, 0, 0, 32, 4, 300, 22.5, 20.0, 0, 24.2, 20, 10, 0, 0],
    ...    [96315, 114.93, 4.93, 29, 32, 7, 230, 1, 88, 9999, 2, 2, 0, 38, 1, 300, 24.8, 8.0, 5, 26.5, 24, 18]
    ...             ])
    >>> m1.write("18091608.000")
    """
    head: Optional[Diamond31Head] = None
    data: Optional[pd.DataFrame] = None
    pathfile: Optional[str] = None
    missing_value: Optional[float] = None
    _ds: Optional[pd.DataFrame] = None

    def __post_init__(self):
        if self.pathfile is not None:
            self.read(self.pathfile)

    def sel(self, element: Any = None, center: Any = None, stid: Any = None,
            lon: Any = None, lat: Any = None, height: Any = None, query: str = None):
        """interface to select data

        Parameters
        ----------
        element: Any
            select element in columns
        center: Any
            select data at given report center or center list (C_CCCC)
        stid: Any
            select data at given flight or flight list (C01006)
        lat: Any
            select data within given flight latitude range (V05001)
        lon: Any
            select data within given flight longitude range (V06001)
        height: Any
            select data within given flight height range (V07002)
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

        for slice_, ele_ in zip((center, stid, lat, lon, height),
                                ('C_CCCC', 'C01006', 'V05001', 'V06001', 'V07002')):
            if slice_ is not None:
                if isinstance(slice_, slice):
                    df = df[(df[ele_] >= slice_.start) & (df[ele_] <= slice_.stop)]
                elif isinstance(slice_, (list, tuple, np.ndarray, pd.Series)):
                    df = df[df[ele_].isin(slice_)]
                elif isinstance(slice_, (str, int, float)):
                    df = df[df[ele_] == stid]
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

        ncols = len(self.dtype)
        headsize = len(fields(Diamond31Head))
        self.head = Diamond31Head(*data[:headsize])
        records = [','.join(data[headsize + i * ncols:headsize + (i + 1) * ncols])
                   for i in range(self.head.nrec)]
        self._ds = self.data = pd.read_csv(StringIO('\n'.join(records)))

    def write(self, pathfile: str, encoding: str = 'GBK',
              fmt: str = '%4s%5i%3i%3i%3i%7s%5i%3i%3i%5s%9.4f%9.4f%4i%4i%9.2f%8.1f%8.1f%8.2f%8.1f%8.1f%5i%5i%5i%5i%5i%5i'):
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
            f.write("\t".join(head) + "\n")

            np.savetxt(f, self.data.values, fmt=fmt)

    @property
    def dtype(self):
        """column data type"""
        dtypes = [('C_CCCC', 'S4'), ('C_LY', 'i2'), ('C_LM', 'i2'), ('C_LD', 'i2'), ('C_LH', 'i2'),
                  ('C01006', 'S6'), ('V04001', 'i2'), ('V04002', 'i2'), ('V04003', 'i2'),
                  ('V_OHM', 'S4'), ('V05001', 'f4'), ('V06001', 'f4'), ('V08004', 'i2'),
                  ('V02061', 'i2'), ('V07002', 'f4'), ('V12001', 'f4'), ('V11001', 'f4'),
                  ('V11002', 'f4'), ('V11041', 'f4'), ('V11031', 'f4'), ('F07002', 'i2'),
                  ('F12001', 'i2'), ('F11001', 'i2'), ('F11002', 'i2'), ('F11041', 'i2'),
                  ('F11031', 'i2')]
        return np.dtype(dtypes)

    @property
    def names(self):
        """column-name dictionary"""
        names = [('C_CCCC', '发报中心'), ('C_LY', '年'), ('C_LM', '月'), ('C_LD', '日'),
                 ('C_LH', '时'),
                 ('C01006', '航班'), ('V04001', '观测时间年'), ('V04002', '月'),
                 ('V04003', '日'),
                 ('V_OHM', '时间'), ('V05001', '纬度'), ('V06001', '经度'),
                 ('V08004', '飞行类型'),
                 ('V02061', '导航状态'), ('V07002', '飞行高度'), ('V12001', '温度'),
                 ('V11001', '风向'),
                 ('V11001', '风速'),
                 ('V11041', '垂直速度'), ('V11031', '湍流度'),
                 ('F07002', '温度可信度'),
                 ('F12001', '风可信度'), ('F11001', '垂直速度可信度'),
                 ('F11002', '湍流度可信度'), ('F11041', '位置可信度'),
                 ('F11031', '高度可信度')]
        return dict(names)

    @property
    def index_cols(self):
        """base index columns, keep in frame when select data"""
        return ['C_CCCC', 'C_LY', 'C_LM', 'C_LD', 'C_LH', 'C01006', 'V04001', 'V04002', 'V04003',
                'V_OHM', 'V05001', 'V06001', 'V08004', 'V02061', 'V07002']

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
