# -*- coding: utf-8 -*-
# @Author: wqshen
# @Email: wqshen91@gmail.com
# @Date: 2023/2/9 22:07
# @Last Modified by: wqshen


import os
import yaml
import itertools
import numpy as np
import pandas as pd
from typing import Optional, Union, Any, cast
from struct import unpack, calcsize
from collections import OrderedDict
from dataclasses import dataclass, fields


@dataclass
class MdfsStationHead:
    discriminator: str
    dtype: int
    description: str
    level: float
    levelDescription: str
    year: int
    month: int
    day: int
    hour: int
    minute: int
    second: int
    timezone: int
    Extent: str

    def __post_init__(self):
        self._cast_fields_types()

    def _cast_fields_types(self):
        for field in fields(cast(dataclass, self)):
            field_value = getattr(self, field.name)
            if isinstance(field_value, bytes):
                field_value = field_value.decode('GBK').rstrip('\x00')
            setattr(self, field.name, field.type(field_value))


@dataclass
class MdfsStationData(object):
    head: Optional[MdfsStationHead] = None
    stationNum: Optional[int] = None
    elementNum: Optional[int] = None
    elementIdTypeMap: Optional[OrderedDict] = None
    data: Optional[dict] = None
    pathfile: Optional[str] = None
    missing_value: Optional[float] = None
    _ds: Optional[pd.DataFrame] = None

    def __post_init__(self):
        if self.pathfile is not None:
            self.read(self.pathfile)
            self._ds = self.to_dataframe()

    def sel(self, element: Any = None, query: str = None, **kwargs):
        """interface to select data

        Parameters
        ----------
        element: Any
            select element in columns
        kwargs: dict
            select data key within given level range
        query: str
            pandas.DataFrame.query string to filter data

        Returns
        -------
        df (pd.DataFrame): selected data
        """
        df = self._ds
        if element is not None:
            if isinstance(element, str):
                df = df[self.translate(element)]
            elif isinstance(element, (list, tuple, np.ndarray, pd.Series)):
                element = [self.translate(ele) for ele in element]
                df = df[[*element]]

        for ele_, slice_ in kwargs.items():
            ele_ = self.translate(ele_)
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

    def translate(self, ele):
        """translate element name into column name"""
        key_map = {'stid': '站号', 'lon': '经度', 'lat': '纬度', 'height': '测站高度',
                   'level': '测站级别（short）'}
        return key_map.get(ele, ele)

    def read(self, path_or_bytes: Union[str, bytes]):
        if isinstance(path_or_bytes, str):
            with open(path_or_bytes, 'rb') as f:
                bytes_array = f.read()
        else:
            bytes_array = path_or_bytes

        head_data = unpack('=4sh100sf50siiiiiii100s', bytes_array[:288])
        self.head = MdfsStationHead(*head_data)

        stationNum, elementNum = unpack("=ih", bytes_array[288:288 + 6])
        elementIdTypeMap = unpack("={}h".format(elementNum * 2),
                                  bytes_array[294:294 + 4 * elementNum])
        elementIdTypeMap = OrderedDict(
            itertools.zip_longest(*[iter(elementIdTypeMap)] * 2, fillvalue="")
        )
        self.stationNum = stationNum
        self.elementNum = elementNum
        self.elementIdTypeMap = elementIdTypeMap

        p = 294 + 4 * elementNum
        data = {}
        _id_dict = self._load_id_description()
        for i in range(stationNum):
            data_head = list(unpack("=iffh", bytes_array[p:p + 14]))
            p += 14

            element = {"站号": data_head[0], "经度": data_head[1], "纬度": data_head[2]}
            for j in range(data_head[-1]):
                element_id = unpack("=h", bytes_array[p:p + 2])[0]
                p += 2
                element_type = self._element_type_dict[elementIdTypeMap[element_id]]
                elemet_value = \
                    unpack("=" + element_type, bytes_array[p:p + calcsize("=" + element_type)])[0]
                element[_id_dict.get(element_id, element_id)] = elemet_value
                p += calcsize("=" + element_type)
                data[data_head[0]] = element
        self.data = data
        self._ds = self.to_dataframe()

    @property
    def _element_type_dict(self) -> dict:
        return {1: 'b', 2: 'h', 3: 'i', 4: 'l', 5: 'f', 6: 'd', 7: 's'}

    def _load_id_description(self, pathfile: str = '../config/ElementNameDict.yml'):
        if not os.path.isfile(pathfile):
            path = os.path.dirname(os.path.realpath(__file__))
            pathfile = os.path.join(path, pathfile)
            if not os.path.isfile(pathfile):
                raise FileNotFoundError(f"configuration file {pathfile} not found")

        with open(pathfile, 'r', encoding='utf8') as f:
            return yaml.load(f, Loader=yaml.SafeLoader)

    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame.from_dict(self.data, orient='index')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close open dataset"""
        del self.pathfile, self.data, self.head, self._ds

    def __repr__(self):
        """print"""
        return self.head.__repr__()
