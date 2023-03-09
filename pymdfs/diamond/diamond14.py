# -*- coding: utf-8 -*-
# @Author: wqshen
# @Email: wqshen91@gmail.com
# @Date: 2019/1/16 12:06
# @Last Modified by: wqshen


import re
import os
import numpy as np
from os import PathLike
from typing import Optional, Union, cast
from dataclasses import dataclass, fields


@dataclass
class Diamond14Head:
    diamond: str
    dtype: int
    description: str
    year: int
    month: int
    day: int
    hour: int
    fh: int

    def __post_init__(self):
        self._cast_fields_types()

    def _cast_fields_types(self):
        for field in fields(cast(dataclass, self)):
            field_value = getattr(self, field.name)
            setattr(self, field.name, field.type(field_value))


@dataclass
class Diamond14:
    """Micaps Diamond 1 Data class

    From this class, one can read and write Micaps Diamond4 file

    Read Diamond14
    --------------
    >>> m14 = Diamond14(pathfile='../../sample_data/diamond_14/18091608.000')
    >>> m14.read()
    >>> print(m14)
    """
    head: Optional[Diamond14Head] = None
    data: Optional[dict] = None
    pathfile: Optional[str] = None
    encoding: Optional[str] = 'GBK'
    missing_value: Optional[float] = None

    def __post_init__(self):
        if self.pathfile is not None:
            self.read(self.pathfile)

    def sel(self, element: str = None):
        """interface to select data

        Parameters
        ----------
        element: str
            select artist in data

        Returns
        -------
        data (dict): selected artist data
        """
        if element is not None and isinstance(element, str):
            return self.data[element]
        else:
            raise NotImplementedError("Only support select specific artist.")

    def read(self, path_or_string: Union[str, bytes]):
        """Read micaps diamond 14 file

        Parameters
        ----------
        path_or_string : Union[str, bytes]
            file content str, bytes or path to file name
        """
        head, sections = self.separate_artist(path_or_string)
        self.head = Diamond14Head(*head.split())
        self.data = {
            'LINES': self.read_lines(sections['LINES']),
            'LINES_SYMBOL': self.read_lines_symbol(sections['LINES_SYMBOL']),
            'SYMBOLS': self.read_symbols(sections['SYMBOLS']),
            'CLOSED_CONTOURS': self.read_closed_contours(sections['CLOSED_CONTOURS']),
            'STATION_SITUATION': self.read_station_situation(sections['STATION_SITUATION']),
            'WEATHER_REGION': self.read_weather_region(sections['WEATHER_REGION']),
            'FILLAREA': self.read_fillarea(sections['FILLAREA']),
            'NOTES_SYMBOL': self.read_notes_symbol(sections['NOTES_SYMBOL']),
            'WithProp_LINESYMBOLS:': self.read_wthprop_linesymbols(
                sections['WithProp_LINESYMBOLS']),
        }

    def separate_artist(self, path_or_string: Union[str, bytes]):
        """Read micaps diamond 14 file

        Parameters
        ----------
        path_or_string : Union[str, bytes]
            file content str, bytes or path to file name
        """
        if isinstance(path_or_string, bytes):
            s = path_or_string.decode(self.encoding)
        elif os.path.isfile(path_or_string):
            with open(path_or_string, 'r', encoding=self.encoding) as f:
                s = f.read()
        else:
            s = path_or_string

        segments = re.split(
            r'(\nLINES:|\nLINES_SYMBOL:|\nSYMBOLS:|\nCLOSED_CONTOURS:|\nSTATION_SITUATION|'
            r'\nWEATHER_REGION:|\nFILLAREA:|\nNOTES_SYMBOL:|\nWithProp_LINESYMBOLS:)',
            s)
        head = segments[0]

        n = len(segments) - 1
        if n % 2 != 0:
            raise Exception("re.split failed to separate key and values")
        sections = {
            segments[i * 2 + 1].split(':')[0][1:]: segments[i * 2 + 1][1:] + segments[i * 2 + 2]
            for i in range(int(n / 2))
        }
        return head, sections

    def read_lines(self, content: str):
        data = content.split()
        nlines = int(data[1])
        if nlines > 0:
            # define data
            line_width = []
            line_xyz_num = []
            line_xyz = []
            line_label_num = []
            line_label = []
            line_label_xyz = []
            idx = 2
            for i in range(nlines):
                # line width
                width = float(data[idx])
                line_width.append(width)
                idx += 1

                # line xyz point number
                xyz_num = int(data[idx])
                line_xyz_num.append(xyz_num)
                idx += 1

                # line xyz
                xyz = np.array(data[idx:(idx + 3 * xyz_num)]).astype(float)
                xyz.shape = [xyz_num, 3]
                line_xyz.append(xyz)
                idx += xyz_num * 3

                # line label
                label = data[idx]
                line_label.append(label)
                idx += 1

                # line label number
                label_num = int(data[idx])
                line_label_num.append(label_num)
                idx += 1

                # label xyz
                if label_num > 0:
                    label_xyz = np.array(
                        data[idx:(idx + 3 * label_num)]).astype(float)
                    label_xyz.shape = [label_num, 3]
                    line_label_xyz.append(label_xyz)
                    idx += label_num * 3
                else:
                    line_label_xyz.append([])

            # construct line data type
            lines = {
                "line_width": line_width, "line_xyz_num": line_xyz_num,
                "line_xyz": line_xyz, "line_label_num": line_label_num,
                "line_label": line_label, "line_label_xyz": line_label_xyz}
        else:
            lines = {}
        return lines

    def read_lines_symbol(self, content):
        data = content.split()
        nlines = int(data[1])
        idx = 2

        # loop every line symbol
        if nlines > 0:
            # define data
            linesym_code = []
            linesym_width = []
            linesym_xyz_num = []
            linesym_xyz = []

            for i in range(nlines):
                # line symbol code
                code = int(data[idx])
                linesym_code.append(code)
                idx += 1

                # line width
                width = float(data[idx])
                linesym_width.append(width)
                idx += 1

                # line symbol xyz point number
                xyz_num = int(data[idx])
                linesym_xyz_num.append(xyz_num)
                idx += 1

                # line symbol xyz
                xyz = np.array(data[idx:(idx + 3 * xyz_num)]).astype(float)
                xyz.shape = [xyz_num, 3]
                linesym_xyz.append(xyz)
                idx += xyz_num * 3

                # line symbol label
                label = data[idx]
                idx += 1

                # line symbol label number
                label_num = int(data[idx])
                idx += label_num * 3 + 1

            lines_symbol = {"linesym_code": linesym_code,
                            "linesym_width": linesym_width,
                            "linesym_xyz_num": linesym_xyz_num,
                            "linesym_xyz": linesym_xyz}
        else:
            lines_symbol = {}
        return lines_symbol

    def read_symbols(self, content):
        data = content.split()
        nlines = int(data[1])
        idx = 2

        # loop every symbol
        if nlines > 0:
            # define data
            symbol_code = []
            symbol_xyz = []
            symbol_value = []

            for i in range(nlines):
                # symbol code
                code = int(data[idx])
                symbol_code.append(code)
                idx += 1

                # symbol xyz
                xyz = np.array(data[idx:(idx + 3)]).astype(float)
                symbol_xyz.append(xyz)
                idx += 3

                # symbol value
                value = data[idx]
                symbol_value.append(value)
                idx += 1

            symbols = {"symbol_code": symbol_code,
                       "symbol_xyz": symbol_xyz,
                       "symbol_value": symbol_value}
        else:
            symbols = {}
        return symbols

    def read_closed_contours(self, content):
        data = content.split()
        nlines = int(data[1])
        idx = 2
        # loop every closed contour
        if nlines > 0:
            # define data
            cn_width = []
            cn_xyz_num = []
            cn_xyz = []
            cn_label_num = []
            cn_label = []
            cn_label_xyz = []

            for i in range(nlines):
                # line width
                width = float(data[idx])
                cn_width.append(width)
                idx += 1

                # line xyz point number
                xyz_num = int(data[idx])
                cn_xyz_num.append(xyz_num)
                idx += 1

                # line xyz
                xyz = np.array(data[idx:(idx + 3 * xyz_num)]).astype(float)
                xyz.shape = [xyz_num, 3]
                cn_xyz.append(xyz)
                idx += 3 * xyz_num

                # line label
                label = data[idx]
                cn_label.append(label)
                idx += 1

                # line label number
                label_num = int(data[idx])
                cn_label_num.append(label_num)
                idx += 1

                # label xyz
                if label_num > 0:
                    label_xyz = np.array(
                        data[idx:(idx + 3 * label_num)]).astype(float)
                    label_xyz.shape = [3, label_num]
                    cn_label_xyz.append(label_xyz)
                    idx += label_num * 3
                else:
                    cn_label_xyz.append([])

            closed_contours = {
                "cn_width": cn_width, "cn_xyz_num": cn_xyz_num,
                "cn_xyz": cn_xyz, "cn_label": cn_label,
                "cn_label_num": cn_label_num, "cn_label_xyz": cn_label_xyz}
        else:
            closed_contours = {}
        return closed_contours

    def read_station_situation(self, content):
        data = content.split()
        if len(data) > 1:
            stations = np.array(data[1:])
            stations.shape = [len(stations) // 2, 2]
        else:
            stations = None
        return stations

    def read_weather_region(self, content):
        data = content.split()
        nlines = int(data[1])
        idx = 2

        # loop every region
        if nlines > 0:
            # define data
            weather_region_code = []
            weather_region_xyz_num = []
            weather_region_xyz = []

            for i in range(nlines):
                # region code
                code = int(data[idx])
                weather_region_code.append(code)
                idx += 1

                # region xyz point number
                xyz_num = int(data[idx])
                weather_region_xyz_num.append(xyz_num)
                idx += 1

                # region xyz point
                xyz = np.array(
                    data[idx:(idx + 3 * xyz_num)]).astype(float)
                xyz.shape = [xyz_num, 3]
                weather_region_xyz.append(xyz)
                idx += 3 * xyz_num

            weather_region = {
                "weather_region_code": weather_region_code,
                "weather_region_xyz_num": weather_region_xyz_num,
                "weather_region_xyz": weather_region_xyz}
        else:
            weather_region = {}
        return weather_region

    def read_fillarea(self, content):
        data = content.split()
        nlines = int(data[1])
        idx = 2

        # loop every fill area
        if nlines > 0:
            # define data
            fillarea_code = []
            fillarea_num = []
            fillarea_xyz = []
            fillarea_type = []
            fillarea_color = []
            fillarea_frontcolor = []
            fillarea_backcolor = []
            fillarea_gradient_angle = []
            fillarea_graphics_type = []
            fillarea_frame = []

            for i in range(nlines):
                # code
                code = int(data[idx])
                fillarea_code.append(code)
                idx += 1

                # xyz point number
                xyz_num = int(data[idx])
                fillarea_num.append(xyz_num)
                idx += 1

                # xyz point
                xyz = np.array(
                    data[idx:(idx + 3 * xyz_num)]).astype(float)
                xyz.shape = [xyz_num, 3]
                fillarea_xyz.append(xyz)
                idx += 3 * xyz_num

                # fill type
                ftype = int(data[idx])
                fillarea_type.append(ftype)
                idx += 1

                # line color
                color = np.array(data[idx:(idx + 4)]).astype(int)
                fillarea_color.append(color)
                idx += 4

                # front color
                front_color = np.array(data[idx:(idx + 4)]).astype(int)
                fillarea_frontcolor.append(front_color)
                idx += 4

                # background color
                back_color = np.array(data[idx:(idx + 4)]).astype(int)
                fillarea_backcolor.append(back_color)
                idx += 4

                # color gradient angle
                gradient_angle = float(data[idx])
                fillarea_gradient_angle.append(gradient_angle)
                idx += 1

                # graphics type
                graphics_type = int(data[idx])
                fillarea_graphics_type.append(graphics_type)
                idx += 1

                # draw frame or not
                frame = int(data[idx])
                fillarea_frame.append(frame)
                idx += 1

            fill_area = {
                "fillarea_code": fillarea_code, "fillarea_num": fillarea_num,
                "fillarea_xyz": fillarea_xyz, "fillarea_type": fillarea_type,
                "fillarea_color": fillarea_color,
                "fillarea_frontcolor": fillarea_frontcolor,
                "fillarea_backcolor": fillarea_backcolor,
                "fillarea_gradient_angle": fillarea_gradient_angle,
                "fillarea_graphics_type": fillarea_graphics_type,
                "fillarea_frame": fillarea_frame}
        else:
            fill_area = {}
        return fill_area

    def read_notes_symbol(self, content):
        data = content.split()
        nlines = int(data[1])
        idx = 2

        # loop every notes symbol
        if nlines > 0:
            # define data
            nsymbol_code = []
            nsymbol_xyz = []
            nsymbol_charLen = []
            nsymbol_char = []
            nsymbol_angle = []
            nsymbol_fontLen = []
            nsymbol_fontName = []
            nsymbol_fontSize = []
            nsymbol_fontType = []
            nsymbol_color = []

            for i in range(nlines):
                # code
                code = int(data[idx])
                nsymbol_code.append(code)
                idx += 1

                # xyz
                xyz = np.array(data[idx:(idx + 3)]).astype(float)
                nsymbol_xyz.append([xyz])
                idx += 3

                # character length
                char_len = int(data[idx])
                nsymbol_charLen.append(char_len)
                idx += 1

                # characters
                char = data[idx]
                nsymbol_char.append(char)
                idx += 1

                # character angle
                angle = data[idx]
                nsymbol_angle.append(angle)
                idx += 1

                # font length
                font_len = data[idx]
                nsymbol_fontLen.append(font_len)
                idx += 1

                # font name
                font_name = data[idx]
                nsymbol_fontName.append(font_name)
                idx += 1

                # font size
                font_size = data[idx]
                nsymbol_fontSize.append(font_size)
                idx += 1

                # font type
                font_type = data[idx]
                nsymbol_fontType.append(font_type)
                idx += 1

                # color
                color = np.array(data[idx:(idx + 4)]).astype(int)
                nsymbol_color.append(color)
                idx += 4

            notes_symbol = {
                "nsymbol_code": nsymbol_code,
                "nsymbol_xyz": nsymbol_xyz,
                "nsymbol_charLen": nsymbol_charLen,
                "nsymbol_char": nsymbol_char,
                "nsymbol_angle": nsymbol_angle,
                "nsymbol_fontLen": nsymbol_fontLen,
                "nsymbol_fontName": nsymbol_fontName,
                "nsymbol_fontSize": nsymbol_fontSize,
                "nsymbol_fontType": nsymbol_fontType,
                "nsymbol_color": nsymbol_color}
        else:
            notes_symbol = {}
        return notes_symbol

    def read_wthprop_linesymbols(self, content):
        data = content.split()
        nlines = int(data[1])
        idx = 2

        # loop every line symbol
        if nlines > 0:
            # define data
            plinesym_code = []
            plinesym_width = []
            plinesym_color = []
            plinesym_type = []
            plinesym_shadow = []
            plinesym_xyz_num = []
            plinesym_xyz = []
            plinesym_label = []
            plinesym_label_num = []
            plinesym_label_xyz = []

            for i in range(nlines):
                # line symbol code
                code = int(data[idx])
                plinesym_code.append(code)
                idx += 1

                # line width
                width = float(data[idx])
                plinesym_width.append(width)
                idx += 1

                # line color
                color = np.array(data[idx:(idx + 4)]).astype(int)
                plinesym_color.append([color])
                idx += 4

                # line type
                ltype = int(data[idx])
                plinesym_type.append(ltype)
                idx += 1

                # line shadow
                shadow = int(data[idx])
                plinesym_shadow.append(shadow)
                idx += 1

                # line symbol xyz point nlines
                xyz_num = int(data[idx])
                plinesym_xyz_num.append(xyz_num)
                idx += 1

                # line symbol xyz
                xyz = np.array(data[idx:(idx + 3 * xyz_num)]).astype(float)
                xyz.shape = [xyz_num, 3]
                plinesym_xyz.append(xyz)
                idx += 3 * xyz_num

                # line symbol label
                label = data[idx]
                plinesym_label.append(label)
                idx += 1

                # line label nlines
                label_num = int(data[idx])
                plinesym_label_num.append(label_num)
                idx += 1

                # label xyz
                if label_num > 0:
                    label_xyz = np.array(
                        data[idx:(idx + 3 * label_num)]).astype(float)
                    label_xyz.shape = [label_num, 3]
                    plinesym_label_xyz.append(label_xyz)
                    idx += label_num * 3
                else:
                    plinesym_label_xyz.append([])

            plines_symbol = {
                "plinesym_code": plinesym_code,
                "plinesym_width": plinesym_width,
                "plinesym_color": plinesym_color,
                "plinesym_type": plinesym_type,
                "plinesym_shadow": plinesym_shadow,
                "plinesym_xyz_num": plinesym_xyz_num,
                "plinesym_xyz": plinesym_xyz,
                "plinesym_label": plinesym_label,
                "plinesym_label_num": plinesym_label_num,
                "plinesym_label_xyz": plinesym_label_xyz}
        else:
            plines_symbol = {}
        return plines_symbol

    def __repr__(self):
        """print"""
        return self.head.__repr__()

    def __getitem__(self, artist: str):
        """get data of station"""
        return self.data[artist]
