#!/usr/bin/env python

# -*- coding: utf-8 -*-
# @Author: wqshen
# @Email: wqshen91@gmail.com
# @Date: 2019/12/27 14:42
# @Last Modified by: wqshen

import os
import sys
import argparse
import logzero
import numpy as np
import pandas as pd
import xarray as xr
from typing import Union
from logzero import logger
from datetime import datetime
from pymdfs import MdfsClient


def typecast(string: str) -> Union[int, float, str]:
    """cast string into int/float/str in sequence"""
    try:
        return int(string)
    except ValueError:
        try:
            return float(string)
        except Exception:
            return string


def args_parser(s: str) -> Union[slice, list, int, float, str]:
    """standardization of special char usage in args

    Parameters
    ----------
    s: str
        argument string, special chars and its meaning as following,
        - , represents slice from it's left to right, step can also be place the last
        , , represents list from split by ,

    Returns
    -------
    Union[slice, list, int, float, str]
    """
    if '-' in s:
        s_ = [typecast(item) for item in s.split('-')]
        if len(s_) == 2:
            return slice(*s_)
        elif len(s_) == 3:
            return list(range(*s_))
        else:
            raise Exception("s can only be split to 2, 3")
    elif ',' in s:
        return [typecast(item) for item in s.split(',')]
    else:
        return typecast(s)


def time_parser(s: str) -> list:
    """parser time string

    Parameters
    ----------
    s: str
        - , start-end-step, represents date range from it's left to right, time freq must be place the last
        , , time1,time2,time3, represents list from split by ,

    Returns
    -------
    list
    """
    parse_t = lambda t: datetime.strptime(t, '%Y%m%d%H')

    if '-' in s:
        s_ = s.split('-')
        if len(s_) != 3:
            raise Exception("s can only be split to 3")
        return pd.date_range(*map(parse_t, s_[:2]), freq=s_[-1]).to_list()
    else:
        return list(map(parse_t, s.split(',')))


def _main():
    example_text = """Example:
     # 1. 读取多个观测时间的地面24小时累计降水数据
     mdfs_dump SURFACE 2023122820,2023122920 -v RAIN24_ALL_STATION -o 10
     
     # 2. 读取多个观测时间探空TLOGP数据
     mdfs_dump UPPER_AIR 2023122820,2023122920 -v TLOGP -o 10
     
     # 3. 读取欧洲中心2024011020起报的24小时时效500hPa的湿度、风、温度、高度
     mdfs_dump ECMWF_HR 2024011020 -f 24 --level 500 -v RH,UGRD,VGRD,TMP,HGT -e ECMWF_HR.2023021920.nc
     
     # 4. 读取欧洲中心2024011020起报的24到48小时时效每3小时的500hPa的湿度
     mdfs_dump ECMWF_HR 2024011020 -f 24-48-6 --level 500 -v RH
     
     # 5. 读取欧洲中心2024011020起报的(30N,120E) 24到72小时时效每3小时的地面温度场和湿度场
     mdfs_dump ECMWF_HR 2024011020 -f 24-72-12 -v RH_2M,TMP_2M --lat 30 --lon 120
     
    # 6. 读取欧洲中心2024011020起报的(20-40N,110-130E) 24小时时效的地面温度场
     mdfs_dump ECMWF_HR 2024011020 -f 24 -v TMP_2M --lat 20-40 --lon 110-130
     
     # 7. 读取多个起报时次预报的2024011508 500hPa温度场，输出结果中time维自动转变为起报时间
     mdfs_dump ECMWF_HR 2024010920-2024011020-24H --leadtime 2024011508 --level 850 -v TMP
     
     # 8. 读取CLDAS 1km 地面2米气温并使用获取30,120的最邻近格点数据
     mdfs_dump CLDAS_1KM 2024011108-2024011116-1H -v TMP,RH --level 2M_ABOVE_GROUND -o 10 --lat 30 --lon 120
     
     # 9. 读取CLDAS 1km 地面10米风场
     mdfs_dump CLDAS_1KM 2024011116 -v WIND --level 10M_ABOVE_GROUND -o 10
     
     # 10. 获取欧洲中心集合预报数据
     mdfs_dump ECMWF_ENSEMBLE/RAW 2024011020 -f 24 -v RAIN12 --lat 20-40 --lon 110-130 -o 10
     """

    parser = argparse.ArgumentParser(description='MDFS Data Dumper', epilog=example_text,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('datasource', help='data source name')
    parser.add_argument('inittime', help='model initial or observation datetime', type=time_parser)
    parser.add_argument('-f', '--fh', help='model forecast hour', type=args_parser)
    parser.add_argument('--leadtime', help='model leadtime', type=time_parser)
    parser.add_argument('-e', '--outfile', type=str, help='output netcdf file name')
    parser.add_argument('-c', '--complevel', type=str, help='output netcdf4 compress level', default=5)
    parser.add_argument('-v', '--varname', help='model variable names', type=args_parser)
    parser.add_argument('-x', '--lon', help='longitude point or range', type=args_parser)
    parser.add_argument('-y', '--lat', help='latitude point or range', type=args_parser)
    parser.add_argument('-p', '--level', help='pressure level point or range', type=str)
    parser.add_argument('-t', '--offset-inittime', help='offset inittime (hours) to variable', type=str)
    parser.add_argument('-n', '--njobs', help='parallel thread numbers', type=int, default=1)
    parser.add_argument('--name_map', help='map variable name to new', type=args_parser)
    parser.add_argument('-s', '--server', type=str, help='GDS server address',
                        default='xxx.xxx.xxx.xxx:xxxx')
    parser.add_argument('--method', help='interpolate method to target points', default='nearest')
    parser.add_argument('-o', '--loglevel', type=int, help='logger level',
                        default=20, choices=range(10, 51, 10))

    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    args = parser.parse_args()
    logzero.loglevel(args.loglevel)
    logger.debug(args)

    coord_kwargs = {arg: getattr(args, arg) for arg in ('lon', 'lat', 'level') if getattr(args, arg) is not None}
    if (('lat' in coord_kwargs and not isinstance(coord_kwargs['lat'], slice)) or
            ('lon' in coord_kwargs and not isinstance(coord_kwargs['lon'], slice))):
        coord_kwargs['lat'] = xr.DataArray(np.atleast_1d(coord_kwargs['lat']), dims='loc')
        coord_kwargs['lon'] = xr.DataArray(np.atleast_1d(coord_kwargs['lon']), dims='loc')
        coord_kwargs['method'] = args.method
    with MdfsClient(args.server) as mc:
        mc.njobs = args.njobs
        dataset = mc.sel(args.datasource, args.inittime, args.fh, args.varname, args.leadtime,
                         merge=True, **coord_kwargs)
        logger.debug(dataset)
        if args.offset_inittime is not None:
            dataset['time'] = pd.to_datetime(dataset.time.values) + pd.to_timedelta(
                args.offset_inittime)

        if args.name_map is not None:
            dataset = dataset.rename({args.name_map[0]: args.name_map[1]})

    logger.info(dataset)
    if args.outfile is not None:
        _, file_extension = os.path.splitext(args.outfile)
        if file_extension in ('.nc', '.nc4') and isinstance(dataset, xr.Dataset):
            comp = dict(zlib=True, complevel=args.complevel)
            encoding = {var: comp for var in dataset.data_vars}
            dataset.to_netcdf(args.outfile, encoding=encoding)
        elif file_extension in ('.txt', '.csv'):
            if isinstance(dataset, xr.DataArray):
                dataset.to_dataframe().to_csv(args.outfile)
            else:
                dataset.to_csv(args.outfile)
        else:
            raise NotImplementedError("Only support nc, nc4, txt, csv extentsion.")


if __name__ == '__main__':
    _main()
