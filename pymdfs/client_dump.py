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
import pandas as pd
import xarray as xr
from logzero import logger
from datetime import datetime
from pymdfs import MdfsClient


def construct_slice(coord):
    """construct point, slice, or step slice from list or tuple

    Parameters
    ----------
    coord : list or tuple
        (point,) or (start, stop) or (start, stop, step)

    Returns
    -------
    s : float, slice
        slice or point
    """
    if len(coord) == 1:
        s = coord[0]
    elif 1 < len(coord) < 3:
        s = slice(*coord)
    else:
        raise Exception("length of coord must less than or equal 3")
    return s


def tpyecast(string):
    """cast string into int/float/str"""
    try:
        return int(string)
    except ValueError:
        try:
            return float(string)
        except Exception:
            return string


def _main():
    example_text = """Example:
     client_dump ECMWF_HR 2023021920 -f 24 --level 500 -v RH,UGRD,VGRD,TMP,HGT -e ECMWF_HR.2023021920.nc
     """

    args_parser = lambda s: [tpyecast(item) for item in s.split(',')]
    parser = argparse.ArgumentParser(description='MDFS Data Dumper', epilog=example_text,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('datasource', help='data source name')
    parser.add_argument('inittime', help='model initial datetime or observation datetime')
    parser.add_argument('-f', '--fh', help='model forecast hour', type=args_parser)
    parser.add_argument('-e', '--outfile', type=str, help='output netcdf file name')
    parser.add_argument('-c', '--complevel', type=str, help='output netcdf4 compress level',
                        default=5)
    parser.add_argument('-v', '--varname', help='model variable names', type=args_parser)
    parser.add_argument('-x', '--lon', help='longitude point or range', type=args_parser)
    parser.add_argument('-y', '--lat', help='latitude point or range', type=args_parser)
    parser.add_argument('-p', '--level', help='pressure level point or range', type=args_parser)
    parser.add_argument('-t', '--offset-inittime', help='offset inittime (hours) to variable',
                        type=str)
    parser.add_argument('--name_map', help='map variable name to new', type=args_parser)
    parser.add_argument('-s', '--server', type=str, help='GDS server address',
                        default='xxx.xxx.xxx.xxx')
    parser.add_argument('-o', '--loglevel', type=int, help='logger level in number', default=20)

    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    args = parser.parse_args()
    logzero.loglevel(args.loglevel)
    logger.debug(args)

    inittime = datetime.strptime(args.inittime, '%Y%m%d%H')
    coord_kwargs = dict()
    if args.lon is not None:
        coord_kwargs['lon'] = construct_slice(args.lon)
    if args.lat is not None:
        coord_kwargs['lat'] = construct_slice(args.lat)
    if args.level is not None:
        coord_kwargs['level'] = construct_slice(args.level)

    with MdfsClient(args.server) as mc:
        dataset = mc.sel(args.datasource, inittime, args.fh, args.varname, **coord_kwargs)
        logger.debug(dataset)
        if isinstance(dataset, list):
            if isinstance(dataset[0], xr.DataArray):
                dataset = xr.merge(dataset)
            elif isinstance(dataset[0], pd.DataFrame):
                dataset = pd.concat(dataset)
        elif isinstance(dataset, xr.DataArray):
            dataset = dataset.to_dataset()
        else:
            raise TypeError(f"type of return ({type(dataset)}) not be understood.")

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
