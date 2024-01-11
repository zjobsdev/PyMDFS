#!/usr/bin/env python

# -*- coding: utf-8 -*-
# @Author: wqshen
# @Email: wqshen91@gmail.com
# @Date: 2019/12/27 14:42
# @Last Modified by: wqshen

import sys
import argparse
import logzero
from logzero import logger
from pymdfs import MdfsClient


def _main():
    example_text = """Example: mdfs_query ECMWF_HR"""

    parser = argparse.ArgumentParser(description='MDFS Data Query',
                                     epilog=example_text,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('datasource', help='data source name')
    parser.add_argument('-s', '--server', type=str, help='GDS server address',
                        default='xxx.xxx.xxx.xxx')
    parser.add_argument('-o', '--loglevel', type=int, help='loglevel: 10, 20, 30, 40, 50',
                        default=20)

    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    args = parser.parse_args()
    logzero.loglevel(args.loglevel)
    logger.debug(args)

    mc = MdfsClient(args.server)
    print(mc.list_vars(args.datasource))


if __name__ == '__main__':
    _main()
