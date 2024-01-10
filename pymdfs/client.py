#!/usr/bin/env python
# coding=utf8
# Many method of MdfsClient rewritten from Sean on http://www.micaps.cn/MifunForum/post/list?tId=123
# Rewritten by Wenqiang Shen at 2018/8/9 Zhejiang
# Email: wqshen91@gmail.com

import os
import re
import inspect
import importlib
import pandas as pd
import xarray as xr
from copy import copy
from struct import unpack
from httplib2 import Http
from logzero import logger
from retrying import retry
from itertools import product
from urllib.parse import urljoin
from datetime import datetime, timedelta
from google.protobuf.message import DecodeError
from .mdfs import DataBlock_pb2 as DataBlock


class MdfsClient(Http):
    def __init__(self, address):
        """GDS Data Client

        Parameters
        ----------
        address: IP address and port,  xxx.xxx.xxx.xxx:xxxx

        Notes
        -----
        You can find ip and port of GDS Server in micaps.exe.config/appSetting/GDSServer1
        """
        super(MdfsClient, self).__init__()
        self.__basicUrl = f"http://{address}/DataService"

    def sel(self, datasource, inittime=None, fh=None, varname=None, leadtime=None, merge=False, **kwargs):
        """interface to select variable from file by given more filter and clip parameters

        Parameters
        ----------
        datasource (str): data source name, see Registry/DATA
        inittime (datetime, list): model initial datetime or observation datetime
        fh (int, list): forecast hour
        varname (str, list): variable name
        leadtime (datetime): lead time, only valid for model data
        kwargs (dict): other k/v arguments passed to `sel` method of specific reader

        Returns
        -------
        (xarray.DataArray, list[xarray.DataArray]): Readed variable in xarray.DataArray
        """
        datasource = [datasource] if isinstance(datasource, str) else datasource
        inittime = [inittime] if isinstance(inittime, datetime) or inittime is None else inittime
        fh = [fh] if isinstance(fh, int) or fh is None else fh
        varname = [varname] if isinstance(varname, str) or varname is None else varname
        leadtime = [leadtime] if isinstance(leadtime, datetime) or leadtime is None else leadtime
        requests = list(product(datasource, inittime, fh, varname, leadtime))

        def fetch(request):
            try:
                return self._sel(*request, **kwargs)
            except Exception as e:
                logger.exception("Worker: {} - {}".format(request, e))
                return

        datas = list(map(fetch, requests))
        if all([i is None for i in datas]):
            logger.exception(f"all requests failed.")
            raise Exception(f"all requests failed.")
        if merge:
            if isinstance(datas, list):
                if isinstance(datas[0], xr.DataArray):
                    if all([d.time == datas[0].time for d in datas]):
                        leadtime = xr.DataArray(datas[0].time.values, dims='time')
                        print(datas)
                        datas = [d.set_index(time='inittime').assign_coords(leadtime=leadtime) for d in datas]
                    datas = xr.merge(datas)
                elif isinstance(datas[0], pd.DataFrame):
                    datas = pd.concat(datas)
            elif isinstance(datas, xr.DataArray):
                datas = datas.to_datas()
            elif isinstance(datas, (list, xr.Dataset, pd.DataFrame, pd.Series)):
                pass
            else:
                raise NotImplementedError(datas)
            return datas
        else:
            return datas if len(requests) > 1 else datas[0]

    def _sel(self, datasource, inittime=None, fh=None, varname=None, leadtime=None, **kwargs):
        """Sel file variable on remote workers

        Parameters
        ----------
        inittime (datetime): model initial datetime
        datasource (str): data source name, see Registry/DATA
        fh (int): forecast hour
        varname (str): variable name
        wildcard (str): file name wildcard, speedup runtime if offered,
        **kwargs: other k/v arguments passed to `sel` method of specific reader

        Returns
        -------
        (xarray.DataArray): variable
        """
        directory = f'{datasource}/{varname}'
        level = kwargs.pop('level', None)
        if level is not None:
            directory += f'/{level}'

        assert inittime is not None or leadtime is not None

        if fh is None:
            if leadtime is not None and inittime is not None:
                fh = int((leadtime - inittime).total_seconds() / 3600.)
            else:
                fh = 0

        if inittime is None:
            inittime = leadtime - timedelta(hours=fh)

        wildcard = kwargs.pop('wildcard', None)
        if wildcard is None:
            wildcard = self.guess_filename_wildcard(directory)

        filename = wildcard.format(inittime=inittime, fh=fh)
        ret = self.get_data(directory, filename)
        cls = self.guess_interface_class(filename, ret.byteArray)
        cls.read(ret.byteArray)
        return cls.sel(**kwargs)

    def guess_interface_class(self, filename: str, contents: bytes):
        """

        Parameters
        ----------
        filename: str
            file name
        contents: bytes
            GDS getData

        Returns
        -------
        cls: class
            interface class
        """
        if filename.upper().endswith('AWX'):
            from awx import Awx
            cls = Awx()
        elif filename.upper().endswith('LATLON'):
            from .latlon import LatLon
            cls = LatLon()
        else:
            discriminator, dtype = unpack('4sh', contents[:6])
            if discriminator == b'mdfs':
                if dtype in (4, 11):
                    from .mdfs.mdfs_grid_data import MdfsGridData
                    cls = MdfsGridData()
                else:
                    from .mdfs.mdfs_station_data import MdfsStationData
                    cls = MdfsStationData()
            else:
                discriminator, dtype = contents.decode().split()[:2]
                if discriminator.lower() == 'diamond':
                    Cls = getattr(importlib.import_module(f'.diamond{dtype}',
                                                          'pymdfs.diamond'),
                                  f'Diamond{dtype}')
                    cls = Cls()
                else:
                    raise NotImplementedError(f"Unsupported file format.")
        if cls is None:
            raise Exception("Failed to guess file type.")
        return cls

    def guess_filename_wildcard(self, directory: str) -> str:
        filelist = self.get_file_list(directory)
        latest = sorted(list(filelist.resultMap.keys()))[-1]
        if re.match(r'.*(\d{14})\.(\d{3})', latest):
            wildcard = '{inittime:%Y%m%d%H%M%S}.{fh:03d}'
        elif re.match(r'.*(\d{8})\.(\d{3})$', latest):
            wildcard = '{inittime:%y%m%d%H}.{fh:03d}'
        elif re.match(r'.*(\d{8})([._])(\d{6}).*', latest):
            wildcard = re.sub(r'(.*)(\d{8})([._])(\d{6})(.*)',
                              r'\g<1>{inittime:%Y%m%d}\g<3>{inittime:%H%M%S}\g<5>',
                              latest)
        elif re.match(r'.*(\d{8})([._])(\d{4}).*', latest):
            wildcard = re.sub(r'(.*)(\d{8})([._])(\d{4})(.*)',
                              r'\g<1>{inittime:%Y%m%d}\g<3>{inittime:%H%M}\g<5>',
                              latest)
        elif re.match(r'.*(\d{14}).*', latest):
            wildcard = re.sub(r'(.*)(\d{14})(.*)', r'\g<1>{inittime:%Y%m%d%H%M%S}\g<3>', latest)
        else:
            raise NotImplementedError(f"filename {latest} can not be guess wildcard.")

        return wildcard

    def get_latest_data_name(self, directory: str, filter: str = "*") -> DataBlock.StringResult:
        """get latest data filename in GDS server at given directory and wildcard

        Parameters
        ----------
        directory: str
            data path in GDS server, like "ECMWF_HR/TMP/850"
        filter: str
            filename wildcard, for instance, "*.024" represents file name ends with 024

        Returns
        -------
        string_result: DataBlock.StringResult
        """
        url = self.get_concate_url("getLatestDataName", directory, "", filter, "")
        response, content = self.request(url)
        string_result = DataBlock.StringResult()
        string_result.ParseFromString(content)
        self.result_check(string_result)
        return string_result

    @retry(stop_max_attempt_number=3)
    def get_file_list(self, directory: str) -> DataBlock.MapResult:
        """get file list in GDS server at given directory

        Parameters
        ----------
        directory: str
            data path in GDS server, like "ECMWF_HR/TMP/850"

        Returns
        -------
        map_result: DataBlock.MapResult
        """
        url = self.get_concate_url("getFileList", directory, "", "", "")
        response, content = self.request(url)
        map_result = DataBlock.MapResult()
        map_result.ParseFromString(content)
        self.result_check(map_result)
        return map_result

    def list_vars(self, directory: str) -> DataBlock.MapResult:
        """list variables in GDS server at given directory

        Parameters
        ----------
        directory: str
            data path in GDS server, like "ECMWF_HR"

        Returns
        -------
        map_result: DataBlock.MapResult
        """
        result = self.get_file_list(directory)
        variables = [k for k, v in result.resultMap.items() if v == 'D']
        return variables

    @retry(stop_max_attempt_number=3)
    def get_data(self, directory: str, filename: str) -> DataBlock.ByteArrayResult:
        """download specific file from GDS server

        Parameters
        ----------
        directory: str
            data path in GDS server, like "ECMWF_HR/TMP/850"
        filename: str
            filename in directory

        Returns
        -------
        byte_array_result: DataBlock.ByteArrayResult
        """
        url = self.get_concate_url("getData", directory, filename, "", "")
        logger.debug(url)
        response, content = self.request(url)
        byte_array_result = DataBlock.ByteArrayResult()
        byte_array_result.ParseFromString(content)
        self.result_check(byte_array_result)
        return byte_array_result

    def write_byte_array(self, url: str, byte_array: str):
        """write contents to GDS server

        Parameters
        ----------
        url: str
            url in GDS server
        byte_array: str
        """
        new_url = self.get_concate_url("writeByteArrayDataWithURL", "", "", "", url)
        return self.send_http_message(new_url, byte_array)

    def send_http_message(self, url: str, byte_array):
        """POST content to server

        Parameters
        ----------
        url: str
        byte_array
        """
        # 异常情况,不应出现,健壮性保护
        if byte_array is None or len(byte_array) == 0 or url is None or len(url) == 0:
            raise Exception("Argument Error.")
        start_time = datetime.now()
        self.request(url, method='POST', headers={"content-type": "application/octet-stream"})
        pass

    def get_concate_url(self, requestType: str, directory: str = None, fileName: str = None,
                        filter: str = None, url: str = None):
        """concate all arguments to GDS url

        Parameters
        ----------
        requestType: str
        directory: str
            data path in GDS server, like "ECMWF_HR/TMP/850"
        fileName: str
            filename in directory
        filter: str
            filename wildcard, for instance, "*.024" represents file name ends with 024
        url: str
            url in GDS server for write bytes array

        Returns
        -------
        url: str
            concate url
        """
        new_url = [self.__basicUrl]
        new_url.append("?requestType=" + requestType)  # requestType始终不为空
        if (directory is not None) and len(directory) > 0:
            new_url.append("&directory=" + directory)
        if (fileName is not None) and len(fileName) > 0:
            new_url.append("&fileName=" + fileName)
        if (filter is not None) and len(filter) > 0:
            new_url.append("&filter=" + filter)
        if (url is not None) and len(url) > 0:
            new_url.append("&url=" + url)
        url = "".join(new_url)
        return url

    def result_check(self, result):
        if result is None:  # 读取服务器发生IO异常
            raise Exception("{}：读取GDS服务器发生IO异常,程序结束".format(inspect.stack()[1][3]))
        if result.errorCode != 0:  # 没有发生IO异常, 但出现其它错误, 比如数据不存在等
            if result.errorMessage == "NotFoundException":
                raise FileNotFoundError("Error, file not found.")
            raise Exception(
                "{}: Code {}, Message {}, 程序结束。".format(inspect.stack()[1][3], result.errorCode,
                                                            result.errorMessage))

    def walk(self, directory: str):
        """recursive get all file path of given path in GDS server to setup a files tree

        Parameters
        ----------
        directory: str
            path to GDS server
        """
        if directory[-1] != "/":
            directory += '/'
        result = self.get_file_list(directory)
        dirs, nondirs, nondirsize, walk_dirs = [], [], [], []
        dirs.extend([k for k, v in result.resultMap.items() if v == 'D'])
        nondirs.extend([k for k, v in result.resultMap.items() if v != 'D'])
        nondirsize.extend([int(v) for k, v in result.resultMap.items() if v != 'D'])
        for dirname in dirs:
            new_path = urljoin(directory, dirname)
            yield from self.walk(new_path)
        yield directory, dirs, nondirs, nondirs

    def get_path_file_list(self, path: str):
        """recursive get all file path of given path in GDS server

        Parameters
        ----------
        path: str
            path to GDS server
        """
        for top, dir, files, sizes in self.walk(path):
            if len(dir) == 0 and len(files) > 0:
                for file in files:
                    yield os.path.join(top, file)

    def download(self, pathfile: str, filesize: int = None, outdir: str = "S:/micaps") -> int:
        """Download file from GDS server to local disk

        Parameters
        ----------
        pathfile: str
            path to file in GDS server
        filesize: int
            file size
        outdir: str
            path to save downloaded files

        Returns
        -------
         0 - file has existed
        -1 - Decode Error
        -2 - File not found
        """

        logger.debug("Download file {} and save to {}".format(pathfile, outdir))
        directory = os.path.dirname(pathfile)
        filename = os.path.basename(pathfile)
        outpath = "{}/{}".format(outdir, directory)
        if not os.path.exists(outpath):
            os.makedirs(outpath)
        pathfile = os.path.join(outpath, filename)
        if os.path.isfile(pathfile):
            logger.warning('{}文件已存在，跳过。'.format(pathfile))
            return 0
        try:
            result = self.get_data(directory, filename)
        except DecodeError:
            logger.error('google.protobuf.message.DecodeError! Skip file {}'.format(pathfile))
            return -1
        except FileNotFoundError:
            logger.error('File not found, skip file {}'.format(pathfile))
            return -2
        with open(pathfile, "wb") as f:
            if filesize is not None:
                logger.debug("{} - {:.1f}kb - {:.1f}kb".format(filename, filesize / 1024.,
                                                               len(result.byteArray) / 1024.))
            else:
                logger.debug("{} - {:.1f}kb".format(filename, len(result.byteArray) / 1024.))
            f.write(result.byteArray)
        if filesize is not None:
            self.file_size_verify(pathfile, filesize)

    def file_size_verify(self, pathfile: str, size: int) -> int:
        """verify file size

        Parameters
        ----------
        pathfile: str
            path to local file
        size: int
            expected file size
        Returns
        -------
        -1 for failure, 0 for success
        """
        fkbytes = os.path.getsize(pathfile) / 1024
        wkbytes = size / 1024
        if wkbytes - fkbytes > 1:
            logger.warning("{}：File {} size verify failed!".format(inspect.stack()[1][3], pathfile))
            return -1
        else:
            return 0

    def parallel_download(self, filelist: list, outdir: str = "S:/micaps", njobs: int = 4):
        """parallel download file in GDS server

        Parameters
        ----------
        filelist: list
            file list to download
        outdir: str
            path to save downloaded files
        njobs: int
            number of cpu counts in parallel download
        """
        from pathos.multiprocessing import ProcessingPool

        pool = ProcessingPool(njobs)
        f = lambda file: self.download(file, outdir=outdir)
        pool.map(f, filelist)
        pool.close()
        pool.join()

    def directory_sync(self, directory: str, outdir: str = "S:/micaps", njobs: int = 4):
        """sync local and gds server

        Parameters
        ----------
        directory: str
            data path in GDS Server
        outdir: str
            local path
        njobs: int
            number of cpu counts in parallel download
        """
        pathfiles = self.get_path_file_list(directory)
        self.parallel_download(pathfiles, outdir=outdir, njobs=njobs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        del self.__basicUrl
