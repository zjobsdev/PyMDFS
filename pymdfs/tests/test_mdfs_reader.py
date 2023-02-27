# -*- coding: utf-8 -*-
# @Author: wqshen
# @Email: wqshen91@gmail.com
# @Date: 2021/5/31 15:47
# @Last Modified by: wqshen

import pytest
import xarray as xr
import pandas as pd
from datetime import datetime, timedelta
from pymdfs.client import MdfsClient


class TestReader:
    def __init__(self):
        self._server = 'xxx.xxx.xxx.xxx:xxxx'

    @property
    def server(self):
        return self._server

    @server.setter
    def server(self, address):
        self._server = address

    @property
    def inittime(self):
        it = datetime.utcnow().replace(hour=8, minute=0, second=0, microsecond=0)
        it -= timedelta(days=1)
        return it

    def test_mdfs_grid_reader(self):
        from pymdfs.mdfs.mdfs_grid_data import MdfsGridData

        fpath = r'../../sample_data/mdfs_grid/23020908.036'
        print(f'read {fpath}')
        ds = MdfsGridData(pathfile=fpath)
        print(ds.head)
        print(ds._ds)
        assert isinstance(ds._ds, xr.DataArray)

    def test_mdfs_station_reader(self):
        from pymdfs.mdfs.mdfs_station_data import MdfsStationData

        fpath = r'../../sample_data/mdfs_station/20230210110000.000'
        print(f'read {fpath}')
        ds = MdfsStationData(pathfile=fpath)
        print(ds._ds)
        assert isinstance(ds._ds, pd.DataFrame)

    def test_mdfs_station_high_reader(self):
        from pymdfs.mdfs.mdfs_station_data import MdfsStationData

        fpath = r'../../sample_data/mdfs_station/20230209200000.000'
        print(f'read {fpath}')
        ds = MdfsStationData(pathfile=fpath)
        print(ds._ds)
        assert isinstance(ds._ds, pd.DataFrame)

    def test_mdfs_client_grid_scale(self):
        gds = MdfsClient(self.server)
        dar = gds.sel('ECMWF_HR', self.inittime, fh=24, varname='RH', level=850,
                      lat=slice(20, 40), lon=slice(110, 130))
        print(dar)
        assert isinstance(dar, xr.DataArray)

    def test_mdfs_client_grid_vector(self):
        gds = MdfsClient(self.server)
        ds = gds.sel('ECMWF_HR', self.inittime, fh=24, varname='WIND', level=850,
                     lat=slice(20, 40), lon=slice(110, 130))
        print(ds)
        assert isinstance(ds, xr.Dataset)

    def test_mdfs_list_vars(self):
        gds = MdfsClient(self.server)
        da = gds.list_vars('ECMWF_HR')
        print(da)
        assert isinstance(da, list)

    def test_mdfs_client_surface_rain24(self):
        gds = MdfsClient(self.server)
        df = gds.sel('SURFACE', self.inittime, varname='RAIN24_ALL_STATION',
                     lat=slice(20, 40), lon=slice(110, 130))
        print(df)
        assert isinstance(df, pd.DataFrame)

    def test_mdfs_latlon(self):
        gds = MdfsClient(self.server)
        dar = gds.sel('RADARMOSAIC', self.inittime, varname='CREF',
                      lat=slice(20, 40), lon=slice(100, 130))
        print(dar)
        assert isinstance(dar, xr.DataArray)

    def test_tlogP(self):
        # mdfs:///UPPER_AIR/TLOGP/20230214080000.000
        gds = MdfsClient(self.server)
        df = gds.sel('UPPER_AIR', self.inittime, varname='TLOGP',
                     lat=slice(20, 40), lon=slice(100, 130))
        print(df)
        assert isinstance(df, pd.DataFrame)

    def test_mdfs_fy2_lambert(self):
        # mdfs:///SATELLITE/FY2/L1/IR2/LAMBERT/ANI_IR2_R01_20230214_0900_FY2G.AWX
        gds = MdfsClient(self.server)
        dar = gds.sel('SATELLITE', self.inittime, varname='FY2/L1/IR2/LAMBERT',
                      lat=slice(20, 40), lon=slice(100, 130))
        print(dar)
        assert isinstance(dar, xr.DataArray)

    def test_mdfs_fy2_mercator(self):
        # mdfs:///SATELLITE/FY2/L1/IR3/MERCATOR/ANI_IR3_R02_20230215_1200_FY2G.AWX
        gds = MdfsClient(self.server)
        dar = gds.sel('SATELLITE', self.inittime, varname='FY2/L1/IR3/MERCATOR',
                      lat=slice(20, 40), lon=slice(100, 130))
        print(dar)
        assert isinstance(dar, xr.DataArray)

    def test_mdfs_fy2_vis_equal(self):
        # mdfs:///SATELLITE/FY2/L1/VIS/EQUAL/ANI_VIS_R04_20230214_0800_FY2G.AWX
        gds = MdfsClient(self.server)
        dar = gds.sel('SATELLITE', self.inittime, varname='FY2/L1/VIS/EQUAL',
                      lat=slice(20, 40), lon=slice(100, 130))
        print(dar)
        assert isinstance(dar, xr.DataArray)

    def test_mdfs_fy4a_l1(self):
        # mdfs:///SATELLITE/FY4A/L1/CHINA/C012/C012_20230214101918_FY4A.AWX
        gds = MdfsClient(self.server)
        dar = gds.sel('SATELLITE', self.inittime, varname='FY4A/L1/CHINA/C012',
                      lat=slice(20, 40), lon=slice(100, 130))
        print(dar)
        assert isinstance(dar, xr.DataArray)

    def test_mdfs_fy4a_l1_vis(self):
        # mdfs:///SATELLITE/FY4A/L1/CHINA/C002/C002_20230215150000_FY4A.AWX
        gds = MdfsClient(self.server)
        dar = gds.sel('SATELLITE', self.inittime, varname='FY4A/L1/CHINA/C002',
                      lat=slice(20, 40), lon=slice(100, 130))
        print(dar)
        assert isinstance(dar, xr.DataArray)

    def test_mdfs_fy4a_l2(self):
        gds = MdfsClient(self.server)
        dar = gds.sel('SATELLITE', self.inittime, varname='FY4A/L2/CHINA/CTH',
                      lat=slice(20, 40), lon=slice(100, 130))
        print(dar)
        assert isinstance(dar, xr.DataArray)


if __name__ == '__main__':
    pytest.main(['-q', __file__])
