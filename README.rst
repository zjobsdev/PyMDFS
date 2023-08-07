PyMDFS
======

高级的、易用的 Micaps 在线数据读取包。

PyMDFS主要包含以下特征，


#. 在线读取Micaps GDS服务器数据，模式、观测、卫星、雷达等
#. 读写Micaps Diamond数据，读取Micaps网络存储二进制格式数据
#. 读取卫星产品数据 (AWX)
#. 读取天气雷达拼图数据 (.LATLON)
#. 过滤站点和经纬度裁剪
#. 主要的数据结构为`pandas.DataFrame <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`_
   和 `xarray.DataArray <https://docs.xarray.dev/en/stable/generated/xarray.DataArray.html>`_

README
^^^^^^

- `English <https://github.com/zjobsdev/pymdfs/blob/master/README.en.rst>`_

安装方法
^^^^^^^^^^^^^^^

使用 *pip* 可以直接安装

.. code:: shell

    pip install pymdfs


简单上手
^^^^^^^^^^^^^^^

从 *Micaps* GDS 服务器中读取数据
---------------------------------------------------------------------------

`pymdfs <https://github.com/zjobsdev/pymdfs>`_ 中最常用的类为 **MdfsClient** ，
它承担了从 GDS 服务器中读取数据和裁剪经纬度的重要功能。


**MdfsClient 介绍**

- 实例化 **MdfsClient** 类时使用 GDS server `address` and `port`.
- **MdfsClient**  拉取 GDS 数据的前端接口，通过一下多个参数组合实现其功能,

  - `datasource`, GDS 服务器中子类数据的顶级路径
  - `inittime`, 数值模式的起报时间或观测数据的观测时间
  - `fh`, 模式预报时效（仅针对模式数据）
  - `varname`, 变量名, 对应于子类数据路径后移植到最后的数据目录路径，以 */* 连接
  - `level`, 模式垂直层（仅针对模式数据）
  - `lat`, 维度切片
  - `lon`, 经度切片
  - `wildcard`, 文件名通配符，如未提供此参数，程序将自动解析，但将增加运行时间

以下程序，使用 **MdfsClient** 拉取 0.125度的 ECWMF 在北京时间2023年2月20日20时起报的未来24小时的相对湿度。

.. code:: python

    from datetime import datetime
    from pymdfs import MdfsClient

    gds = MdfsClient('xxx.xxx.xxx.xxx:xxxx')
    dar = gds.sel('ECMWF_HR', datetime(2023, 2, 20, 20), fh=24, varname='RH',
                  level=850, lat=slice(20, 40), lon=slice(110, 130))
    print(dar)

以下程序，使用 **MdfsClient** 拉取2023年2月20日20时观测的24小时站点降水量，
并过滤出北纬20-40度、东经110-130度范围的站点数据，数据结构为 pandas.DataFrame


.. code:: python

    from datetime import datetime
    from pymdfs import MdfsClient

    gds = MdfsClient('xxx.xxx.xxx.xxx:xxxx')
    df = gds.sel('SURFACE', datetime(2023, 2, 20, 20), varname='RAIN24_ALL_STATION',
                 lat=slice(20, 40), lon=slice(110, 130))
    print(df)


命令行程序
^^^^^^^^^^^^^^^^^^^^^^

1. client_query
----------------

用法:
    mdfs_query [-h] [-s SERVER] [-o LOGLEVEL] datasource

MDFS数据变量查询

位置参数:
  datasource            数据名称

可选参数:
    +----------------------------------+---------------------------------+
    | arguments                        | Description                     |
    +==================================+=================================+
    | -h, --help                       | show this help message and exit |
    +----------------------------------+---------------------------------+
    | -s SERVER, --server SERVER       | GDS server address              |
    +----------------------------------+---------------------------------+
    | -o LOGLEVEL, --loglevel LOGLEVEL | loglevel: 10, 20, 30, 40, 50    |
    +----------------------------------+---------------------------------+


示例:

.. code:: python

    mdfs_query ECMWF_HR

2. client_dump
----------------

用法:
    mdfs_dump [-h] [-f FH] [-e OUTFILE] [-c COMPLEVEL] [-v VARNAME] [-x LON] [-y LAT] [-p LEVEL] [-t OFFSET_INITTIME] [--name_map NAME_MAP] [-s SERVER] [-o LOGLEVEL] datasource inittime

MDFS数据读取下载

位置参数:
    +-------------+------------------------------------------------+
    | arguments   | Description                                    |
    +=============+================================================+
    | datasource  | data source name                               |
    +-------------+------------------------------------------------+
    | inittime    | model initial datetime or observation datetime |
    +-------------+------------------------------------------------+

可选参数:
    +-------------------------------------------------------+-------------------------------------+
    | arguments                                             | Description                         |
    +=======================================================+=====================================+
    | -h, --help                                            | show this help message and exit     |
    +-------------------------------------------------------+-------------------------------------+
    | -f FH, --fh FH                                        | model forecast hour                 |
    +-------------------------------------------------------+-------------------------------------+
    | -e OUTFILE, --outfile OUTFILE                         | output netcdf file name             |
    +-------------------------------------------------------+-------------------------------------+
    | -c COMPLEVEL, --complevel COMPLEVEL                   | output netcdf4 compress level       |
    +-------------------------------------------------------+-------------------------------------+
    | -v VARNAME, --varname VARNAME                         | model variable names                |
    +-------------------------------------------------------+-------------------------------------+
    | -x LON, --lon LON                                     | longitude point or range            |
    +-------------------------------------------------------+-------------------------------------+
    | -y LAT, --lat LAT                                     | latitude point or range             |
    +-------------------------------------------------------+-------------------------------------+
    | -p LEVEL, --level LEVEL                               | pressure level point or range       |
    +-------------------------------------------------------+-------------------------------------+
    | -t OFFSET_INITTIME, --offset-inittime OFFSET_INITTIME | offset inittime (hours) to variable |
    +-------------------------------------------------------+-------------------------------------+
    | --name_map NAME_MAP                                   | map variable name to new            |
    +-------------------------------------------------------+-------------------------------------+
    | -s SERVER, --server SERVER                            | GDS server address                  |
    +-------------------------------------------------------+-------------------------------------+
    | -o LOGLEVEL, --loglevel LOGLEVEL                      | logger level in number              |
    +-------------------------------------------------------+-------------------------------------+

示例:

以下脚本使用 **client_dump** 命令行程序，拉取ECMWF 2023年2月19日20时起报的24小时预报时效，
500hPa的相对湿度、U/V风场、温度场、高度场的数据，并存储为 ECMWF_HR.2023021920.nc 文件。

.. code:: shell

     mdfs_dump ECMWF_HR 2023021920 -f 24 --level 500 -v RH,UGRD,VGRD,TMP,HGT -e ECMWF_HR.2023021920.nc


更多细节和特征，请参与项目文档 `readthedocs <www.pymdfs.readthedocs.org>`_ .
