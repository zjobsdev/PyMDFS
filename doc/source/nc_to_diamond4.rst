netCDF to Diamond 4
===================

With the facility of basic data structure powered by xarray,
it is easy to convert data format between netCDF (.nc) grid data and Micaps Diamond 4 data.

.. python::

    import numpy as np
    import xarray as xr
    from pymdfs.diamond import diamond4

    # read Diamond 4 data and save to netCDF format
    ds = diamond4.Diamond4(pathfile=your_diamond4_data_path)
    dar = ds.values
    dar.name = 'varibale_name'
    dar.to_netcdf('the_output_netcdf_file_path.nc')

    # instance a Diamond4 class
    d4 = diamond4.Diamond4()
    # use from_xarray to convert a xarray.DataArray to initialize Diamond4
    d4.from_xarray(dar)
    # save Diamond4 to file
    d4.write(pathfile='the_output_diamond4_file_path.000', fmt='%.0f')
