# -*- coding: utf-8 -*-
# @Author: wqshen
# @Date: 2019/4/6 11:59
# @Last Modified by: wqshen


from setuptools import setup, find_packages

version = "0.1"
name = "pymdfs"

install_requires = [
    "awx",
    "numpy>=1.10",
    "xarray",
    "pandas",
    "logzero>=1.0",
    "pyyaml",
    "protobuf",
    "retrying",
    "httplib2"
]

classifiers = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Science/Research/Operation",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.7",
    "Topic :: GeoScience :: Data",
]

setup(
    name=name,
    version=version,
    description="High level python interface to Micaps MDFS Data",
    author="Wenqiang Shen",
    author_email="wqshen91@163.com",
    python_requires=">=3.7",
    install_requires=install_requires,
    packages=find_packages(),
    long_description=open('README.rst', 'r', encoding='utf8').read(),
    url='https://github.com/zjobsdev/PyMDFS',
    entry_points={
        'console_scripts': [
            'mdfs_dump=pymdfs.client_dump:_main',
            'mdfs_query=pymdfs.client_query:_main',
        ]
    },
    include_package_data=True,
    zip_safe=False,
)
