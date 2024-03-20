# -*- coding: utf-8 -*-
# 23/02/07
# create by: snower

import sys
import os
from setuptools import find_packages, setup

version = "0.0.13"

if os.path.exists("README.md"):
    if sys.version_info[0] >= 3:
        try:
            with open("README.md", encoding="utf-8") as fp:
                long_description = fp.read()
        except Exception as e:
            print("Waring: " + str(e))
            long_description = 'https://github.com/snower/syncany-server'
    else:
        try:
            with open("README.md") as fp:
                long_description = fp.read()
        except Exception as e:
            print("Waring: " + str(e))
            long_description = 'https://github.com/snower/syncany-server'
else:
    long_description = 'https://github.com/snower/syncany-server'

setup(
    name='syncanyserver',
    version=version,
    url='https://github.com/snower/syncany-server',
    author='snower',
    author_email='sujian199@gmail.com',
    license='MIT',
    packages=find_packages(exclude=['*tests*']),
    zip_safe=False,
    install_requires=[
        "pyyaml>=5.1.2",
        "sqlglot>=11.5.5,<12",
        "syncanysql>=0.1.19",
        "mysql-mimic>=2.2.3,<2.3"
    ],
    extras_require={
        "pymongo": ['pymongo>=3.6.1'],
        "pymysql": ['PyMySQL>=0.8.1'],
        "openpyxl": ["openpyxl>=2.5.0"],
        "postgresql": ["psycopg2>=2.8.6"],
        "elasticsearch": ["elasticsearch>=6.3.1"],
        "influxdb": ["influxdb>=5.3.1"],
        "clickhouse": ["clickhouse_driver>=0.1.5"],
        "redis": ["redis>=3.5.3"],
        "requests": ["requests>=2.22.0"],
        "pymssql": ['pymssql>=2.2.7'],
        "prql-python": ["prql-python>=0.11.1"],
    },
    package_data={
        '': ['README.md'],
    },
    entry_points={
        'console_scripts': [
            'syncany-server = syncanyserver.main:main',
        ],
    },
    description='Simple and easy to use SQL scripts to build virtual database tables MySQL Server',
    long_description=long_description,
    long_description_content_type='text/markdown'
)
