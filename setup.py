#!/usr/bin/env python
from setuptools import setup

import os.path as op
CURRENT_DIR = op.dirname(__file__)

version = open(op.join(CURRENT_DIR, 'postgres_zfs_backup', 'VERSION.txt')).read()

requirements = open(op.join(CURRENT_DIR, 'requirements.txt')).read()

setup(
    name='postgres_zfs_backup',
    packages=['postgres_zfs_backup'],

    author='Stupeflix',
    author_email='thomas@stupeflix.com',
    description='PostgreSQL backup tool using ZFS',
    license='MIT',
    keywords='backup postgres postgresql zfs',
    url='https://github.com/Stupeflix/postgres_zfs_backup',

    version=version,
    include_package_data=True,
    zip_safe=False,
    install_requires=[line for line in requirements.splitlines() if line and not line.startswith("--")],
    entry_points='''
        [console_scripts]
        postgres_zfs_backup=postgres_zfs_backup.cli:main
    ''',
)
