#!/usr/bin/env python
from setuptools import setup

import os.path as op
CURRENT_DIR = op.dirname(__file__)

version = open(op.join(CURRENT_DIR, 'zfs_backup', 'VERSION.txt')).read().strip()

requirements = open(op.join(CURRENT_DIR, 'requirements.txt')).read()

setup(
    name='zfs_backup',
    packages=['zfs_backup'],

    author='Stupeflix',
    author_email='thomas@stupeflix.com',
    description='Backup tool using ZFS snapshots',
    license='MIT',
    keywords='backup zfs snapshot postgres postgresql psql mysql',
    url='https://github.com/Stupeflix/zfs_backup',

    version=version,
    include_package_data=True,
    zip_safe=False,
    install_requires=[line for line in requirements.splitlines() if line and not line.startswith("--")],
    entry_points='''
        [console_scripts]
        zfs_backup=zfs_backup.cli:main
    ''',
)
