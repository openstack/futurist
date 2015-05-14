#!/usr/bin/env python

# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2015 Yahoo! Inc. All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from setuptools import find_packages
from setuptools import setup

with open("README.rst", "r") as readme:
    long_description = readme.read()

setup(
    name='futurist',
    description='Useful additions to futures, from the future.',
    author="Joshua Harlow",
    author_email='harlowja@yahoo-inc.com',
    url='https://github.com/harlowja/futurist',
    license="ASL 2.0",
    install_requires=[
        # Only needed on 2.6 and 2.7 (can be killed/removed on 3.2+)
        'futures>=3.0',
        'six',
        'oslo.utils',
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
    ],
    keywords="futures async",
    packages=find_packages(),
    long_description=long_description,
)
