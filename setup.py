# -*- coding: utf-8 -*-

from setuptools import setup

from sqlite_tensor import (
    __version__, __author__, __description__,
    __long_description__, __email__, __package_name__
)

setup(
    name=__package_name__,
    version=__version__,
    author=__author__,
    author_email=__email__,
    license='MIT',
    url='https://github.com/osoken/sqlite-tensor',
    description=__description__,
    long_description=__long_description__,
    packages=[__package_name__],
    install_requires=['numpy', 'shortuuid']
)
