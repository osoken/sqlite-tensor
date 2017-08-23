# -*- coding: utf-8 -*-

__description__ = u'sqlite_tensor'

__long_description__ = u'''sqlite_tensor
'''

__author__ = u'osoken'
__email__ = u'osoken.devel@outlook.jp'
__version__ = '0.0.1'

__package_name__ = u'sqlite_tensor'

try:
    from . import core
    Tensor = core.Tensor
    Database = core.Database
except Exception as e:
    x = e

    def _err(*args, **kwargs):
        raise x

    Tensor = _err
    Database = _err
