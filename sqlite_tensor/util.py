# -*- coding: utf-8 -*-


import time
from datetime import datetime

import shortuuid


shortuuid.set_alphabet(
    'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
)


def gen_id():
    """Return generated short UUID.
    """
    return shortuuid.uuid()


def dt2ts(dt):
    return int(time.mktime(dt.timetuple()) * 1000) + (dt.microsecond // 1000)


def ts2dt(ts):
    return datetime.fromtimestamp(
        int(ts) // 1000
    ).replace(microsecond=(int(ts) % 1000 * 1000))


def now():
    return dt2ts(datetime.now())
