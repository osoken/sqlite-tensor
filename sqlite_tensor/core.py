# -*- coding: utf-8 -*-

import io
import sqlite3
import pickle
try:
    from collections.abc import MutableMapping
except:
    from collections import MutableMapping

import numpy as np

from . import util


class Tensor(object):
    """``numpy.array`` with attr and id.

    :param data: ``numpy.array``
    :param attr: ``dict`` object or ``None``
    :param id: ``str``
    """

    def __init__(self, data, attr=None, id=None, **kwargs):
        super(Tensor, self).__init__()
        super(Tensor, self).__setattr__('_data', data)
        super(Tensor, self).__setattr__(
            '_id', str(id) if id is not None else util.gen_id()
        )
        super(Tensor, self).__setattr__(
            '_attr',
            None if attr is None and len(kwargs) == 0 else
            dict(attr if attr is not None else {}, **kwargs)
        )

    @property
    def id(self):
        """get id of this object
        """
        return self._id

    @property
    def data(self):
        """get data of this object
        """
        return self._data

    @property
    def attr(self):
        """get attr of this object. If ``_attr`` is None, this method creates\
        an empty dict.
        """
        if self._attr is None:
            self._attr = {}
        return self._attr

    def __getattr__(self, name):
        """direct access to ``numpy.array``\'s attributes

        :param name: ``str``
        """
        return getattr(self._data, name)

    def __getitem__(self, key):
        """direct access to ``numpy.array``\'s indexing

        :param key: any objects acceptable for ``numpy.array``\'s\
        ``__getitem__``
        """
        return self._data.__getitem__(key)

    def __setitem__(self, key, value):
        """direct access to ``numpy.array``\'s index assignation

        :param key: any objects acceptable for ``numpy.array``\'s\
        ``__setitem__``
        """
        return self._data.__setitem__(key, value)


class Database(MutableMapping):

    @property
    def schema_version(self):
        return '0'

    def __init__(self, connection):
        super(Database, self).__init__()
        if isinstance(connection, sqlite3.Connection):
            self.connection = connection
        else:
            self.connection = sqlite3.Connection(connection)
        if not self.is_init():
            self.__init_tables()

    def __init_tables(self):
        cur = self.connection.cursor()
        cur.execute(
            'CREATE TABLE metadata (key TEXT, value TEXT)'
        )
        cur.execute(
            'CREATE TABLE tensor (' +
            'id TEXT(22) PRIMARY KEY, ' +
            'data BLOB, ' +
            'attr BLOB)'
        )
        cur.executemany(
            'INSERT INTO metadata (key, value) VALUES (?, ?)',
            (('schema_version', self.schema_version),
             ('initialized_at', util.now()))
        )
        self.connection.commit()

    def is_init(self):
        """return ``True`` if this object is initialized with current schema\
        and return ``False`` otherwise.
        """
        cur = self.connection.cursor()
        masterdata = list(cur.execute(
            'SELECT * FROM sqlite_master WHERE name="metadata"'
        ))
        if len(masterdata) == 0:
            return False
        schema_version = list(cur.execute(
            'SELECT value FROM metadata ' +
            'WHERE key="schema_version"'
        ))
        if len(schema_version) > 0 and \
                schema_version[0][0] == self.schema_version:
            return True
        return False

    def save(self, tensor, commit=True):
        """update or insert ``tensor`` into the table.

        :param tensor: ``Tensor`` object
        """
        cur = self.connection.cursor()
        rec = list(cur.execute(
            'SELECT id FROM ' +
            'tensor WHERE id=?', (tensor.id, )
        ))
        if len(rec) != 0:
            cur.execute(
                'UPDATE tensor SET data=?, attr=? WHERE id=?',
                self.serialize(tensor)
            )
        else:
            cur.execute(
                'INSERT INTO tensor (data, attr, id) VALUES (?, ?, ?)',
                self.serialize(tensor)
            )
        if commit:
            self.connection.commit()

    def erase(self, tensor_id, commit=True):
        """delete tensor whose id is ``tensor_id`` from the table.

        :param tensor_id: ``str`` or ``Tensor`` object
        """
        if isinstance(tensor_id, Tensor):
            return self.erase(tensor_id.id, commit)
        cur = self.connection.cursor()
        cur.execute('DELETE FROM tensor WHERE id=?', (tensor_id, ))
        if commit:
            self.connection.commit()

    def __getitem__(self, key):
        if not isinstance(key, (str, bytes)):
            return self.__getitem__(str(key))
        d = list(self.connection.cursor().execute(
            'SELECT data, attr, id FROM tensor WHERE id=?', (key, )
        ))
        if len(d) == 0:
            raise KeyError(key)
        return self.deserialize(d[0])

    def __setitem__(self, key, value):
        if isinstance(value, Tensor):
            return self.save(Tensor(data=value.data, attr=value.attr, id=key))
        return self.save(Tensor(data=np.array(value), attr=None, id=key))

    def __delitem__(self, key):
        return self.erase(key)

    def __iter__(self):
        for x in self.connection.cursor().execute(
                'SELECT id FROM tensor'):
            yield x[0]

    def __len__(self):
        return list(self.connection.cursor().execute(
            'SELECT COUNT(*) FROM tensor'
        ))[0][0]

    @classmethod
    def serialize(cls, tensor):
        return (cls.serialize_array(tensor._data),
                cls.serialize_attr(tensor._attr),
                tensor._id)

    @classmethod
    def deserialize(cls, record):
        return Tensor(
            data=cls.deserialize_array(record[0]),
            attr=cls.deserialize_attr(record[1]),
            id=record[2]
        )

    @classmethod
    def serialize_array(cls, data):
        s = io.BytesIO()
        np.save(s, data)
        s.seek(0)
        return sqlite3.Binary(s.read())

    @classmethod
    def deserialize_array(cls, data):
        s = io.BytesIO(data)
        s.seek(0)
        return np.load(s)

    @classmethod
    def serialize_attr(cls, data):
        if data is None:
            return None
        s = io.BytesIO()
        pickle.dump(data, s)
        s.seek(0)
        return sqlite3.Binary(s.read())

    @classmethod
    def deserialize_attr(cls, data):
        if data is None:
            return None
        s = io.BytesIO(data)
        s.seek(0)
        return pickle.load(s)
