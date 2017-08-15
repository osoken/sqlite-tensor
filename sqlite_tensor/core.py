# -*- coding: utf-8 -*-

import io
import sqlite3
import pickle

import numpy as np

from . import util


class Tensor(object):
    def __init__(self, data, attr=None, id=None, **kwargs):
        super(Tensor, self).__init__()
        super(Tensor, self).__setattr__('_data', data)
        super(Tensor, self).__setattr__(
            '_id', str(id) if id is not None else util.gen_id()
        )
        super(Tensor, self).__setattr__(
            '_attr',
            dict(attr if attr is not None else {}, **kwargs)
        )

    @property
    def id(self):
        return self._id

    @property
    def data(self):
        return self._data

    @property
    def attr(self):
        return self._attr

    def __getattr__(self, name):
        return getattr(self._data, name)

    def __getitem__(self, key):
        return self._data.__getitem__(key)

    def __setitem__(self, key, value):
        return self._data.__setitem__(key, value)


class Connection(object):
    @property
    def schema_version(self):
        return '0'

    def __init__(self, connection):
        super(Connection, self).__init__()
        self.connection = connection
        if not self.is_init():
            self.__init()

    def __init(self):
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
        cur.execute(
            'CREATE TABLE collection (' +
            'id TEXT(22) PRIMARY KEY, ' +
            'name TEXT, ' +
            'attr BLOB)'
        )
        cur.execute(
            'CREATE TABLE collection_member (' +
            'collection_id TEXT(22) NOT NULL REFERENCES collection(id), ' +
            'tensor_id TEXT(22) NOT NULL REFERENCES tensor(id), ' +
            'CONSTRAINT collection_member_pkey PRIMARY KEY (' +
            'collection_id, tensor_id))'
        )
        cur.execute(
            'CREATE INDEX collection_member_ix_collection_id ON ' +
            'collection_member (collection_id)'
        )
        cur.executemany(
            'INSERT INTO metadata (key, value) VALUES (?, ?)',
            (('schema_version', self.schema_version),
             ('initialized_at', util.now()))
        )

    def is_init(self):
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

    def save(self, tensor):
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
        cur = self.connection.cursor()
        cur.execute('DELETE FROM tensor WHERE id=?', (key, ))

    def __iter__(self):
        for x in self.connection.cursor().execute(
                'SELECT data, attr, id FROM tensor'):
            yield self.deserialize(x)

    def __len__(self):
        return list(self.connection.cursor().execute(
            'SELECT COUNT(*) FROM tensor'
        ))[0][0]

    @classmethod
    def serialize(cls, tensor):
        return (cls.serialize_array(tensor.data),
                cls.serialize_attr(tensor.attr),
                tensor.id)

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
        s = io.BytesIO()
        pickle.dump(data, s)
        s.seek(0)
        return sqlite3.Binary(s.read())

    @classmethod
    def deserialize_attr(cls, data):
        s = io.BytesIO(data)
        s.seek(0)
        return pickle.load(s)
