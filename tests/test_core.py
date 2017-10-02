# -*- coding: utf-8 -*-

import unittest
import sqlite3

import numpy as np

from sqlite_tensor import core


class TensorTester(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test__init__(self):
        t = core.Tensor(np.random.uniform(size=(2, 3)))
        self.assertEqual(t._data.shape, (2, 3))
        self.assertTrue(isinstance(t._id, (str, bytes)))
        self.assertTrue(len(t._id), 22)
        self.assertIsNone(t._attr)
        t = core.Tensor(np.zeros(3), None, '123')
        self.assertEqual(t._id, '123')
        t = core.Tensor(np.zeros(3), None, 123)
        self.assertEqual(t._id, '123')
        t = core.Tensor(np.zeros(3), {'a': 1}, '123')
        self.assertTrue('a' in t._attr)
        self.assertEqual(t._attr['a'], 1)
        t = core.Tensor(np.zeros(3), {'a': 1, 'b': 2}, '123', a=3)
        self.assertTrue('a' in t._attr)
        self.assertEqual(t._attr['a'], 3)
        self.assertTrue('b' in t._attr)
        self.assertEqual(t._attr['b'], 2)
        t = core.Tensor(np.zeros(3), attr={'a': 1, 'b': 2}, a=3)
        self.assertTrue('a' in t._attr)
        self.assertEqual(t._attr['a'], 3)
        self.assertTrue('b' in t._attr)
        self.assertEqual(t._attr['b'], 2)

    def test_property_id(self):
        t = core.Tensor(np.zeros(3), None, '123')
        self.assertEqual(t._id, t.id)
        self.assertEqual(t.id, '123')
        t = core.Tensor(np.zeros(3), None, 123)
        self.assertEqual(t._id, t.id)
        self.assertEqual(t.id, '123')

    def test_property_data(self):
        t = core.Tensor(np.zeros(3))
        self.assertEqual(t._data.shape, t.data.shape)
        self.assertTrue(np.all(t._data == t.data))
        self.assertTrue(np.all(t._data[:2] == t.data[:2]))
        t.data[:1] = 1
        self.assertTrue(np.all(t._data == t.data))

    def test_property_attr(self):
        t = core.Tensor(np.zeros(3), {'a': 1, 'b': 2}, a=3)
        self.assertTrue('a' in t.attr)
        self.assertEqual(t.attr['a'], 3)
        self.assertTrue('b' in t.attr)
        self.assertEqual(t.attr['b'], 2)
        t.attr['c'] = 2
        self.assertTrue('c' in t._attr)
        self.assertTrue('c' in t.attr)
        self.assertEqual(t._attr['c'], t.attr['c'])

    def test__getattr__(self):
        arr = np.random.uniform(size=(2, 3))
        t = core.Tensor(arr)
        self.assertEqual(t.shape, t.data.shape)
        arr[0, 0] = 0
        self.assertEqual(arr[0, 0], t.data[0, 0])
        for a in dir(arr):
            self.assertTrue(hasattr(t, a))

    def test__getitem__(self):
        arr = np.random.uniform(size=(5, 6))
        t = core.Tensor(arr)
        self.assertTrue(np.all(t[:, :1] == arr[:, :1]))

    def test__setitem__(self):
        arr = np.random.uniform(size=(5, 6))
        arr_bk = np.copy(arr)
        t = core.Tensor(arr)
        t[:, 1] = 0
        self.assertTrue(np.all(t == arr))
        self.assertFalse(np.all(t == arr_bk))


class TestDatabase(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test__init__(self):
        conn = sqlite3.Connection(':memory:')
        tconn = core.Database(conn)
        self.assertTrue(tconn.is_init())
        conn.cursor().execute(
            'DELETE FROM metadata WHERE key="schema_version"'
        )
        conn.commit()
        self.assertFalse(tconn.is_init())

        tconn = core.Database(':memory:')
        self.assertTrue(tconn.is_init())
        tconn.connection.cursor().execute(
            'DELETE FROM metadata WHERE key="schema_version"'
        )
        self.assertFalse(tconn.is_init())

    def test_serialize_deserialize_array(self):
        t = np.random.uniform(size=(80, 90))
        s = core.Database.serialize_array(t)
        self.assertTrue(np.all(t == core.Database.deserialize_array(s)))

    def test_serialize_deserialize_attr(self):
        t = core.Tensor(np.zeros(3), {'a': 1, 'b': 2})
        s = core.Database.deserialize_attr(
            core.Database.serialize_attr(t.attr)
        )
        self.assertTrue(all([
            t.attr[k] == s[k] for k in set(t.attr.keys()).union(s.keys())
        ]))

    def test_serialize_deserialize(self):
        t = core.Tensor(np.zeros(3), {'a': 1, 'b': 2})
        s = core.Database.deserialize(
            core.Database.serialize(t)
        )
        self.assertTrue(np.all(t.data == s.data))
        self.assertTrue(all(
            t.attr[k] == s.attr[k] for k in
            set(t.attr.keys()).union(s.attr.keys())
        ))
        self.assertEqual(t.id, s.id)

    def test_save(self):
        conn = sqlite3.Connection(':memory:')
        tconn = core.Database(conn)
        t = core.Tensor(np.random.uniform(size=(2, 3)))
        tconn.save(t)
        s = core.Database.deserialize(conn.cursor().execute(
            'SELECT data, attr, id FROM tensor'
        ).fetchone())
        self.assertTrue(np.all(t.data == s.data))
        self.assertTrue(all(
            t.attr[k] == s.attr[k] for k in
            set(t.attr.keys()).union(s.attr.keys())
        ))
        self.assertEqual(t.id, s.id)
        s.data[:, 0] = 0.0
        tconn.save(s)
        u = core.Database.deserialize(conn.cursor().execute(
            'SELECT data, attr, id FROM tensor'
        ).fetchone())
        self.assertFalse(np.all(t.data == s.data))
        self.assertTrue(all(
            t.attr[k] == s.attr[k] for k in
            set(t.attr.keys()).union(s.attr.keys())
        ))
        self.assertEqual(t.id, s.id)
        self.assertTrue(np.all(u.data == s.data))
        self.assertTrue(all(
            u.attr[k] == s.attr[k] for k in
            set(u.attr.keys()).union(s.attr.keys())
        ))
        self.assertEqual(u.id, s.id)

        t = core.Tensor({
            'x': np.random.uniform(size=(2, 3)),
            'y': np.random.uniform(size=(3, )),
            'z': np.random.uniform(size=(2, ))
        })
        tconn.save(t)
        s = core.Database.deserialize(conn.cursor().execute(
            'SELECT data, attr, id FROM tensor WHERE id=?', (t.id, )
        ).fetchone())
        for k in ('x', 'y', 'z'):
            self.assertTrue(np.all(t.data[k] == s.data[k]))

    def test__getitem__(self):
        conn = sqlite3.Connection(':memory:')
        tconn = core.Database(conn)
        t = core.Tensor(np.random.uniform(size=(2, 3)))
        tconn.save(t)
        s = tconn[t.id]
        self.assertTrue(np.all(t.data == s.data))
        self.assertTrue(all(
            t.attr[k] == s.attr[k] for k in
            set(t.attr.keys()).union(s.attr.keys())
        ))
        self.assertEqual(t.id, s.id)
        t = core.Tensor(np.random.uniform(size=(2, 3)))
        tconn.save(t)
        s = tconn[t.id]
        self.assertTrue(np.all(t.data == s.data))
        self.assertTrue(all(
            t.attr[k] == s.attr[k] for k in
            set(t.attr.keys()).union(s.attr.keys())
        ))
        self.assertEqual(t.id, s.id)
        t = core.Tensor(np.random.uniform(size=(2, 3)))
        with self.assertRaises(KeyError):
            tconn[t.id]

    def test__setitem__(self):
        tconn = core.Database(sqlite3.Connection(':memory:'))
        t = core.Tensor(np.random.uniform(size=(2, 3)))
        tconn[t.id] = t
        s = tconn[t.id]
        self.assertTrue(np.all(t.data == s.data))
        self.assertTrue(all(
            t.attr[k] == s.attr[k] for k in
            set(t.attr.keys()).union(s.attr.keys())
        ))
        self.assertEqual(t.id, s.id)
        new_id = core.util.gen_id()
        new_array = np.random.uniform(size=(5, 6))
        tconn[new_id] = new_array
        s = tconn[new_id]
        self.assertTrue(np.all(new_array == s.data))
        self.assertTrue(all(
            t.attr[k] == s.attr[k] for k in
            set(t.attr.keys()).union(s.attr.keys())
        ))
        self.assertEqual(new_id, s.id)

    def test__delitem__(self):
        tconn = core.Database(sqlite3.Connection(':memory:'))
        t = core.Tensor(np.random.uniform(size=(2, 3)))
        tconn[t.id] = t
        s = tconn[t.id]
        self.assertTrue(np.all(t.data == s.data))
        self.assertTrue(all(
            t.attr[k] == s.attr[k] for k in
            set(t.attr.keys()).union(s.attr.keys())
        ))
        del tconn[t.id]
        with self.assertRaises(KeyError):
            tconn[t.id]

    def test__iter__(self):
        tconn = core.Database(sqlite3.Connection(':memory:'))
        arrays = [np.random.uniform(size=(3, 5)) for i in range(5)]
        for d in arrays:
            tconn.save(core.Tensor(d))
        for d in tconn:
            self.assertTrue(any([
                np.all(tconn[d].data == dd) for dd in arrays
            ]))

    def test__len__(self):
        tconn = core.Database(sqlite3.Connection(':memory:'))
        self.assertEqual(len(tconn), 0)
        arrays = [np.random.uniform(size=(3, 5)) for i in range(5)]
        for d in arrays:
            tconn.save(core.Tensor(d))
        self.assertEqual(len(tconn), 5)
        for d in tconn:
            del tconn[d]
        self.assertEqual(len(tconn), 0)
