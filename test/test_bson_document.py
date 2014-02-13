# Copyright 2014 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Test the BSONDocument type."""


import sys
import unittest
from bson import SON, BSON

sys.path[0:0] = [""]

from nose.plugins.skip import SkipTest

try:
    from bson._cbson import BSONBuffer
except ImportError:
    BSONBuffer = None


class TestBSONDocument(unittest.TestCase):
    def setUp(self):
        if not BSONBuffer:
            raise SkipTest("_cbson not compiled")

    def test_iteritems(self):
        array = bytearray(BSON.encode(SON([('foo', 'bar'), ('oof', 1)])))
        buf = BSONBuffer(array)
        doc = next(buf)
        iteritems = doc.iteritems()
        self.assertTrue('BSONDocument iterator' in str(iteritems))

        item0 = next(iteritems)
        self.assertEqual(('foo', 'bar'), item0)

        item1 = next(iteritems)
        self.assertEqual(('oof', 1), item1)

        self.assertRaises(StopIteration, next, iteritems)

    def test_empty(self):
        bson_bytes = BSON.encode(SON([]))
        array = bytearray(bson_bytes)
        buf = BSONBuffer(array)
        doc = next(buf)
        self.assertEqual({}, dict(doc))
        self.assertRaises(KeyError, lambda: doc['key'])
        self.assertEqual([], list(doc.iteritems()))
        doc['a'] = 1
        self.assertEqual(1, doc['a'])
        self.assertEqual([('a', 1)], list(doc.iteritems()))

    def test_types(self):
        # TODO: int32
        bson_bytes = BSON.encode(SON([
            ('string', 'bar'),
            ('long', 1),
            ('document', {'k': 'v'})
        ]))

        array = bytearray(bson_bytes)
        local_buffer = [None]
        def get_doc():
            # Keep recreating the doc to avoid auto-inflation.
            local_buffer[0] = buf = BSONBuffer(array)
            return next(buf)

        self.assertEqual('bar', get_doc()['string'])
        self.assertEqual(1, get_doc()['long'])
        self.assertEqual('v', get_doc()['document']['k'])

    def test_explicit_inflate(self):
        buf = BSONBuffer(bytearray(BSON.encode({})))
        doc0 = next(buf)
        self.assertFalse(doc0.inflated())
        doc0.inflate()
        self.assertTrue(doc0.inflated())

    def test_inflate_before_modify(self):
        array = bytearray(BSON.encode({'foo': 'bar'}))
        buf = BSONBuffer(array)
        doc = next(buf)
        self.assertEqual('bar', doc['foo'])

        # Modifying the document triggers it to inflate.
        self.assertFalse(doc.inflated())
        doc['a'] = 1
        self.assertTrue(doc.inflated())
        self.assertEqual(1, doc['a'])

        # The 'a' key comes last.
        self.assertEqual(
            [('foo', 'bar'), ('a', 1)],
            list(doc.iteritems()))

    def test_inflate_after_accesses(self):
        buf = BSONBuffer(bytearray(BSON.encode({'foo': 'bar'})))
        doc0 = next(buf)
        self.assertFalse(doc0.inflated())
        for _ in range(10):
            doc0['foo']

        self.assertTrue(doc0.inflated())

    def test_iteritems_inflate(self):
        array = bytearray(BSON.encode(SON([('foo', 'bar'), ('oof', 1)])))
        buf = BSONBuffer(array)
        doc = next(buf)
        iteritems = doc.iteritems()
        self.assertEqual(('foo', 'bar'), next(iteritems))

        # Inflate.
        doc['a'] = 2
        self.assertEqual(
            [('oof', 1), ('a', 2)],
            list(iteritems))

if __name__ == '__main__':
    unittest.main()
