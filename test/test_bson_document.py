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
import uuid

from nose.plugins.skip import SkipTest

from bson import SON, BSON, Binary, ObjectId
from bson.py3compat import b

sys.path[0:0] = [""]

try:
    from bson._cbson import BSONBuffer, BSONDocument
except ImportError:
    BSONBuffer = None
    BSONDocument = None


class TestBSONDocument(unittest.TestCase):
    def setUp(self):
        if not BSONBuffer:
            raise SkipTest("_cbson not compiled")

    def test_repr(self):
        array = bytearray(BSON.encode(SON([('foo', 'bar'), ('oof', 1)])))
        buf = BSONBuffer(array)
        doc = next(iter(buf))
        expected_repr = '{%r: %r, %r: %r}' % ('foo', 'bar', 'oof', 1)
        self.assertEqual(expected_repr, repr(doc))
        doc.inflate()
        self.assertEqual(expected_repr, repr(doc))

        del doc['foo']
        doc['recurse'] = doc
        expected_repr = '{%r: %r, %r: {...}}' % ('oof', 1, 'recurse')
        self.assertEqual(expected_repr, repr(doc))

        self.assertEqual('{}', repr(BSONDocument()))

    def test_iteritems(self):
        array = bytearray(BSON.encode(SON([('foo', 'bar'), ('oof', 1)])))
        buf = BSONBuffer(array)
        doc = next(iter(buf))
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
        doc = next(iter(buf))
        self.assertEqual({}, dict(doc))
        self.assertRaises(KeyError, lambda: doc['key'])
        self.assertEqual([], list(doc.iteritems()))
        doc['a'] = 1
        self.assertEqual(1, doc['a'])
        self.assertEqual([('a', 1)], list(doc.iteritems()))

    def test_types(self):
        my_id = ObjectId()
        my_binary = Binary(b('foo'), subtype=2)  # Random subtype.
        my_uuid = uuid.uuid4()

        bson_bytes = BSON.encode(SON([
            ('_id', my_id),
            ('double', 1.5),
            ('document', {'k': 'v'}),
            ('array', [1, 'foo', 1.5]),
            ('binary', my_binary),
            ('uuid', my_uuid),
            ('string', 'bar'),
            ('int', 1),
            ('long', 2 << 40),
        ]))

        array = bytearray(bson_bytes)
        local_buffer = [None]

        def get_doc():
            # Keep recreating the doc to avoid auto-inflation.
            local_buffer[0] = buf = BSONBuffer(array)
            return next(iter(buf))

        self.assertEqual(my_id, get_doc()['_id'])
        self.assertEqual(1.5, get_doc()['double'])
        self.assertEqual('v', get_doc()['document']['k'])
        self.assertEqual([1, 'foo', 1.5], get_doc()['array'])
        self.assertEqual(my_binary, get_doc()['binary'])
        self.assertEqual(my_uuid, get_doc()['uuid'])
        self.assertEqual('bar', get_doc()['string'])
        self.assertEqual(1, get_doc()['int'])
        self.assertEqual(2 << 40, get_doc()['long'])

    def test_explicit_inflate(self):
        buf = BSONBuffer(bytearray(BSON.encode({})))
        doc0 = next(iter(buf))
        self.assertFalse(doc0.inflated())
        doc0.inflate()
        self.assertTrue(doc0.inflated())

    def test_inflate_before_modify(self):
        array = bytearray(BSON.encode({'foo': 'bar'}))
        buf = BSONBuffer(array)
        doc = next(iter(buf))
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

        # Recreate doc.
        buf = BSONBuffer(array)
        doc = next(iter(buf))
        self.assertFalse(doc.inflated())
        del doc['foo']
        self.assertTrue(doc.inflated())
        self.assertEqual({}, doc)

    def test_inflate_after_accesses(self):
        buf = BSONBuffer(bytearray(BSON.encode({'foo': 'bar'})))
        doc0 = next(iter(buf))
        self.assertFalse(doc0.inflated())
        for _ in range(10):
            doc0['foo']

        self.assertTrue(doc0.inflated())

    def test_iteritems_inflate(self):
        array = bytearray(BSON.encode(SON([('foo', 'bar'), ('oof', 1)])))
        buf = BSONBuffer(array)
        doc = next(iter(buf))
        iteritems = doc.iteritems()
        self.assertEqual(('foo', 'bar'), next(iteritems))

        # Inflate.
        doc['a'] = 2
        self.assertEqual(
            [('oof', 1), ('a', 2)],
            list(iteritems))

    def test_iteritems_change_size(self):
        array = bytearray(BSON.encode(SON([
            ('a', 1),
            ('b', 2),
            ('c', 3),
        ])))

        def check_doc(doc):
            iteritems = doc.iteritems()
            self.assertEqual(('a', 1), next(iteritems))

            # Change size.
            doc['x'] = 4
            self.assertEqual(('b', 2), next(iteritems))

            # Remove next element.
            del doc['c']
            self.assertEqual(('x', 4), next(iteritems))
            self.assertRaises(StopIteration, next, iteritems)

        buf = BSONBuffer(array)
        doc = next(iter(buf))
        check_doc(doc)

        # Same if document is inflated before calling iteritems().
        buf = BSONBuffer(array)
        doc = next(iter(buf))
        doc.inflate()
        check_doc(doc)

if __name__ == '__main__':
    unittest.main()
