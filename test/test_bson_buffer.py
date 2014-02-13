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

"""Test the BSONBuffer type."""


import sys
import unittest

sys.path[0:0] = [""]

from nose.plugins.skip import SkipTest

from bson import BSON, SON, EMPTY, InvalidBSON
from bson.py3compat import b

try:
    from bson._cbson import BSONBuffer
except ImportError:
    BSONBuffer = None


class TestBSONBuffer(unittest.TestCase):
    def setUp(self):
        if not BSONBuffer:
            raise SkipTest("_cbson not compiled")

        self.bson_bytes = EMPTY.join([
            BSON.encode(SON([('foo', 'bar'), ('oof', 1)])),
            BSON.encode(SON([('fiddle', 'fazzle')]))])

        self.array = bytearray(self.bson_bytes)

    def test_bson_buffer(self):
        # Requires an argument.
        self.assertRaises(BSONBuffer)

        buf = BSONBuffer(self.array)
        self.assertTrue(isinstance(buf, BSONBuffer))
        doc0, doc1 = buf

        # TODO:
        # self.assertEqual("{'foo': 'bar', 'oof': 1}", str(doc0))
        self.assertEqual(['foo', 'oof'], doc0.keys())
        self.assertEqual(2, len(doc0))
        self.assertEqual('bar', doc0['foo'])
        self.assertEqual(1, doc0['oof'])
        try:
            doc0['not-here']
        except KeyError, e:
            self.assertEqual("'not-here'", str(e))
        else:
            self.fail('Expected KeyError')

        # TODO:
        # self.assertEqual("{'fiddle': 'fazzle'}", str(doc0))
        self.assertEqual(['fiddle'], doc1.keys())
        self.assertEqual(1, len(doc1))
        self.assertEqual('fazzle', doc1['fiddle'])

    def test_explicit_inflate(self):
        buf = BSONBuffer(self.array)
        doc0, doc1 = buf
        self.assertFalse(doc0.inflated())
        doc0.inflate()
        self.assertTrue(doc0.inflated())

    def test_inflate_after_accesses(self):
        buf = BSONBuffer(self.array)
        doc0, doc1 = buf
        self.assertFalse(doc0.inflated())
        for _ in range(10):
            doc0['foo']

        self.assertTrue(doc0.inflated())

    def test_inflate_when_buffer_destroyed(self):
        buf = BSONBuffer(self.array)
        doc0, doc1 = buf
        self.assertFalse(doc0.inflated())
        self.assertFalse(doc0.inflated())
        del buf
        self.assertTrue(doc0.inflated())
        self.assertTrue(doc1.inflated())

    def test_invalid(self):
        bson_bytes = EMPTY.join([
            BSON.encode({}),
            b('not valid bson')])

        array = bytearray(bson_bytes)
        buf = BSONBuffer(array)
        self.assertTrue(isinstance(buf, BSONBuffer))
        
        doc = next(buf)
        self.assertEqual([], doc.keys())
        self.assertEqual(0, len(doc))

        self.assertRaises(InvalidBSON, next, buf)

        # Now the iterator is invalid.
        self.assertRaises(StopIteration, next, buf)

if __name__ == '__main__':
    unittest.main()
