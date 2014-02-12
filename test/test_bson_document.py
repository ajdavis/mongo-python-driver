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

    def test_inflate(self):
        array = bytearray(BSON.encode(SON([('foo', 'bar')])))
        buf = BSONBuffer(array)
        doc1 = next(buf)
        self.assertEqual('bar', doc1['foo'])
        doc1['a'] = 1
        self.assertEqual(1, doc1['a'])

    def test_iteritems(self):
        array = bytearray(BSON.encode(SON([('foo', 'bar'), ('oof', 1)])))
        buf = BSONBuffer(array)
        doc = next(buf)
        iteritems = doc.iteritems()  # Returns wrong type.
        self.assertFalse('dictionary-itemiterator' in str(iteritems))

        item0 = next(iteritems)
        self.assertEqual(('foo', 'bar'), item0)

        item1 = next(iteritems)
        self.assertEqual(('oof', 1), item1)

        self.assertRaises(StopIteration, next, iteritems)

if __name__ == '__main__':
    unittest.main()
