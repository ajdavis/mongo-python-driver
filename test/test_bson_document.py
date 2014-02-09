# -*- coding: utf-8 -*-
#
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
sys.path[0:0] = [""]

from nose.plugins.skip import SkipTest

import bson
from bson import BSON, SON
try:
    from bson._cbson import BSONDocument, load_from_bytearray
except ImportError:
    pass

PY3 = sys.version_info[0] == 3


class TestBSONDocument(unittest.TestCase):
    def setUp(self):
        if not bson._use_c:
            raise SkipTest("_cbson not compiled")

    def test_bson_document(self):
        # No error.
        str(BSONDocument())

        # Doesn't accept input.
        self.assertRaises(TypeError, BSONDocument, {})
        self.assertRaises(TypeError, BSONDocument, kw=1)

    def test_load_from_bytearray(self):
        bson_bytes = b''.join([
            BSON.encode(SON([('foo', 'bar'), ('oof', 'ugh')])),
            BSON.encode(SON([('fiddle', 'fazzle')]))])

        array = bytearray(bson_bytes)
        documents = list(load_from_bytearray(array))
        doc0, doc1 = documents
        self.assertTrue(isinstance(doc0, BSONDocument))
        self.assertEqual(['foo', 'oof'], doc0.keys())
        self.assertEqual(2, len(doc0))
        self.assertEqual('bar', doc0['foo'])
        self.assertEqual('ugh', doc0['oof'])

        self.assertTrue(isinstance(doc1, BSONDocument))
        self.assertEqual(['fiddle'], doc1.keys())
        self.assertEqual(1, len(doc1))
        self.assertEqual('fazzle', doc1['fiddle'])


if __name__ == '__main__':
    unittest.main()
