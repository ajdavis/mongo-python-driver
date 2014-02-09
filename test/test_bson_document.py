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

try:
    from bson._cbson import BSONDocument
except ImportError:
    BSONDocument = None


class TestBSONDocument(unittest.TestCase):
    def setUp(self):
        if not BSONDocument:
            raise SkipTest("_cbson not compiled")

    def test_bson_document(self):
        # No error.
        str(BSONDocument())

        # Doesn't accept input.
        self.assertRaises(TypeError, BSONDocument, {})
        self.assertRaises(TypeError, BSONDocument, kw=1)

if __name__ == '__main__':
    unittest.main()
