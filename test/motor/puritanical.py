# Copyright 2012 10gen, Inc.
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

"""Test Motor, an asynchronous driver for MongoDB and Tornado."""

import sys
import traceback
import unittest

from tornado import ioloop


class PuritanicalIOLoop(ioloop.IOLoop):
    """
    A loop that quits when it encounters an Exception.
    """
    def handle_callback_exception(self, callback):
        exc_type, exc_value, tb = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, tb)
        raise exc_value

class PuritanicalTest(unittest.TestCase):
    def setUp(self):
        super(PuritanicalTest, self).setUp()
        # So any function that calls IOLoop.instance() gets the
        # PuritanicalIOLoop instead of the default loop.
        self.stop()
        PuritanicalIOLoop().install()

    def stop(self):
        # Clear previous loop
        if ioloop.IOLoop.initialized():
            loop = ioloop.IOLoop.instance()
            if loop:
                assert not loop.running()
                loop.close()
            del ioloop.IOLoop._instance

    def tearDown(self):
        self.stop()