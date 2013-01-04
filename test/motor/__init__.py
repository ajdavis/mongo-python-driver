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

import functools
import os
import time
import types
import unittest

from nose.plugins.skip import SkipTest

import pymongo
import pymongo.errors
import motor

if not motor.requirements_satisfied:
    raise SkipTest("Tornado or greenlet not installed")

from tornado import gen, ioloop

from test.test_connection import host, port

have_ssl = True
try:
    import ssl
except ImportError:
    have_ssl = False


# TODO: replicate asyncmongo's whole test suite?

class AsyncTestRunner(gen.Runner):
    def __init__(self, gen, timeout):
        # Tornado 2.3 added a second argument to Runner()
        super(AsyncTestRunner, self).__init__(gen, lambda: None)
        self.timeout = timeout

    def run(self):
        loop = ioloop.IOLoop.instance()

        try:
            super(AsyncTestRunner, self).run()
        except:
            loop.remove_timeout(self.timeout)
            loop.stop()
            raise

        if self.finished:
            loop.remove_timeout(self.timeout)
            loop.stop()


def async_test_engine(timeout_sec=5, io_loop=None):
    if not isinstance(timeout_sec, int) and not isinstance(timeout_sec, float):
        raise TypeError(
"""Expected int or float, got %s
Use async_test_engine like:
    @async_test_engine()
or:
    @async_test_engine(timeout_sec=10)""" % (
        repr(timeout_sec)))

    timeout_sec = max(timeout_sec, float(os.environ.get('TIMEOUT_SEC', 0)))
    is_done = [False]

    def decorator(func):
        def done():
            is_done[0] = True

        @functools.wraps(func)
        def _async_test(self):
            # Uninstall previous loop
            if hasattr(ioloop.IOLoop, '_instance'):
                del ioloop.IOLoop._instance

            loop = io_loop or ioloop.IOLoop.instance()
            assert not loop._stopped
            if io_loop:
                io_loop.install()

            def on_timeout():
                loop.stop()
                raise AssertionError("%s timed out" % func)

            timeout = loop.add_timeout(time.time() + timeout_sec, on_timeout)

            try:
                generator = func(self, done)
                assert isinstance(generator, types.GeneratorType), (
                    "%s should be a generator, include a yield "
                    "statement" % func
                )

                runner = AsyncTestRunner(generator, timeout)
                runner.run()
                loop.start()
                if not runner.finished:
                    # Something stopped the loop before func could finish or
                    # throw an exception.
                    raise Exception('%s did not finish' % func)

                if not is_done[0]:
                    raise Exception('%s did not call done()' % func)
            finally:
                del ioloop.IOLoop._instance # Uninstall

        return _async_test
    return decorator

async_test_engine.__test__ = False # Nose otherwise mistakes it for a test


class AssertRaises(gen.Task):
    def __init__(self, exc_type, func, *args, **kwargs):
        super(AssertRaises, self).__init__(func, *args, **kwargs)
        if not isinstance(exc_type, type):
            raise TypeError("%s is not a class" % repr(exc_type))

        if not issubclass(exc_type, Exception):
            raise TypeError(
                "%s is not a subclass of Exception" % repr(exc_type))
        self.exc_type = exc_type

    def get_result(self):
        (result, error), _ = self.runner.pop_result(self.key)
        if not isinstance(error, self.exc_type):
            if error:
                raise AssertionError("%s raised instead of %s" % (
                    repr(error), self.exc_type.__name__))
            else:
                raise AssertionError("%s not raised" % self.exc_type.__name__)
        return result


class AssertEqual(gen.Task):
    def __init__(self, expected, func, *args, **kwargs):
        super(AssertEqual, self).__init__(func, *args, **kwargs)
        self.expected = expected

    def get_result(self):
        (result, error), _ = self.runner.pop_result(self.key)
        if error:
            raise error


        if self.expected != result:
            raise AssertionError("%s returned %s\nnot\n%s" % (
                self.func, repr(result), repr(self.expected)))

        return result


class AssertTrue(AssertEqual):
    def __init__(self, func, *args, **kwargs):
        super(AssertTrue, self).__init__(True, func, *args, **kwargs)


class AssertFalse(AssertEqual):
    def __init__(self, func, *args, **kwargs):
        super(AssertFalse, self).__init__(False, func, *args, **kwargs)


class MotorTest(unittest.TestCase):
    longMessage = True # Used by unittest.TestCase
    ssl = False # If True, connect with SSL, skip if mongod isn't SSL

    def setUp(self):
        super(MotorTest, self).setUp()

        # Store a regular synchronous pymongo Connection for convenience while
        # testing. Low timeouts so we don't hang a test because, say, Mongo
        # isn't up or is hung by a long-running $where clause.
        connectTimeoutMS = socketTimeoutMS = 2000
        if self.ssl:
            if not have_ssl:
                raise SkipTest("Python compiled without SSL")
            try:
                self.sync_cx = pymongo.Connection(
                    host, port, connectTimeoutMS=connectTimeoutMS,
                    socketTimeoutMS=socketTimeoutMS,
                    ssl=True)
            except pymongo.errors.ConnectionFailure:
                raise SkipTest("mongod doesn't support SSL, or is down")
        else:
            self.sync_cx = pymongo.Connection(
                host, port, connectTimeoutMS=connectTimeoutMS,
                socketTimeoutMS=socketTimeoutMS,
                ssl=False)

        self.sync_db = self.sync_cx.pymongo_test
        self.sync_coll = self.sync_db.test_collection
        self.sync_coll.drop()

        # Make some test data
        self.sync_coll.ensure_index([('s', pymongo.ASCENDING)], unique=True)
        self.sync_coll.insert(
            [{'_id': i, 's': hex(i)} for i in range(200)], safe=True)

        self.open_cursors = self.get_open_cursors()

    def get_open_cursors(self):
        # TODO: we've found this unreliable in PyMongo testing; find instead a
        # way to track cursors Motor creates and assert they're all closed
        output = self.sync_cx.admin.command('serverStatus')
        return output.get('cursors', {}).get('totalOpen', 0)

    def motor_connection(self, host, port, *args, **kwargs):
        """Get an open MotorClient. Ignores self.ssl, you must pass 'ssl'
           argument.
        """
        return motor.MotorClient(host, port, *args, **kwargs).open_sync()

    def check_callback_handling(self, fn, required):
        """
        Take a function and verify that it accepts a 'callback' parameter
        and properly type-checks it. If 'required', check that fn requires
        a callback.
        """
        self.assertRaises(TypeError, fn, callback='foo')
        self.assertRaises(TypeError, fn, callback=1)

        if required:
            self.assertRaises(TypeError, fn)
            self.assertRaises(TypeError, fn, None)
        else:
            # Should not raise
            fn(callback=None)

        # Should not raise
        fn(callback=lambda result, error: None)

    def check_required_callback(self, fn, *args, **kwargs):
        self.check_callback_handling(
            functools.partial(fn, *args, **kwargs),
            True)

    def check_optional_callback(self, fn, *args, **kwargs):
        self.check_callback_handling(
            functools.partial(fn, *args, **kwargs),
            False)

    def tearDown(self):
        actual_open_cursors = self.get_open_cursors()
        self.assertEqual(
            self.open_cursors,
            actual_open_cursors,
            "%d open cursors at start of test, %d at end, should be equal" % (
                self.open_cursors, actual_open_cursors
                )
        )

        self.sync_coll.drop()
        super(MotorTest, self).tearDown()


class MotorTestBasic(MotorTest):
    def test_repr(self):
        cx = self.motor_connection(host, port)
        self.assertTrue(repr(cx).startswith('MotorClient'))
        db = cx.pymongo_test
        self.assertTrue(repr(db).startswith('MotorDatabase'))
        coll = db.test_collection
        self.assertTrue(repr(coll).startswith('MotorCollection'))
