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

"""Test replica set operations and failures. A Motor version of PyMongo's
   test/high_availability/test_ha.py
"""

import time
import unittest
from tornado import gen

from tornado.ioloop import IOLoop

from test.high_availability import ha_tools

import pymongo
from pymongo import ReadPreference
from pymongo.mongo_replica_set_client import (
    Member, Monitor, _partition_node)
from pymongo.errors import AutoReconnect, OperationFailure

import motor
from test.motor import async_test_engine, AssertEqual, AssertRaises

# Override default 30-second interval for faster testing
Monitor._refresh_interval = MONITOR_INTERVAL = 0.5


class MotorTestDirectConnection(unittest.TestCase):
    def setUp(self):
        super(MotorTestDirectConnection, self).setUp()
        members = [{}, {}, {'arbiterOnly': True}]
        res = ha_tools.start_replica_set(members)
        self.seed, self.name = res

    @async_test_engine(timeout_sec=60)
    def test_secondary_connection(self, done):
        self.c = motor.MotorReplicaSetClient(
            self.seed, replicaSet=self.name).open_sync()
        self.assertTrue(bool(len(self.c.secondaries)))
        db = self.c.pymongo_test
        yield motor.Op(db.test.remove, {}, w=len(self.c.secondaries))

        # Wait for replication...
        w = len(self.c.secondaries) + 1
        yield motor.Op(db.test.insert, {'foo': 'bar'}, w=w)

        # Test direct connection to a primary or secondary
        primary_host, primary_port = ha_tools.get_primary().split(':')
        primary_port = int(primary_port)
        (secondary_host,
         secondary_port) = ha_tools.get_secondaries()[0].split(':')
        secondary_port = int(secondary_port)
        arbiter_host, arbiter_port = ha_tools.get_arbiters()[0].split(':')
        arbiter_port = int(arbiter_port)

        # A Connection succeeds no matter the read preference
        for kwargs in [
            {'read_preference': ReadPreference.PRIMARY},
            {'read_preference': ReadPreference.PRIMARY_PREFERRED},
            {'read_preference': ReadPreference.SECONDARY},
            {'read_preference': ReadPreference.SECONDARY_PREFERRED},
            {'read_preference': ReadPreference.NEAREST},
            {'slave_okay': True}
        ]:
            conn = yield motor.Op(motor.MotorClient(
                primary_host, primary_port, **kwargs).open)
            self.assertEqual(primary_host, conn.host)
            self.assertEqual(primary_port, conn.port)
            self.assertTrue(conn.is_primary)

            # Direct connection to primary can be queried with any read pref
            self.assertTrue((yield motor.Op(conn.pymongo_test.test.find_one)))

            conn = yield motor.Op(motor.MotorClient(
                secondary_host, secondary_port, **kwargs).open)
            self.assertEqual(secondary_host, conn.host)
            self.assertEqual(secondary_port, conn.port)
            self.assertFalse(conn.is_primary)

            # Direct connection to secondary can be queried with any read pref
            # but PRIMARY
            if kwargs.get('read_preference') != ReadPreference.PRIMARY:
                self.assertTrue((
                    yield motor.Op(conn.pymongo_test.test.find_one)))
            else:
                yield AssertRaises(
                    AutoReconnect, conn.pymongo_test.test.find_one)

            # Since an attempt at an acknowledged write to a secondary from a
            # direct connection raises AutoReconnect('not master'), MotorClient
            # should do the same for unacknowledged writes.
            try:
                yield motor.Op(conn.pymongo_test.test.insert, {}, safe=False)
            except AutoReconnect, e:
                self.assertEqual('not master', e.args[0])
            else:
                self.fail(
                    'Unacknowledged insert into secondary connection %s should'
                    'have raised exception' % conn)

            # Test direct connection to an arbiter
            conn = yield motor.Op(motor.MotorClient(
                arbiter_host, arbiter_port, **kwargs).open)
            self.assertEqual(arbiter_host, conn.host)
            self.assertEqual(arbiter_port, conn.port)
            self.assertFalse(conn.is_primary)

            # See explanation above
            try:
                yield motor.Op(conn.pymongo_test.test.insert, {}, safe=False)
            except AutoReconnect, e:
                self.assertEqual('not master', e.message)
            else:
                self.fail(
                    'Unacknowledged insert into arbiter connection %s should'
                    'have raised exception' % conn)
        done()

    def tearDown(self):
        ha_tools.kill_all_members()
        self.c.close()
        super(MotorTestDirectConnection, self).tearDown()


class MotorTestPassiveAndHidden(unittest.TestCase):
    def setUp(self):
        super(MotorTestPassiveAndHidden, self).setUp()
        members = [{},
                {'priority': 0},
                {'arbiterOnly': True},
                {'priority': 0, 'hidden': True},
                {'priority': 0, 'slaveDelay': 5}
        ]
        res = ha_tools.start_replica_set(members)
        self.seed, self.name = res

    @async_test_engine(timeout_sec=60)
    def test_passive_and_hidden(self, done):
        loop = IOLoop.instance()
        self.c = motor.MotorReplicaSetClient(
            self.seed, replicaSet=self.name)
        self.c.open_sync()
        db = self.c.pymongo_test
        w = len(self.c.secondaries) + 1
        yield motor.Op(db.test.remove, w=w)
        yield motor.Op(db.test.insert, {'foo': 'bar'}, w=w)
        db.read_preference = ReadPreference.SECONDARY

        passives = ha_tools.get_passives()
        passives = [_partition_node(member) for member in passives]
        hidden = ha_tools.get_hidden_members()
        hidden = [_partition_node(member) for member in hidden]
        self.assertEqual(self.c.secondaries, set(passives))

        for mode in (
            ReadPreference.SECONDARY, ReadPreference.SECONDARY_PREFERRED
        ):
            db.read_preference = mode
            for _ in xrange(10):
                cursor = db.test.find()
                yield cursor.fetch_next
                self.assertTrue(
                    cursor.delegate._Cursor__connection_id in passives)
                self.assertTrue(
                    cursor.delegate._Cursor__connection_id not in hidden)

        ha_tools.kill_members(ha_tools.get_passives(), 2)
        yield gen.Task(loop.add_timeout, time.time() + 2 * MONITOR_INTERVAL)
        db.read_preference = ReadPreference.SECONDARY_PREFERRED

        for _ in xrange(10):
            cursor = db.test.find()
            yield cursor.fetch_next
            self.assertEqual(
                cursor.delegate._Cursor__connection_id, self.c.primary)

        done()

    def tearDown(self):
        ha_tools.kill_all_members()
        self.c.close()
        super(MotorTestPassiveAndHidden, self).tearDown()


class MotorTestHealthMonitor(unittest.TestCase):
    def setUp(self):
        super(MotorTestHealthMonitor, self).setUp()
        res = ha_tools.start_replica_set([{}, {}, {}])
        self.seed, self.name = res

    @async_test_engine(timeout_sec=60)
    def test_primary_failure(self, done):
        loop = IOLoop.instance()
        c = motor.MotorReplicaSetClient(self.seed, replicaSet=self.name)
        c.open_sync()
        self.assertTrue(bool(len(c.secondaries)))
        primary = c.primary
        secondaries = c.secondaries
        killed = ha_tools.kill_primary()
        self.assertTrue(bool(len(killed)))
        yield gen.Task(loop.add_timeout, time.time() + 1)

        # Wait for new primary to step up, and for MotorReplicaSetClient
        # to detect it.
        for _ in xrange(30):
            if c.primary != primary and c.secondaries != secondaries:
                break
            yield gen.Task(loop.add_timeout, time.time() + 1)
        else:
            self.fail("New primary not detected")
        done()

    @async_test_engine(timeout_sec=60)
    def test_secondary_failure(self, done):
        loop = IOLoop.instance()
        c = motor.MotorReplicaSetClient(self.seed, replicaSet=self.name)
        c.open_sync()
        self.assertTrue(bool(len(c.secondaries)))
        primary = c.primary
        secondaries = c.secondaries

        killed = ha_tools.kill_secondary()
        self.assertTrue(bool(len(killed)))
        self.assertEqual(primary, c.primary)

        yield gen.Task(loop.add_timeout, time.time() + 2 * MONITOR_INTERVAL)
        secondaries = c.secondaries

        ha_tools.restart_members([killed])
        self.assertEqual(primary, c.primary)

        # Wait for secondary to join, and for MotorReplicaSetClient
        # to detect it.
        for _ in xrange(30):
            if c.secondaries != secondaries:
                break
            yield gen.Task(loop.add_timeout, time.time() + 1)
        else:
            self.fail("Dead secondary not detected")
        done()

    @async_test_engine(timeout_sec=60)
    def test_primary_stepdown(self, done):
        loop = IOLoop.instance()
        c = motor.MotorReplicaSetClient(self.seed, replicaSet=self.name)
        c.open_sync()
        self.assertTrue(bool(len(c.secondaries)))
        primary = c.primary
        secondaries = c.secondaries.copy()
        ha_tools.stepdown_primary()

        # Wait for primary to step down, and for MotorReplicaSetClient
        # to detect it.
        for _ in xrange(30):
            if c.primary != primary and secondaries != c.secondaries:
                break
            yield gen.Task(loop.add_timeout, time.time() + 1)
        else:
            self.fail("New primary not detected")
        done()

    def tearDown(self):
        super(MotorTestHealthMonitor, self).tearDown()
        ha_tools.kill_all_members()


class MotorTestWritesWithFailover(unittest.TestCase):
    def setUp(self):
        super(MotorTestWritesWithFailover, self).setUp()
        res = ha_tools.start_replica_set([{}, {}, {}])
        self.seed, self.name = res

    @async_test_engine(timeout_sec=60)
    def test_writes_with_failover(self, done):
        loop = IOLoop.instance()
        c = motor.MotorReplicaSetClient(self.seed, replicaSet=self.name)
        c.open_sync()
        primary = c.primary
        db = c.pymongo_test
        w = len(c.secondaries) + 1
        yield motor.Op(db.test.remove, w=w)
        yield motor.Op(db.test.insert, {'foo': 'bar'}, w=w)
        result = yield motor.Op(db.test.find_one)
        self.assertEqual('bar', result['foo'])

        killed = ha_tools.kill_primary(9)
        self.assertTrue(bool(len(killed)))
        yield gen.Task(loop.add_timeout, time.time() + 2)

        for _ in xrange(30):
            try:
                yield motor.Op(db.test.insert, {'bar': 'baz'})

                # Success
                break
            except AutoReconnect:
                yield gen.Task(loop.add_timeout, time.time() + 1)
        else:
            self.fail("Couldn't insert after primary killed")

        self.assertTrue(primary != c.primary)
        result = yield motor.Op(db.test.find_one, {'bar': 'baz'})
        self.assertEqual('baz', result['bar'])
        done()

    def tearDown(self):
        ha_tools.kill_all_members()


class MotorTestReadWithFailover(unittest.TestCase):
    def setUp(self):
        super(MotorTestReadWithFailover, self).setUp()
        res = ha_tools.start_replica_set([{}, {}, {}])
        self.seed, self.name = res

    @async_test_engine(timeout_sec=60)
    def test_read_with_failover(self, done):
        loop = IOLoop.instance()
        c = motor.MotorReplicaSetClient(self.seed, replicaSet=self.name)
        c.open_sync()
        self.assertTrue(bool(len(c.secondaries)))

        db = c.pymongo_test
        w = len(c.secondaries) + 1
        db.test.remove({}, safe=True, w=w)
        # Force replication
        yield motor.Op(db.test.insert, [{'foo': i} for i in xrange(10)], w=w)
        yield AssertEqual(10, db.test.count)

        db.read_preference = ReadPreference.SECONDARY
        cursor = db.test.find().batch_size(5)
        yield cursor.fetch_next
        self.assertEqual(5, cursor.delegate._Cursor__retrieved)
        for i in range(5):
            cursor.next_object()
        ha_tools.kill_primary()
        yield gen.Task(loop.add_timeout, time.time() + 2)

        # Primary failure shouldn't interrupt the cursor
        yield cursor.fetch_next
        self.assertEqual(10, cursor.delegate._Cursor__retrieved)
        done()

    def tearDown(self):
        ha_tools.kill_all_members()


class MotorTestReadPreference(unittest.TestCase):
    def setUp(self):
        super(MotorTestReadPreference, self).setUp()
        members = [
            # primary
            {'tags': {'dc': 'ny', 'name': 'primary'}},

            # secondary
            {'tags': {'dc': 'la', 'name': 'secondary'}, 'priority': 0},

            # other_secondary
            {'tags': {'dc': 'ny', 'name': 'other_secondary'}, 'priority': 0},
        ]

        res = ha_tools.start_replica_set(members)
        self.seed, self.name = res

        primary = ha_tools.get_primary()
        self.primary = _partition_node(primary)
        self.primary_tags = ha_tools.get_tags(primary)
        # Make sure priority worked
        self.assertEqual('primary', self.primary_tags['name'])

        self.primary_dc = {'dc': self.primary_tags['dc']}

        secondaries = ha_tools.get_secondaries()

        (secondary, ) = [
            s for s in secondaries
            if ha_tools.get_tags(s)['name'] == 'secondary']

        self.secondary = _partition_node(secondary)
        self.secondary_tags = ha_tools.get_tags(secondary)
        self.secondary_dc = {'dc': self.secondary_tags['dc']}

        (other_secondary, ) = [
            s for s in secondaries
            if ha_tools.get_tags(s)['name'] == 'other_secondary']

        self.other_secondary = _partition_node(other_secondary)
        self.other_secondary_tags = ha_tools.get_tags(other_secondary)
        self.other_secondary_dc = {'dc': self.other_secondary_tags['dc']}

        # Synchronous PyMongo interfaces for convenience
        self.c = pymongo.mongo_replica_set_client.MongoReplicaSetClient(
            self.seed, replicaSet=self.name)
        self.db = self.c.pymongo_test
        self.w = len(self.c.secondaries) + 1
        self.db.test.remove({}, safe=True, w=self.w)
        self.db.test.insert(
            [{'foo': i} for i in xrange(10)], safe=True, w=self.w)

        self.clear_ping_times()

    def set_ping_time(self, host, ping_time_seconds):
        Member._host_to_ping_time[host] = ping_time_seconds

    def clear_ping_times(self):
        Member._host_to_ping_time.clear()

    @async_test_engine(timeout_sec=300)
    def test_read_preference(self, done):
        # This is long, but we put all the tests in one function to save time
        # on setUp, which takes about 30 seconds to bring up a replica set.
        # We pass through four states:
        #
        #       1. A primary and two secondaries
        #       2. Primary down
        #       3. Primary up, one secondary down
        #       4. Primary up, all secondaries down
        #
        # For each state, we verify the behavior of PRIMARY,
        # PRIMARY_PREFERRED, SECONDARY, SECONDARY_PREFERRED, and NEAREST
        loop = IOLoop.instance()
        c = motor.MotorReplicaSetClient(
            self.seed, replicaSet=self.name).open_sync()

        @gen.engine
        def read_from_which_host(
            rsc,
            mode,
            tag_sets=None,
            latency=15,
            callback=None
        ):
            db = rsc.pymongo_test
            db.read_preference = mode
            if isinstance(tag_sets, dict):
                tag_sets = [tag_sets]
            db.tag_sets = tag_sets or [{}]
            db.secondary_acceptable_latency_ms = latency

            cursor = db.test.find()
            try:
                yield cursor.fetch_next
                callback(cursor.delegate._Cursor__connection_id)
            except AutoReconnect:
                callback(None)

        @gen.engine
        def assertReadFrom(member, *args, **kwargs):
            callback = kwargs.pop('callback')
            for _ in range(10):
                used = yield gen.Task(
                    read_from_which_host, c, *args, **kwargs)
                self.assertEqual(member, used)

            # Done
            callback()

        @gen.engine
        def assertReadFromAll(members, *args, **kwargs):
            callback = kwargs.pop('callback')
            members = set(members)
            all_used = set()
            for _ in range(100):
                used = yield gen.Task(
                    read_from_which_host, c, *args, **kwargs)
                all_used.add(used)
                if members == all_used:
                    # Success
                    callback()
                    break

            # This will fail
            self.assertEqual(members, all_used)
            
        def unpartition_node(node):
            host, port = node
            return '%s:%s' % (host, port)

        # To make the code terser, copy modes and hosts into local scope
        PRIMARY = ReadPreference.PRIMARY
        PRIMARY_PREFERRED = ReadPreference.PRIMARY_PREFERRED
        SECONDARY = ReadPreference.SECONDARY
        SECONDARY_PREFERRED = ReadPreference.SECONDARY_PREFERRED
        NEAREST = ReadPreference.NEAREST

        primary = self.primary
        secondary = self.secondary
        other_secondary = self.other_secondary

        bad_tag = {'bad': 'tag'}

        # 1. THREE MEMBERS UP -------------------------------------------------
        #       PRIMARY
        yield gen.Task(assertReadFrom, primary, PRIMARY)

        #       PRIMARY_PREFERRED
        # Trivial: mode and tags both match
        yield gen.Task(assertReadFrom, primary, PRIMARY_PREFERRED, self.primary_dc)

        # Secondary matches but not primary, choose primary
        yield gen.Task(assertReadFrom, primary, PRIMARY_PREFERRED, self.secondary_dc)

        # Chooses primary, ignoring tag sets
        yield gen.Task(assertReadFrom, primary, PRIMARY_PREFERRED, self.primary_dc)

        # Chooses primary, ignoring tag sets
        yield gen.Task(assertReadFrom, primary, PRIMARY_PREFERRED, bad_tag)
        yield gen.Task(assertReadFrom, primary, PRIMARY_PREFERRED, [bad_tag, {}])

        #       SECONDARY
        yield gen.Task(assertReadFromAll, [secondary, other_secondary], SECONDARY, latency=9999999)

        #       SECONDARY_PREFERRED
        yield gen.Task(assertReadFromAll, [secondary, other_secondary], SECONDARY_PREFERRED, latency=9999999)

        # Multiple tags
        yield gen.Task(assertReadFrom, secondary, SECONDARY_PREFERRED, self.secondary_tags)

        # Fall back to primary if it's the only one matching the tags
        yield gen.Task(assertReadFrom, primary, SECONDARY_PREFERRED, {'name': 'primary'})

        # No matching secondaries
        yield gen.Task(assertReadFrom, primary, SECONDARY_PREFERRED, bad_tag)

        # Fall back from non-matching tag set to matching set
        yield gen.Task(assertReadFromAll, [secondary, other_secondary],
            SECONDARY_PREFERRED, [bad_tag, {}], latency=9999999)

        yield gen.Task(assertReadFrom, other_secondary,
            SECONDARY_PREFERRED, [bad_tag, {'dc': 'ny'}])

        #       NEAREST
        self.clear_ping_times()

        yield gen.Task(assertReadFromAll, [primary, secondary, other_secondary], NEAREST, latency=9999999)

        yield gen.Task(assertReadFromAll, [primary, other_secondary],
            NEAREST, [bad_tag, {'dc': 'ny'}], latency=9999999)

        self.set_ping_time(primary, 0)
        self.set_ping_time(secondary, .03) # 30 ms
        self.set_ping_time(other_secondary, 10)

        # Nearest member, no tags
        yield gen.Task(assertReadFrom, primary, NEAREST)

        # Tags override nearness
        yield gen.Task(assertReadFrom, primary, NEAREST, {'name': 'primary'})
        yield gen.Task(assertReadFrom, secondary, NEAREST, self.secondary_dc)

        # Make secondary fast
        self.set_ping_time(primary, .03) # 30 ms
        self.set_ping_time(secondary, 0)

        yield gen.Task(assertReadFrom, secondary, NEAREST)

        # Other secondary fast
        self.set_ping_time(secondary, 10)
        self.set_ping_time(other_secondary, 0)

        yield gen.Task(assertReadFrom, other_secondary, NEAREST)

        # High secondaryAcceptableLatencyMS, should read from all members
        yield gen.Task(assertReadFromAll, 
            [primary, secondary, other_secondary],
            NEAREST, latency=9999999)

        self.clear_ping_times()

        yield gen.Task(assertReadFromAll, [primary, other_secondary], NEAREST, [{'dc': 'ny'}], latency=9999999)

        # 2. PRIMARY DOWN -----------------------------------------------------
        killed = ha_tools.kill_primary()

        # Let monitor notice primary's gone
        yield gen.Task(loop.add_timeout, time.time() + 2 * MONITOR_INTERVAL)

        #       PRIMARY
        yield gen.Task(assertReadFrom, None, PRIMARY)

        #       PRIMARY_PREFERRED
        # No primary, choose matching secondary
        yield gen.Task(assertReadFromAll, [secondary, other_secondary], PRIMARY_PREFERRED, latency=9999999)
        yield gen.Task(assertReadFrom, secondary, PRIMARY_PREFERRED, {'name': 'secondary'})

        # No primary or matching secondary
        yield gen.Task(assertReadFrom, None, PRIMARY_PREFERRED, bad_tag)

        #       SECONDARY
        yield gen.Task(assertReadFromAll, [secondary, other_secondary], SECONDARY, latency=9999999)

        # Only primary matches
        yield gen.Task(assertReadFrom, None, SECONDARY, {'name': 'primary'})

        # No matching secondaries
        yield gen.Task(assertReadFrom, None, SECONDARY, bad_tag)

        #       SECONDARY_PREFERRED
        yield gen.Task(assertReadFromAll, [secondary, other_secondary], SECONDARY_PREFERRED, latency=9999999)

        # Mode and tags both match
        yield gen.Task(assertReadFrom, secondary, SECONDARY_PREFERRED, {'name': 'secondary'})

        #       NEAREST
        self.clear_ping_times()

        yield gen.Task(assertReadFromAll, [secondary, other_secondary], NEAREST, latency=9999999)

        # 3. PRIMARY UP, ONE SECONDARY DOWN -----------------------------------
        ha_tools.restart_members([killed])

        for _ in range(30):
            if ha_tools.get_primary():
                break
            yield gen.Task(loop.add_timeout, time.time() + 1)
        else:
            self.fail("Primary didn't come back up")

        ha_tools.kill_members([unpartition_node(secondary)], 2)
        self.assertTrue(pymongo.connection.Connection(
            unpartition_node(primary),
            slave_okay=True
        ).admin.command('ismaster')['ismaster'])

        yield gen.Task(loop.add_timeout, time.time() + 2 * MONITOR_INTERVAL)

        #       PRIMARY
        yield gen.Task(assertReadFrom, primary, PRIMARY)

        #       PRIMARY_PREFERRED
        yield gen.Task(assertReadFrom, primary, PRIMARY_PREFERRED)

        #       SECONDARY
        yield gen.Task(assertReadFrom, other_secondary, SECONDARY)
        yield gen.Task(assertReadFrom, other_secondary, SECONDARY, self.other_secondary_dc)

        # Only the down secondary matches
        yield gen.Task(assertReadFrom, None, SECONDARY, {'name': 'secondary'})

        #       SECONDARY_PREFERRED
        yield gen.Task(assertReadFrom, other_secondary, SECONDARY_PREFERRED)
        yield gen.Task(assertReadFrom, 
            other_secondary, SECONDARY_PREFERRED, self.other_secondary_dc)

        # The secondary matching the tag is down, use primary
        yield gen.Task(assertReadFrom, primary, SECONDARY_PREFERRED, {'name': 'secondary'})

        #       NEAREST
        yield gen.Task(assertReadFromAll, [primary, other_secondary], NEAREST, latency=9999999)
        yield gen.Task(assertReadFrom, other_secondary, NEAREST, {'name': 'other_secondary'})
        yield gen.Task(assertReadFrom, primary, NEAREST, {'name': 'primary'})

        # 4. PRIMARY UP, ALL SECONDARIES DOWN ---------------------------------
        ha_tools.kill_members([unpartition_node(other_secondary)], 2)
        self.assertTrue(pymongo.connection.Connection(
            unpartition_node(primary),
            slave_okay=True
        ).admin.command('ismaster')['ismaster'])

        #       PRIMARY
        yield gen.Task(assertReadFrom, primary, PRIMARY)

        #       PRIMARY_PREFERRED
        yield gen.Task(assertReadFrom, primary, PRIMARY_PREFERRED)
        yield gen.Task(assertReadFrom, primary, PRIMARY_PREFERRED, self.secondary_dc)

        #       SECONDARY
        yield gen.Task(assertReadFrom, None, SECONDARY)
        yield gen.Task(assertReadFrom, None, SECONDARY, self.other_secondary_dc)
        yield gen.Task(assertReadFrom, None, SECONDARY, {'dc': 'ny'})

        #       SECONDARY_PREFERRED
        yield gen.Task(assertReadFrom, primary, SECONDARY_PREFERRED)
        yield gen.Task(assertReadFrom, primary, SECONDARY_PREFERRED, self.secondary_dc)
        yield gen.Task(assertReadFrom, primary, SECONDARY_PREFERRED, {'name': 'secondary'})
        yield gen.Task(assertReadFrom, primary, SECONDARY_PREFERRED, {'dc': 'ny'})

        #       NEAREST
        yield gen.Task(assertReadFrom, primary, NEAREST)
        yield gen.Task(assertReadFrom, None, NEAREST, self.secondary_dc)
        yield gen.Task(assertReadFrom, None, NEAREST, {'name': 'secondary'})

        # Even if primary's slow, still read from it
        self.set_ping_time(primary, 100)
        yield gen.Task(assertReadFrom, primary, NEAREST)
        yield gen.Task(assertReadFrom, None, NEAREST, self.secondary_dc)

        self.clear_ping_times()
        done()

    def tearDown(self):
        self.c.close()
        ha_tools.kill_all_members()
        self.clear_ping_times()


class MotorTestReplicaSetAuth(unittest.TestCase):
    def setUp(self):
        super(MotorTestReplicaSetAuth, self).setUp()
        members = [
            {},
            {'priority': 0},
            {'priority': 0},
        ]

        res = ha_tools.start_replica_set(members, auth=True)
        self.seed, self.name = res
        self.c = pymongo.mongo_replica_set_client.MongoReplicaSetClient(
            self.seed, replicaSet=self.name)

        # Add an admin user to enable auth
        try:
            self.c.admin.add_user('admin', 'adminpass')
        except:
            # SERVER-4225
            pass
        self.c.admin.authenticate('admin', 'adminpass')
        self.c.pymongo_ha_auth.add_user('user', 'userpass')

    @async_test_engine(timeout_sec=300)
    def test_auth_during_failover(self, done):
        loop = IOLoop.instance()
        c = motor.MotorReplicaSetClient(self.seed, replicaSet=self.name)
        c.open_sync()
        db = c.pymongo_ha_auth
        res = yield motor.Op(db.authenticate, 'user', 'userpass')
        self.assertTrue(res)
        yield motor.Op(db.foo.insert,
            {'foo': 'bar'},
            safe=True, w=3, wtimeout=1000)
        yield motor.Op(db.logout)
        yield AssertRaises(OperationFailure, db.foo.find_one)

        primary = '%s:%d' % self.c.primary
        ha_tools.kill_members([primary], 2)

        # Let monitor notice primary's gone
        yield gen.Task(loop.add_timeout, time.time() + 2 * MONITOR_INTERVAL)

        # Make sure we can still authenticate
        res = yield motor.Op(db.authenticate, 'user', 'userpass')
        self.assertTrue(res)

        # And still query.
        db.read_preference = ReadPreference.PRIMARY_PREFERRED
        res = yield motor.Op(db.foo.find_one)
        self.assertEqual('bar', res['foo'])
        c.close()
        done()

    def tearDown(self):
        self.c.close()
        ha_tools.kill_all_members()


class MotorTestMongosHighAvailability(unittest.TestCase):
    def setUp(self):
        super(MotorTestMongosHighAvailability, self).setUp()
        self.seed_list = ha_tools.create_sharded_cluster()
        self.dbname = 'pymongo_mongos_ha'
        self.conn = pymongo.connection.Connection(self.seed_list)
        self.conn.drop_database(self.dbname)

    @async_test_engine(timeout_sec=300)
    def test_mongos_ha(self, done):
        conn = motor.MotorClient(self.seed_list).open_sync()
        coll = conn[self.dbname].test
        res = yield motor.Op(coll.insert, {'foo': 'bar'})
        self.assertTrue(res)

        first = '%s:%d' % (conn.host, conn.port)
        ha_tools.kill_mongos(first)
        # Fail first attempt
        yield AssertRaises(AutoReconnect, coll.count)
        # Find new mongos
        yield AssertEqual(1, coll.count)

        second = '%s:%d' % (conn.host, conn.port)
        self.assertNotEqual(first, second)
        ha_tools.kill_mongos(second)
        # Fail first attempt
        yield AssertRaises(AutoReconnect, coll.count)
        # Find new mongos
        yield AssertEqual(1, coll.count)

        third = '%s:%d' % (conn.host, conn.port)
        self.assertNotEqual(second, third)
        ha_tools.kill_mongos(third)
        # Fail first attempt
        yield AssertRaises(AutoReconnect, coll.count)

        # We've killed all three, restart one.
        ha_tools.restart_mongos(first)

        # Find new mongos
        yield AssertEqual(1, coll.count)
        done()

    def tearDown(self):
        self.conn.disconnect() # Force reconnect, things have happened ....
        self.conn.drop_database(self.dbname)
        ha_tools.kill_all_members()


if __name__ == '__main__':
    unittest.main()
