# Copyright 2011-2012 10gen, Inc.
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

"""Motor, an asynchronous driver for MongoDB and Tornado."""

import functools
import socket
import time
import uuid

from tornado import ioloop, iostream, gen, stack_context
import greenlet

import pymongo
import pymongo.master_slave_connection
import pymongo.database
import pymongo.collection
import pymongo.son_manipulator
import pymongo.errors
from pymongo import common
from pymongo.pool import BasePool, NO_REQUEST

# Hook for unittesting; if true all MotorSockets get unique ids
socket_uuid = False

__all__ = ['MotorConnection', 'MotorReplicaSetConnection']

# TODO: sphinx-formatted docstrings
# TODO: document that all Motor methods accept a callback, but only
#   some require them. Get that into Sphinx for each async method somehow
# TODO: note you can't use from multithreaded app, can't fork, consider special
#   checks to prevent it?
# TODO: document that default timeout is None, ensure we're doing
#   timeouts as efficiently as possible, test performance hit with timeouts
#   from registering and cancelling timeouts
# TODO: examine & document what connection and network timeouts mean here
# TODO: verify cursors are closed ASAP
# TODO: document use of MotorConnection.delegate, MotorDatabase.delegate, etc.
# TODO: document that with a callback passed in, Motor's default is
#   to do SAFE writes, unlike PyMongo.
# TODO: what about PyMongo BaseObject's underlying safeness, as well
#   as w, wtimeout, and j? how do they affect control? test that.
# TODO: check handling of safe and get_last_error_options() and kwargs,
#   make sure we respect them
# TODO: test tailable cursor a little more
# TODO: SSL, IPv6
# TODO: document which versions of greenlet and tornado this has been tested
#   against, include those in some file that pip or pypi can understand?
# TODO: document requests and describe how concurrent ops are prevented,
#   demo how to avoid errors. Describe using NullContext to clear request.
# TODO: document that Motor doesn't do auto_start_request
# TODO: is while cursor.alive or while True the right way to iterate with
#   gen.engine and next()?
# TODO: document, smugly, that Motor has configurable IOLoops


def check_callable(kallable, required=False):
    if required and not kallable:
        raise TypeError("callable is required")
    if kallable is not None and not callable(kallable):
        raise TypeError("callback must be callable")


def motor_sock_method(check_closed=False):
    def wrap(method):
        @functools.wraps(method)
        def _motor_sock_method(self, *args, **kwargs):
            child_gr = greenlet.getcurrent()
            assert child_gr.parent, "Should be on child greenlet"

            # We need to alter this value in inner functions, hence the list
            timeout = [None]
            self_timeout = self.timeout
            if self_timeout:
                def timeout_err():
                    timeout[0] = None
                    self.stream.set_close_callback(None)
                    self.stream.close()
                    child_gr.throw(socket.timeout("timed out"))

                timeout[0] = self.stream.io_loop.add_timeout(
                    time.time() + self_timeout, timeout_err
                )

            # This is run by IOLoop on the main greenlet when socket has
            # connected; switch back to child to continue processing
            def callback(result=None, error=None):
                self.stream.set_close_callback(None)
                if timeout[0] or not self_timeout:
                    # We didn't time out
                    if timeout[0]:
                        self.stream.io_loop.remove_timeout(timeout[0])

                    if error:
                        child_gr.throw(error)
                    else:
                        child_gr.switch(result)

            if check_closed:
                def closed():
                    # There's no way to know what the error was, see
                    # https://groups.google.com/d/topic/python-tornado/3fq3mA9vmS0/discussion
                    child_gr.throw(socket.error("error"))
                self.stream.set_close_callback(closed)

            method(self, *args, callback=callback, **kwargs)

            # Resume main greenlet
            return child_gr.parent.switch()

        return _motor_sock_method
    return wrap


class MotorSocket(object):
    """
    Replace socket with a class that yields from the current greenlet, if we're
    on a child greenlet, when making blocking calls, and uses Tornado IOLoop to
    schedule child greenlet for resumption when I/O is ready.

    We only implement those socket methods actually used by pymongo.
    """
    def __init__(self, sock, io_loop, use_ssl=False):
        self.use_ssl = use_ssl
        self.timeout = None
        if self.use_ssl:
           self.stream = iostream.SSLIOStream(sock, io_loop=io_loop)
        else:
           self.stream = iostream.IOStream(sock, io_loop=io_loop)

        # Unittest hook
        if socket_uuid:
            self.uuid = uuid.uuid4()

    def setsockopt(self, *args, **kwargs):
        self.stream.socket.setsockopt(*args, **kwargs)

    def settimeout(self, timeout):
        # IOStream calls socket.setblocking(False), which does settimeout(0.0).
        # We must not allow pymongo to set timeout to some other value (a
        # positive number or None) or the socket will start blocking again.
        # Instead, we simulate timeouts by interrupting ourselves with
        # callbacks.
        self.timeout = timeout

    @motor_sock_method(check_closed=True)
    def connect(self, pair, callback):
        """
        @param pair: A tuple, (host, port)
        """
        self.stream.connect(pair, callback)

    @motor_sock_method()
    def sendall(self, data, callback):
        self.stream.write(data, callback)

    @motor_sock_method()
    def recv(self, num_bytes, callback):
        self.stream.read_bytes(num_bytes, callback)

    def close(self):
        if self.stream:
            self.stream.close()

    def fileno(self):
        return self.stream.socket.fileno()

    def __del__(self):
        self.close()


class MotorPool(pymongo.pool.BasePool):
    """A simple connection pool of MotorSockets.
    """
    def __init__(self, io_loop, *args, **kwargs):
        self.io_loop = io_loop
        self._current_request_to_sock = {}
        self._request_socks_outstanding = set()
        super(MotorPool, self).__init__(*args, **kwargs)

    def create_connection(self, pair):
        assert greenlet.getcurrent().parent, "Should be on child greenlet"

        # Don't try IPv6 if we don't support it.
        family = socket.AF_INET
        if socket.has_ipv6:
            family = socket.AF_UNSPEC

        if not (pair or self.pair):
            raise pymongo.errors.OperationFailure(
                "(host, port) pair not configured")

        host, port = pair or self.pair
        err = None
        for res in socket.getaddrinfo(host, port, family, socket.SOCK_STREAM):
            af, socktype, proto, dummy, sa = res

            # TODO: support IPV6; somehow we're not properly catching the error
            # right now and trying IPV4 as we intend in this loop, see
            # MotorSocket.connect()
            if af == socket.AF_INET6:
                continue
            try:
                sock = socket.socket(af, socktype, proto)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

                motor_sock = MotorSocket(sock, self.io_loop, use_ssl=self.use_ssl)
                motor_sock.settimeout(self.conn_timeout)

                # MotorSocket will pause the current greenlet and resume it
                # when connection has completed
                motor_sock.connect(pair or self.pair)
                motor_sock.settimeout(self.net_timeout) # TODO: necessary? BasePool.connect() handles this
                return motor_sock

            except socket.error, e:
                err = e

        if err is not None:
            raise err
        else:
            # This likely means we tried to connect to an IPv6 only
            # host with an OS/kernel or Python interpeter that doesn't
            # support IPv6. The test case is Jython2.5.1 which doesn't
            # support IPv6 at all.
            raise socket.error('getaddrinfo failed')

    def get_socket(self, pair=None):
        if self.in_request():
            if current_request in self._request_socks_outstanding:
                # TODO: better error message
                raise pymongo.errors.InvalidOperation(
                    "Can't begin concurrent operations in a request"
                )
            # We're giving out a socket in a request, keep track of this to
            # ensure the socket isn't given out twice without being returned
            # in between.
            self._request_socks_outstanding.add(current_request)

        return super(MotorPool, self).get_socket(pair)

    def return_socket(self, sock_info):
        if self.in_request():
            self._request_socks_outstanding.discard(current_request)
        super(MotorPool, self).return_socket(sock_info)

    def _set_request_state(self, sock_info):
        if sock_info == NO_REQUEST:
            self._current_request_to_sock.pop(current_request, None)
            self._request_socks_outstanding.discard(current_request)
        else:
            self._current_request_to_sock[current_request] = sock_info

    def _get_request_state(self):
        return self._current_request_to_sock.get(current_request, NO_REQUEST)

    def _reset(self):
        self._current_request_to_sock.clear()


def asynchronize(io_loop, sync_method, has_safe_arg, callback_required):
    """
    @param io_loop:           A Tornado IOLoop
    @param sync_method:       Bound method of pymongo Collection, Database,
                              Connection, or Cursor
    @param has_safe_arg:      Whether the method takes a 'safe' argument
    @param callback_required: If True, raise TypeError if no callback is passed
    """
    assert isinstance(io_loop, ioloop.IOLoop)

    # TODO doc
    # TODO: staticmethod of base class for Motor objects, add some custom
    #   stuff, like Connection can't do anything before open()
    @functools.wraps(sync_method)
    def method(*args, **kwargs):
        callback = kwargs.get('callback')
        check_callable(callback, required=callback_required)

        if 'callback' in kwargs:
            # Don't pass callback to sync_method
            kwargs = kwargs.copy()
            del kwargs['callback']

        if 'safe' not in kwargs and has_safe_arg:
            kwargs['safe'] = bool(callback)

        def call_method():
            result, error = None, None
            try:
                result = sync_method(*args, **kwargs)
            except Exception, e:
                error = e

            # Schedule the callback to be run on the main greenlet
            if callback:
                io_loop.add_callback(
                    functools.partial(callback, result, error)
                )
            elif error:
                # TODO: correct?
                raise error

        # Start running the operation on a greenlet
        greenlet.greenlet(call_method).switch()

    return method


current_request = None
current_request_seq = 0


class DelegateProperty(object):
    pass


# TODO doc
class Async(DelegateProperty):
    def __init__(self, has_safe_arg, cb_required):
        """
        @param has_safe_arg:    Whether the method takes a 'safe' argument
        @param cb_required:     Whether callback is required or optional
        """
        self.has_safe_arg = has_safe_arg
        self.cb_required = cb_required
        self.name = None

    def __get__(self, obj, objtype):
        # self.name is set by MotorMeta
        sync_method = getattr(obj.delegate, self.name)
        return asynchronize(
            obj.get_io_loop(),
            sync_method,
            has_safe_arg=self.has_safe_arg,
            callback_required=self.cb_required
        )


class ReadOnlyDelegateProperty(DelegateProperty):
    def __get__(self, obj, objtype):
        # self.name is set by MotorMeta
        return getattr(obj.delegate, self.name)


class ReadWriteDelegateProperty(ReadOnlyDelegateProperty):
    def __set__(self, obj, val):
        # self.name is set by MotorMeta
        setattr(obj.delegate, self.name, val)


class MotorMeta(type):
    def __new__(cls, name, bases, attrs):
        # Create the class.
        new_class = type.__new__(cls, name, bases, attrs)

        # Set DelegateProperties' names
        for name, attr in attrs.items():
            if isinstance(attr, DelegateProperty):
                attr.name = name

        return new_class


class MotorBase(object):
    __metaclass__ = MotorMeta
    def __cmp__(self, other):
        if isinstance(other, self.__class__):
            return cmp(self.delegate, other.delegate)
        return NotImplemented

    document_class              = ReadWriteDelegateProperty()
    slave_okay                  = ReadWriteDelegateProperty()
    safe                        = ReadWriteDelegateProperty()
    get_lasterror_options       = ReadWriteDelegateProperty()
    set_lasterror_options       = ReadWriteDelegateProperty()
    unset_lasterror_options     = ReadWriteDelegateProperty()
    name                        = ReadOnlyDelegateProperty()

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, repr(self.delegate))


class MotorConnectionBase(MotorBase):
    server_info                 = Async(has_safe_arg=False, cb_required=False)
    database_names              = Async(has_safe_arg=False, cb_required=False)
    copy_database               = Async(has_safe_arg=False, cb_required=True)
    close                       = ReadOnlyDelegateProperty()
    disconnect                  = ReadOnlyDelegateProperty()
    max_bson_size               = ReadOnlyDelegateProperty()
    max_pool_size               = ReadOnlyDelegateProperty()
    in_request                  = ReadOnlyDelegateProperty()
    tz_aware                    = ReadOnlyDelegateProperty()

    def __init__(self, *args, **kwargs):
        # Store args and kwargs for when open() is called
        # TODO: document io_loop kw arg
        if 'auto_start_request' in kwargs:
            raise pymongo.errors.ConfigurationError(
                "Motor doesn't support auto_start_request, use "
                "%s.start_request explicitly" % self.__class__.__name__)

        # TODO: refactor into check_ioloop(kwargs)
        if 'io_loop' in kwargs:
            kwargs = kwargs.copy()
            self.io_loop = kwargs.pop('io_loop')
            if not isinstance(self.io_loop, ioloop.IOLoop):
                raise TypeError(
                    "io_loop must be instance of IOLoop, not %s" % (
                        repr(self.io_loop)))
        else:
            self.io_loop = ioloop.IOLoop.instance()

        self._init_args = args
        self._init_kwargs = kwargs
        self.delegate = None

    def open(self, callback):
        """
        Actually connect, passing self to a callback when connected.
        @param callback: Optional function taking parameters (connection, error)
        """
        check_callable(callback)

        if self.connected:
            # TODO: test this branch, with and without callback
            if callback:
                self.io_loop.add_callback(
                    functools.partial(callback, self, None))
            return

        # Actually connect on a child greenlet
        # TODO: move self._connect back into this method
        connect = functools.partial(self._connect, callback, self.io_loop)
        greenlet.greenlet(connect).switch()

    def open_sync(self):
        """Synchronous open(), returning self
        """
        if self.connected:
            return self

        # Run a private IOLoop until connected or error
        old_loop, self.io_loop = self.io_loop, ioloop.IOLoop()
        try:

            outcome = {}
            def callback(connection, error):
                outcome['error'] = error
                self.io_loop.stop()

            self.open(callback)
            self.io_loop.start()

            # callback has been executed and loop stopped
            # TODO: make sure these are deleted
    #        del self._init_args
    #        del self._init_kwargs

        finally:
            # Replace the private IOLoop with the default loop
            self.io_loop = old_loop
            self.delegate.pool_class = functools.partial(MotorPool, self.io_loop)
            for pool in self._get_pools():
                pool.io_loop = self.io_loop
                pool.reset()

        if outcome['error']:
            raise outcome['error']

        return self

    def get_io_loop(self):
        return self.io_loop

    def __getattr__(self, name):
        if not self.connected:
            msg = "Can't access database on %s before calling open()" \
                  " or open_sync()" % (
                self.__class__.__name__
            )
            raise pymongo.errors.InvalidOperation(msg)

        return MotorDatabase(self, name)

    __getitem__ = __getattr__

    def start_request(self):
        """Assigns a socket to the current Tornado StackContext. There is no
           `end_request`: a request ends when there are no more pending
           callbacks wrapped in this request's StackContext.

           Unlike a regular pymongo Connection, start_request() on a
           MotorConnection *must* be used as a context manager:
        >>> connection = TorndadoConnection()
        >>> db = connection.test
        >>> def on_error(result, error):
        ...     print 'getLastError returned:', result
        ...
        >>> with connection.start_request():
        ...     # unsafe inserts, second one violates unique index on _id
        ...     db.collection.insert({'_id': 1}, callback=next_step)
        >>> def next_step(result, error):
        ...     db.collection.insert({'_id': 1})
        ...     # call getLastError. Because we're in a request, error() uses
        ...     # same socket as insert, and gets the error message from
        ...     # last insert.
        ...     db.error(callback=on_error)
        """
        # TODO: this is so spaghetti & implicit & magic
        global current_request, current_request_seq

        # TODO: overflow?
        current_request_seq += 1
        current_request = current_request_seq
        self.delegate.start_request()
        return RequestContext(self._get_pools(), current_request)

    def end_request(self):
        raise NotImplementedError(
            "Motor does not support end_request(). See documentation for"
            " MotorConnectionBase.start_request()."
        )

    # TODO: doc why we need to override this
    def drop_database(self, name_or_database, callback):
        if isinstance(name_or_database, MotorDatabase):
            name_or_database = name_or_database.delegate.name

        async_method = asynchronize(
            self.io_loop, self.delegate.drop_database, False, True)
        async_method(name_or_database, callback=callback)

    @property
    def connected(self):
        return self.delegate is not None

    def _connect(self, callback, loop):
        # Run on child greenlet
        # TODO: can this use asynchronize()?

        check_callable(callback, False)
        error = None
        try:
            kw = self._init_kwargs
            kw['auto_start_request'] = False
            kw['_pool_class'] = functools.partial(MotorPool, loop)
            self.delegate = self._new_delegate(*self._init_args, **kw)

            del self._init_args
            del self._init_kwargs
        except Exception, e:
            error = e

        if callback:
            # Schedule callback to be executed on main greenlet, with
            # (self, None) if no error, else (None, error)
            loop.add_callback(
                functools.partial(
                    callback, None if error else self, error))


class MotorConnection(MotorConnectionBase):
    # TODO: auto-gen Sphinx documentation that pulls from PyMongo for all these
    close_cursor                = Async(has_safe_arg=False, cb_required=True)
    kill_cursors                = Async(has_safe_arg=False, cb_required=True)
    fsync                       = Async(has_safe_arg=False, cb_required=False)
    unlock                      = Async(has_safe_arg=False, cb_required=False)
    nodes                       = ReadOnlyDelegateProperty()
    host                        = ReadOnlyDelegateProperty()
    port                        = ReadOnlyDelegateProperty()

    # TODO: test directly, doc this difference b/w PyMongo and Motor
    def is_locked(self, callback):
        def is_locked(result, error):
            if error:
                callback(None, error)
            else:
                callback(result.get('fsyncLock', 0), None)

        self.admin.current_op(callback=is_locked)

    def _new_delegate(self, *args, **kwargs):
        return pymongo.connection.Connection(*args, **kwargs)

    def _get_pools(self):
        # TODO: expose the PyMongo pool, or otherwise avoid this
        return [self.delegate._Connection__pool]

class MotorReplicaSetConnection(MotorConnectionBase):
    primary                       = ReadOnlyDelegateProperty()
    secondaries                   = ReadOnlyDelegateProperty()
    arbiters                      = ReadOnlyDelegateProperty()
    hosts                         = ReadOnlyDelegateProperty()
    read_preference               = ReadOnlyDelegateProperty()
    seeds                         = ReadOnlyDelegateProperty()

    def _new_delegate(self, *args, **kwargs):
        return pymongo.replica_set_connection.ReplicaSetConnection(
            *args, **kwargs)

    def _get_pools(self):
        # TODO: expose the PyMongo pools, or otherwise avoid this
        pools = []
        for mongo in self.delegate._ReplicaSetConnection__pools.values():
            if 'pool' in mongo:
                pools.append(mongo['pool'])

        return pools

# TODO: doc that this can take MotorConnections or PyMongo Connections
# TODO: verify & test this w/ configurable IOLoop
class MotorMasterSlaveConnection(MotorConnectionBase):
    close_cursor = ReadOnlyDelegateProperty()

    def _new_delegate(self, master, slaves, *args, **kwargs):
        # For master connection and each slave connection, if they're
        # MotorConnections unwrap them before passing to PyMongo
        # MasterSlaveConnection
        if isinstance(master, MotorConnection):
            master = master.delegate

        slaves = [s.delegate if isinstance(s, MotorConnection) else s
            for s in slaves]

        return pymongo.master_slave_connection.MasterSlaveConnection(
            master, slaves, *args, **kwargs)

    @property
    def master(self):
        motor_master = MotorConnection()
        motor_master.delegate = self.delegate.master
        motor_master.io_loop = self.io_loop
        return motor_master

    @property
    def slaves(self):
        motor_slaves = []
        for slave in self.delegate.slaves:
            motor_slave = MotorConnection()
            motor_slave.delegate = slave
            motor_slave.io_loop = self.io_loop
            motor_slaves.append(motor_slave)

        return motor_slaves

    def _get_pools(self):
        # TODO: expose the PyMongo pool, or otherwise avoid this
        return [self.master._Connection__pool]


class RequestContext(stack_context.StackContext):
    def __init__(self, pools, request_id):
        class RequestContextFactoryFactory(object):
            def __del__(self):
                global current_request
                assert current_request is None, (
                    "Expected current_request to be None when Request deleted")

                current_request = request_id
                for pool in pools:
                    pool.end_request()

                current_request = None

            def __call__(self):
                class RequestContextFactory(object):
                    def __enter__(self):
                        global current_request
                        current_request = request_id

                    def __exit__(self, type, value, traceback):
                        global current_request
                        assert current_request == request_id, (
                            "request_id %s does not match expected %s" % (
                                current_request, request_id
                            )
                        )
                        current_request = None
                        if value:
                            raise value

                return RequestContextFactory()

        super(RequestContext, self).__init__(RequestContextFactoryFactory())


class MotorDatabase(MotorBase):
    # list of overridden async operations on a MotorDatabase instance
    set_profiling_level           = Async(has_safe_arg=False, cb_required=False)
    reset_error_history           = Async(has_safe_arg=False, cb_required=False)
    add_user                      = Async(has_safe_arg=False, cb_required=False)
    remove_user                   = Async(has_safe_arg=False, cb_required=False)
    authenticate                  = Async(has_safe_arg=False, cb_required=False)
    logout                        = Async(has_safe_arg=False, cb_required=False)
    command                       = Async(has_safe_arg=False, cb_required=False)

    collection_names              = Async(has_safe_arg=False, cb_required=True)
    current_op                    = Async(has_safe_arg=False, cb_required=True)
    profiling_level               = Async(has_safe_arg=False, cb_required=True)
    profiling_info                = Async(has_safe_arg=False, cb_required=True)
    error                         = Async(has_safe_arg=False, cb_required=True)
    last_status                   = Async(has_safe_arg=False, cb_required=True)
    previous_error                = Async(has_safe_arg=False, cb_required=True)
    dereference                   = Async(has_safe_arg=False, cb_required=True)
    eval                          = Async(has_safe_arg=False, cb_required=True)

    # TODO: remove system_js?
    system_js                     = ReadOnlyDelegateProperty()
    incoming_manipulators         = ReadOnlyDelegateProperty()
    incoming_copying_manipulators = ReadOnlyDelegateProperty()
    outgoing_manipulators         = ReadOnlyDelegateProperty()
    outgoing_copying_manipulators = ReadOnlyDelegateProperty()

    def __init__(self, connection, name, *args, **kwargs):
        # *args and **kwargs are not currently supported by pymongo Database,
        # but it doesn't cost us anything to include them and future-proof
        # this method.
        if not isinstance(connection, MotorConnectionBase):
            raise TypeError("First argument to MotorDatabase must be "
                            "MotorConnectionBase, not %s" % repr(connection))

        self.connection = connection
        self.delegate = pymongo.database.Database(
            connection.delegate, name, *args, **kwargs
        )

    def __getattr__(self, name):
        return MotorCollection(self, name)

    __getitem__ = __getattr__

    # TODO: doc why we need to override this, refactor
    def drop_collection(self, name_or_collection, callback):
        name = name_or_collection
        if isinstance(name, MotorCollection):
            name = name.delegate.name

        sync_method = self.delegate.drop_collection
        async_method = asynchronize(
            self.get_io_loop(), sync_method, False, False)
        async_method(name, callback=callback)

    # TODO: doc why we need to override this, refactor
    def validate_collection(self, name_or_collection, *args, **kwargs):
        callback = kwargs.get('callback')
        check_callable(callback, required=True)

        name = name_or_collection
        if isinstance(name, MotorCollection):
            name = name.delegate.name

        sync_method = self.delegate.validate_collection
        async_method = asynchronize(self.get_io_loop(), sync_method, False, True)
        async_method(name, callback=callback)

    # TODO: test that this raises an error if collection exists in Motor, and
    # test creating capped coll
    def create_collection(self, name, *args, **kwargs):
        # We need to override create_collection specially, rather than simply
        # include it in async_ops, because we have to wrap the Collection it
        # returns in a MotorCollection.
        callback = kwargs.get('callback')
        check_callable(callback, required=False)
        if 'callback' in kwargs:
            del kwargs['callback']

        sync_method = self.delegate.create_collection
        async_method = asynchronize(self.get_io_loop(), sync_method, False, False)

        def cb(collection, error):
            if isinstance(collection, pymongo.collection.Collection):
                collection = MotorCollection(self, name)

            callback(collection, error)

        async_method(name, *args, callback=cb, **kwargs)

    # TODO: doc why we need to override this
    def add_son_manipulator(self, manipulator):
        if isinstance(manipulator, pymongo.son_manipulator.AutoReference):
            db = manipulator.database
            if isinstance(db, MotorDatabase):
                manipulator.database = db.delegate

        self.delegate.add_son_manipulator(manipulator)

    def get_io_loop(self):
        return self.connection.get_io_loop()


class MotorCollection(MotorBase):
    create_index            = Async(has_safe_arg=False, cb_required=False)
    drop_indexes            = Async(has_safe_arg=False, cb_required=False)
    drop_index              = Async(has_safe_arg=False, cb_required=False)
    drop                    = Async(has_safe_arg=False, cb_required=False)
    ensure_index            = Async(has_safe_arg=False, cb_required=False)
    reindex                 = Async(has_safe_arg=False, cb_required=False)
    rename                  = Async(has_safe_arg=False, cb_required=False)
    find_and_modify         = Async(has_safe_arg=False, cb_required=False)

    update                  = Async(has_safe_arg=True, cb_required=False)
    insert                  = Async(has_safe_arg=True, cb_required=False)
    remove                  = Async(has_safe_arg=True, cb_required=False)
    save                    = Async(has_safe_arg=True, cb_required=False)

    index_information       = Async(has_safe_arg=False, cb_required=True)
    count                   = Async(has_safe_arg=False, cb_required=True)
    options                 = Async(has_safe_arg=False, cb_required=True)
    group                   = Async(has_safe_arg=False, cb_required=True)
    distinct                = Async(has_safe_arg=False, cb_required=True)
    inline_map_reduce       = Async(has_safe_arg=False, cb_required=True)
    find_one                = Async(has_safe_arg=False, cb_required=True)

    uuid_subtype            = ReadWriteDelegateProperty()

    def __init__(self, database, name, *args, **kwargs):
        if not isinstance(database, MotorDatabase):
            raise TypeError("First argument to MotorCollection must be "
                            "MotorDatabase, not %s" % repr(database))

        self.database = database
        self.delegate = pymongo.collection.Collection(
            self.database.delegate, name)

    def __getattr__(self, name):
        # dotted collection name, like foo.bar
        return MotorCollection(
            self.database,
            self.name + '.' + name
        )

    def find(self, *args, **kwargs):
        """
        Get a MotorCursor.
        """
        if 'callback' in kwargs:
            # TODO: is ConfigurationError the right exception? Not
            #   OperationFailure?
            raise pymongo.errors.ConfigurationError(
                "Pass a callback to next, each, to_list, count, or tail, not"
                " to find"
            )

        cursor = self.delegate.find(*args, **kwargs)
        return MotorCursor(cursor, self)

    def map_reduce(self, *args, **kwargs):
        # We need to override map_reduce specially because we have to wrap the
        # Collection it returns in a MotorCollection.
        callback = kwargs.get('callback')
        check_callable(callback, required=False)
        if 'callback' in kwargs:
            kwargs = kwargs.copy()
            del kwargs['callback']

        def map_reduce_callback(result, error):
            if isinstance(result, pymongo.collection.Collection):
                result = self.database[result.name]
            callback(result, error)

        sync_method = self.delegate.map_reduce
        async_mr = asynchronize(self.get_io_loop(), sync_method, False, True)
        async_mr(*args, callback=map_reduce_callback, **kwargs)

    def get_io_loop(self):
        return self.database.get_io_loop()


class MotorCursorChainingMethod(DelegateProperty):
    def __get__(self, obj, objtype):
        # self.name is set by MotorMeta
        method = getattr(obj.delegate, self.name)

        @functools.wraps(method)
        def return_clone(*args, **kwargs):
            method(*args, **kwargs)
            return obj

        return return_clone


class MotorCursor(MotorBase):
    # TODO: test all these in test_motor_cursor.py
    count                       = Async(has_safe_arg=False, cb_required=True)
    distinct                    = Async(has_safe_arg=False, cb_required=True)
    explain                     = Async(has_safe_arg=False, cb_required=True)
    next                        = Async(has_safe_arg=False, cb_required=True)
    __exit__                    = Async(has_safe_arg=False, cb_required=True)

    slave_okay                  = ReadOnlyDelegateProperty()
    alive                       = ReadOnlyDelegateProperty()
    cursor_id                   = ReadOnlyDelegateProperty()

    batch_size                  = MotorCursorChainingMethod()
    add_option                  = MotorCursorChainingMethod()
    remove_option               = MotorCursorChainingMethod()
    limit                       = MotorCursorChainingMethod()
    skip                        = MotorCursorChainingMethod()
    max_scan                    = MotorCursorChainingMethod()
    sort                        = MotorCursorChainingMethod()
    hint                        = MotorCursorChainingMethod()
    where                       = MotorCursorChainingMethod()
    __enter__                   = MotorCursorChainingMethod()

    def __init__(self, cursor, collection):
        """
        @param cursor:      Synchronous pymongo Cursor
        @param collection:  MotorCollection
        """
        if not isinstance(cursor, pymongo.cursor.Cursor):
            raise TypeError(
                "cursor must be instance of pymongo.cursor.Cursor, not %s" % (
                repr(cursor)))

        if not isinstance(collection, MotorCollection):
            raise TypeError(
                "collection must be instance of MotorCollection, not %s" % (
                repr(collection)))

        self.delegate = cursor
        self.collection = collection
        self.started = False

    def _get_more(self, callback):
        """
        Get a batch of data asynchronously, either performing an initial query
        or getting more data from an existing cursor.
        @param callback:    function taking parameters (batch_size, error)
        """
        if self.started and not self.alive:
            raise pymongo.errors.InvalidOperation(
                "Can't call get_more() on a MotorCursor that has been"
                " exhausted or killed."
            )

        self.started = True
        async_refresh = asynchronize(
            self.get_io_loop(), self.delegate._refresh, False, True)
        async_refresh(callback=callback)

    def next(self, callback):
        """Asynchronously retrieve the next document in the result set,
        fetching a batch of results from the server if necessary.

        # TODO: note that you should close() cursor if you cancel iteration
        # TODO: prevent concurrent uses of this cursor, as IOStream does, and
        #   document that and how to avoid it.

        @param callback: function taking (document, error)
        """
        check_callable(callback, required=True)
        add_callback = self.get_io_loop().add_callback

        # TODO: simplify, review, comment
        if self.buffer_size > 0:
            try:
                doc = self.delegate.next()
            except StopIteration:
                # Special case: limit is 0.
                # TODO: verify limit 0 is tested.
                add_callback(functools.partial(callback, None, None))

                # TODO: shouldn't this just be self.delegate.close(), not
                # add_callback(self.close)?
                add_callback(self.close)
                return

            add_callback(functools.partial(callback, doc, None))
        elif self.alive and (self.cursor_id or not self.started):
            def got_more(batch_size, error):
                if error:
                    callback(None, error)
                else:
                    self.next(callback)

            self._get_more(got_more)
        else:
            # Complete
            add_callback(functools.partial(callback, None, None))

    def each(self, callback):
        """Iterates over all the documents for this cursor. Return False from
        the callback to stop iteration. each returns immediately, and your
        callback is executed asynchronously for each document. callback is
        passed (None, None) when iteration is complete.

        # TODO: note that you should close() cursor if you cancel iteration

        @param callback: function taking (document, error)
        """
        check_callable(callback, required=True)
        add_callback = self.get_io_loop().add_callback

        def next_callback(doc, error):
            if error:
                callback(None, error)
            elif doc:
                # Quit if callback returns exactly False (not None)
                if callback(doc, None) is not False:
                    add_callback(functools.partial(self.each, callback))
            else:
                # Complete
                add_callback(functools.partial(callback, None, None))

        self.next(next_callback)

    def to_list(self, callback):
        """Get a list of documents. The caller is responsible for making sure
        that there is enough memory to store the results -- it is strongly
        recommended you use a limit like:

        >>> collection.find().limit(some_number).to_list(callback)

        to_list returns immediately, and your callback is executed
        asynchronously with the list of documents.

        @param callback: function taking (documents, error)
        """
        # TODO: error if tailable
        # TODO: significant optimization if we reimplement this without each()?
        check_callable(callback, required=True)
        the_list = []

        def for_each(doc, error):
            if error:
                callback(None, error)
            elif doc is not None:
                the_list.append(doc)
            else:
                # Iteration complete
                callback(the_list, None)

        self.each(for_each)

    def clone(self):
        return MotorCursor(self.delegate.clone(), self.collection)

    def close(self):
        """Explicitly close this cursor.
        """
        # TODO: either use asynchronize() or explain why this works
        # TODO: test and document a technique of calling close instead of
        #   returning False from callback in order to cancel iteration
        greenlet.greenlet(self.delegate.close).switch()

    def rewind(self):
        # TODO: test, doc -- this seems a little extra weird w/ Motor
        self.delegate.rewind()
        self.started = False
        return self

    def tail(self, callback, await_data=None):
        # TODO: doc, prominently =)
        # TODO: doc that tailing an empty collection is expensive
        # TODO: test dropping a collection while tailing it
        # TODO: test tailing collection that isn't empty at first
        check_callable(callback, True)

        loop = self.get_io_loop()
        add_callback = loop.add_callback
        add_timeout = loop.add_timeout

        cursor = self.clone()

        # This is a list so we can modify it from the inner callback
        started = [False]

        # TODO: HACK!
        cursor.delegate._Cursor__tailable = True

        # If await_data parameter is set, then override whatever await_data
        # value was passed to find() (default False)
        # TODO: reconsider or at least test this crazy logic, doc
        if await_data is not None:
            # TODO: HACK!
            cursor.delegate._Cursor__await_data = await_data

        def inner_callback(result, error):
            if error:
                cursor.close()
                callback(None, error)
            elif result is not None:
                started[0] = True
                if callback(result, None) is False:
                    cursor.close()
                    return False
            elif cursor.alive:
                # result and error are both none, meaning no new data in
                # this batch; keep on truckin'
                add_callback(
                    functools.partial(cursor.each, inner_callback)
                )
            else:
                # cursor died, start over soon, but only if it's because this
                # collection was empty when we began.
                if not started[0]:
                    add_timeout(
                        time.time() + 0.5,
                        functools.partial(cursor.tail, callback, await_data)
                    )
                else:
                    # TODO: why exactly would this happen?
                    exc = pymongo.errors.OperationFailure("cursor died")
                    add_callback(functools.partial(callback, None, exc))

        # Start tailing
        cursor.each(inner_callback)

    def get_io_loop(self):
        return self.collection.get_io_loop()

    @property
    def buffer_size(self):
        # TODO: expose so we don't have to use double-underscore hack
        return len(self.delegate._Cursor__data)

    def __getitem__(self, index):
        # TODO test that this raises TypeError if index is not slice, int, long
        # TODO doc that this does not raise IndexError if index > len results
        # TODO test that this raises IndexError if index < 0
        # TODO: doctest
        # TODO: test this is an error if tailable
        if isinstance(index, slice):
             return MotorCursor(self.delegate[index], self.collection)
        else:
            if not isinstance(index, (int, long)):
                raise TypeError("index %r cannot be applied to MotorCursor "
                                "instances" % index)
            # Get one document, force hard limit of 1 so server closes cursor
            # immediately
            return self[self.delegate._Cursor__skip+index:].limit(-1)

    def __del__(self):
        if self.alive and self.cursor_id:
            self.close()


# TODO: test, doc
class Op(gen.Task):
    def __init__(self, func, *args, **kwargs):
        check_callable(func, True)
        super(Op, self).__init__(func, *args, **kwargs)

    def get_result(self):
        (result, error), _ = super(Op, self).get_result()
        if error:
            raise error
        return result


class WaitOp(gen.Wait):
    def get_result(self):
        (result, error), _ = super(WaitOp, self).get_result()
        if error:
            raise error

        return result


class WaitAllOps(gen.WaitAll):
    def get_result(self):
        super_results = super(WaitAllOps, self).get_result()

        results = []
        for (result, error), _ in super_results:
            if error:
                raise error
            else:
                results.append(result)

        return results
