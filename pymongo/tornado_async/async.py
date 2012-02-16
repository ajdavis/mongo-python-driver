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

"""Tornado asynchronous Python driver for MongoDB.

Not implemented:
Various methods on pymongo Cursor, like explain(), count(), distinct(), hint(),
or where() on a cursor -- use arguments to find() or database commands directly
instead.
"""
import inspect
import os

import socket
import sys

import tornado.ioloop, tornado.iostream
import greenlet

from bson.binary import OLD_UUID_SUBTYPE
from bson.son import SON

import pymongo
from pymongo import helpers, greenlet_pool
from pymongo.errors import InvalidOperation


have_ssl = True
try:
    import ssl
except ImportError:
    have_ssl = False


__all__ = ['TornadoConnection', 'TornadoReplicaSetConnection']

# TODO: sphinx-formatted docstrings
# TODO: note you can't use from multithreaded app, consider special checks
# to prevent it?
# TODO: convenience method for count()? How does Node do it?
# TODO: maybe Tornado classes should inherit from pymongo classes after all

def check_callable(kallable, required=False):
    if required and not kallable:
        raise TypeError("callable is required")
    if kallable is not None and not callable(kallable):
        raise TypeError("callback must be callable")

class TornadoSocket(object):
    """
    Replace socket with a class that yields from the current greenlet, if we're
    on a child greenlet, when making blocking calls, and uses Tornado IOLoop to
    schedule child greenlet for resumption when I/O is ready.

    We only implement those socket methods actually used by pymongo: connect,
    sendall, and recv.
    """
    def __init__(self, sock, use_ssl=False):
        self.use_ssl = use_ssl
        if self.use_ssl:
           self.stream = tornado.iostream.SSLIOStream(sock)
        else:
           self.stream = tornado.iostream.IOStream(sock)

    def setsockopt(self, *args, **kwargs):
        self.stream.socket.setsockopt(*args, **kwargs)

    def settimeout(self, timeout):
        """
        Do nothing -- IOStream calls socket.setblocking(False), which does
        settimeout(0.0). We must not allow pymongo to set timeout to some other
        value (a positive number or None) or the socket will start blocking
        again.
        """
        pass

    def connect(self, pair):
        """
        @param pair: A tuple, (host, port)
        """
        child_gr = greenlet.getcurrent()
        assert child_gr.parent, "Should be on child greenlet"

        # This is run by IOLoop on the main greenlet when socket has connected;
        # switch back to child to continue processing
        def connect_callback():
            child_gr.switch()

        self.stream.connect(pair, callback=connect_callback)

        # Resume main greenlet
        child_gr.parent.switch()

    def sendall(self, data):
        child_gr = greenlet.getcurrent()
        assert child_gr.parent, "Should be on child greenlet"

        # This is run by IOLoop on the main greenlet when data has been sent;
        # switch back to child to continue processing
        def sendall_callback():
            child_gr.switch()

        self.stream.write(data, callback=sendall_callback)

        # Resume main greenlet
        child_gr.parent.switch()

    def recv(self, num_bytes):
        """
        @param num_bytes:   Number of bytes to read from socket
        @return:            Data received
        """
        child_gr = greenlet.getcurrent()
        assert child_gr.parent, "Should be on child greenlet"

        # This is run by IOLoop on the main greenlet when socket has connected;
        # switch back to child to continue processing
        def recv_callback(data):
            child_gr.switch(data)

#        print >> sys.stderr, "starting read_bytes(%d) at %d" % (
#            num_bytes, time.time()
#        )
        self.stream.read_bytes(num_bytes, callback=recv_callback)

        # Resume main greenlet, returning the data received
#        print >> sys.stderr, "recv switching to parent: %s at %d" % (
#            child_gr.parent, time.time()
#        )
        return child_gr.parent.switch()

    def close(self):
        self.stream.close()

    def __del__(self):
        self.close()


class TornadoPool(greenlet_pool.GreenletPool):
    """A simple connection pool of TornadoSockets.
    """
    def connect(self, pair):
        """Connect to Mongo and return a new connected TornadoSocket.
        """
        assert greenlet.getcurrent().parent, "Should be on child greenlet"
        self._check_pair_arg(pair)

        # Prefer IPv4. If there is demand for an option
        # to specify one or the other we can add it later.
        socket_types = (socket.AF_INET, socket.AF_INET6)
        for socket_type in socket_types:
            try:
                s = socket.socket(socket_type)
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                break
            except socket.gaierror:
                # If that fails try IPv6
                continue
        else:
            # None of the socket types worked
            raise

        tornado_sock = TornadoSocket(s, use_ssl=self.use_ssl)

        # TornadoSocket will pause the current greenlet and resume it when
        # connection has completed
        tornado_sock.connect(pair or self.pair)
        return tornado_sock


def asynchronize(sync_method, sync_attr):
    """
    @param sync_method: unbound method of a pymongo Collection, Database,
                        or Connection
    @param sync_attr:   Like 'sync_connection', the async class's wrapped member
    """
    # TODO doc
    def method(self, *args, **kwargs):
        """
        @param self:    A TornadoConnection, TornadoDatabase, or
                        TornadoCollection
        """
        assert isinstance(self, (
            TornadoConnection, TornadoDatabase, TornadoCollection
        ))

        client_callback = kwargs.get('callback')
        check_callable(client_callback)

        if 'callback' in kwargs:
            kwargs = kwargs.copy()
            del kwargs['callback']

        # TODO: find another way, or at least cache this info in case
        # getargspec is expensive.
        if 'safe' in inspect.getargspec(sync_method).args:
            kwargs['safe'] = bool(client_callback)

        def call_method():
            result, error = None, None
            try:
                sync_obj = getattr(self, sync_attr)
                result = sync_method(sync_obj, *args, **kwargs)
            except Exception, e:
                error = e

            # Schedule the callback to be run on the main greenlet
            if client_callback:
                tornado.ioloop.IOLoop.instance().add_callback(
                    lambda: client_callback(result, error)
                )

        # Start running the operation on greenlet
        greenlet.greenlet(call_method).switch()

    return method


# TODO: better name, with 'Mongo' in it!
class TornadoConnection(object):
    # list of overridden async operations on a TornadoConnection instance
    async_ops = set([
        'drop_database', 'database_names', 'close_cursor', 'kill_cursors',
        'server_info', 'database_names', 'drop_database', 'copy_database',
        'is_locked', 'fsync', 'unlock',
    ])

    def __init__(self, *args, **kwargs):
        # Store args and kwargs for when open() is called
        self._init_args = args
        self._init_kwargs = kwargs

        # The synchronous pymongo Connection
        self.sync_connection = None
        self.connected = False

    def open(self, callback):
        """
        Actually connect, passing self to a callback when connected.
        @param callback: Optional function taking parameters (connection, error)
        """
        # TODO: connect on demand? Remove open() as a public method?
        check_callable(callback)

        if self.connected:
            callback(self, None)
            return
        
        def connect():
            # Run on child greenlet
            error = None
            try:
                self.sync_connection = pymongo.Connection(
                    *self._init_args,
                    _pool_class=TornadoPool,
                    **self._init_kwargs
                )
                
                self.connected = True
            except Exception, e:
                error = e

            if callback:
                # Schedule callback to be executed on main greenlet, with
                # (self, None) if no error, else (None, error)
                tornado.ioloop.IOLoop.instance().add_callback(
                    lambda: callback(
                        None if error else self,
                        error
                    )
                )

        # Actually connect on a child greenlet
        greenlet.greenlet(connect).switch()

    def __getattr__(self, name):
        """
        Override pymongo Connection's attributes to replace blocking operations
        with async alternatives, and to get references to TornadoDatabase
        instances instead of Database.
        @param name:            Like 'drop_database', 'database_names', ...
        @return:                A proxy method that will implement the operation
                                asynchronously, and requires a 'callback' kwarg
        """
        if name not in dir(pymongo.connection.Connection):
            # We're getting a database from the connection
            if not self.connected:
                raise InvalidOperation(
                    "Can't access database on TornadoConnection before"
                    " calling open()"
                )
            return TornadoDatabase(self, name)

    __getitem__ = __getattr__

    def __repr__(self):
        return 'TornadoConnection(%s)' % (
            ','.join([
                i for i in [
                    ','.join([str(i) for i in self._init_args]),
                    ','.join(['%s=%s' for k, v in self._init_kwargs.items()]),
                ] if i
            ])
        )


# Replace synchronous methods like 'drop_database' with async versions
for op in TornadoConnection.async_ops:
    sync_method = getattr(pymongo.connection.Connection, op)
    setattr(TornadoConnection, op, asynchronize(sync_method, 'sync_connection'))


class TornadoReplicaSetConnection(object):
    def __init__(self, *args, **kwargs):
        # TODO
        pass


class TornadoDatabase(object):
    # list of overridden async operations on a TornadoDatabase instance
    async_ops = set([
        'create_collection', 'drop_collection', 'collection_names',
        'validate_collection', 'current_op', 'profiling_level',
        'set_profiling_level', 'profiling_info', 'error', 'last_status',
        'previous_error', 'reset_error_history', 'add_user', 'remove_user',
        'authenticate', 'logout', 'dereference', 'eval',
    ])

    def __init__(self, connection, name, *args, **kwargs):
        # *args and **kwargs are not currently supported by pymongo Database,
        # but it doesn't cost us anything to include them and future-proof
        # this method
        self.name = name
        if isinstance(connection, TornadoConnection):
            self.connection = connection
            self.sync_database = pymongo.database.Database(
                connection.sync_connection, name, *args, **kwargs
            )
        else:
            # TODO: should we support this?
            assert isinstance(connection, pymongo.connection.Connection)
            assert False, "Can't make TornadoDatabase from pymongo Connection"
            self.connection = TornadoConnection(connection)
            self.sync_database = pymongo.database.Database(
                connection, name, *args, **kwargs
            )

    def __getattr__(self, name):
        """
        Override pymongo Database's attributes to replace blocking operations
        with async alternatives, and to get references to TornadoCollection
        instances instead of Database.
        @param name:            Like 'drop_collection', 'collection_names', ...
        @return:                A proxy method that will implement the operation
                                asynchronously if provided a callback
        """
        if name not in dir(pymongo.database.Database):
            return TornadoCollection(self, name)
        else:
            # Just a regular attribute that doesn't use the network, e.g.
            # self.name
            return getattr(self.sync_database, name)

    # TODO: necessary? or just add 'command' to async_db_ops?
    def command(self, command, value=1,
                check=True, allowable_errors=[],
                uuid_subtype=OLD_UUID_SUBTYPE, **kwargs):
        # TODO: What semantics exactly shall we support with check and callback?
        #   Is check still necessary to support the pymongo API internally ... ?
        callback = kwargs.get('callback')
        if 'callback' in kwargs:
            del kwargs['callback']

#        if check and not callback:
#            raise InvalidOperation("Must pass a callback if check is True")

        check_callable(callback)

        if isinstance(command, basestring):
            command = SON([(command, value)])

        use_master = kwargs.pop('_use_master', True)

        fields = kwargs.get('fields')
        if fields is not None and not isinstance(fields, dict):
            kwargs['fields'] = helpers._fields_list_to_dict(fields)

        command.update(kwargs)

        def command_callback(result, error):
            # TODO: what's the diff b/w getting an error here and getting one in
            # _check_command_response?
            if error:
                if callback:
                    callback(result, error)
            elif check:
                msg = "command %s failed: %%s" % repr(command).replace("%", "%%")
                try:
                    # TODO: test if disconnect() is called correctly
                    helpers._check_command_response(result, self.connection.disconnect,
                        msg, allowable_errors)

                    # No exception thrown
                    if callback:
                        callback(result, error)
                except Exception, e:
                    if callback:
                        callback(result, e)

        self["$cmd"].find_one(command,
                              _must_use_master=use_master,
                              _is_command=True,
                              _uuid_subtype=uuid_subtype,
                              callback=command_callback)

    def __getitem__(self, name):
        """Get a database by name.

        Raises :class:`~pymongo.errors.InvalidName` if an invalid
        database name is used.

        :Parameters:
          - `name`: the name of the database to get
        """
        return self.__getattr__(name)

    def __repr__(self):
        return 'Tornado' + self.sync_database.__repr__()

    def __cmp__(self, other):
        return cmp(self.sync_database, other.sync_database)


# Replace synchronous methods like 'command' with async versions
for op in TornadoDatabase.async_ops:
    sync_method = getattr(pymongo.database.Database, op)
    setattr(TornadoDatabase, op, asynchronize(sync_method, 'sync_database'))


class TornadoCollection(object):
    # list of overridden async operations on a TornadoCollection instance
    async_ops = set([
        'update', 'insert', 'remove', 'create_index', 'index_information',
        'drop_indexes', 'drop_index', 'drop', 'count', 'ensure_index', 'reindex',
        'options', 'group', 'rename', 'distinct', 'map_reduce', 'inline_map_reduce',
        'find_and_modify',
    ])

    def __init__(self, database, name, *args, **kwargs):
        if isinstance(database, TornadoDatabase):
            self._tdb = database
            self.sync_collection = pymongo.collection.Collection(
                self._tdb.sync_database, name
            )
        else:
            assert isinstance(database, pymongo.database.Database)
            assert False, (
                "TODO: support creating TornadoCollection from pymongo"
                "Database?"
            )

            self._tdb = TornadoDatabase(
                database.connection, name, *args, **kwargs
            )

            self.sync_collection = pymongo.collection.Collection(
                database, name
            )

    def __getattr__(self, name):
        """
        Override pymongo Collection's attributes to replace the basic CRUD
        operations with async alternatives.
        # TODO: Just override them explicitly?
        @param name:            Like 'find', 'remove', 'update', ...
        @return:                A proxy method that will implement the operation
                                asynchronously if provided a callback
        """
        # Get pymongo's synchronous method for this operation
        sync_method = getattr(self.sync_collection, name)

        # TODO: cheaper way to find if something's a collection than
        # actually instantiating one? what about:
        # name in dir(self.sync_collection)
        if isinstance(sync_method, pymongo.collection.Collection):
            # dotted collection name, like foo.bar
            return TornadoCollection(
                self._tdb,
                self.name + '.' + sync_method.name
            )

        return sync_method

    # Delegate to pymongo.collection.Collection's uuid_subtype property
    def __get_uuid_subtype(self):
        return self.sync_collection.uuid_subtype

    def __set_uuid_subtype(self, subtype):
        self.sync_collection.uuid_subtype = subtype

    uuid_subtype = property(__get_uuid_subtype, __set_uuid_subtype)

    # TODO: necessary to override explicitly, or just put in
    # async_collection_ops?
    def save(self, to_save, manipulate=True, safe=False, **kwargs):
        """Save a document in this collection."""
        # TODO: need to override find and find_one distinctly, or can we just
        # add them to async_collection_ops?
        if not isinstance(to_save, dict):
            raise TypeError("cannot save object of type %s" % type(to_save))

        if "_id" not in to_save:
            return self.insert(to_save, manipulate, safe=safe, **kwargs)
        else:
            client_callback = kwargs.get('callback')
            check_callable(client_callback)
            if 'callback' in kwargs:
                kwargs = kwargs.copy()
                del kwargs['callback']

            if client_callback:
                # update() calls the callback with server's response to
                # getLastError, but we want to call it with the _id of the
                # saved document.
                def callback(result, error):
                    client_callback(
                        None if error else to_save['_id'],
                        error
                    )
            else:
                callback = None

            self.update({"_id": to_save["_id"]}, to_save, True,
                manipulate, _check_keys=True, safe=safe, callback=callback,
                **kwargs)

    def find(self, *args, **kwargs):
        """
        Run an async find(), and return a TornadoCursor, rather than returning a
        pymongo Cursor for synchronous operations.
        """
        client_callback = kwargs.get('callback')
        check_callable(client_callback, required=True)
        kwargs = kwargs.copy()
        del kwargs['callback']

        # Getting the cursor doesn't actually use a socket yet; it's get_more
        # that we have to make non-blocking
        cursor = self.sync_collection.find(*args, **kwargs)
        tornado_cursor = TornadoCursor(cursor)
        tornado_cursor.get_more(client_callback)

        # When the greenlet has sent the query on the socket, it will switch
        # back to the main greenlet, here, and we return to the caller.
        return tornado_cursor

    def find_one(self, *args, **kwargs):
        # TODO: shorten by calling pymongo Collection find_one in greenlet?
        client_callback = kwargs.get('callback')
        check_callable(client_callback, required=True)
        
        if 'callback' in kwargs:
            kwargs = kwargs.copy()
            del kwargs['callback']

        if 'limit' in kwargs:
            raise TypeError("'limit' argument not allowed for find_one")

        def find_one_callback(result, error):
            # Turn single-document list into a plain document.
            # This is run on the main greenlet.
            assert result is None or len(result) in (0, 1), (
                "Got %d results from a findOne" % len(result)
            )

            client_callback(result[0] if result else None, error)

        # TODO: python2.4-compatible?
        self.find(*args, limit=-1, callback=find_one_callback, **kwargs)

    def __repr__(self):
        return 'Tornado' + repr(self.sync_collection)

    def __cmp__(self, other):
        return cmp(self.sync_collection, other.sync_collection)

# Replace synchronous methods like 'insert' or 'drop' with async versions
for op in TornadoCollection.async_ops:
    sync_method = getattr(pymongo.collection.Collection, op)
    setattr(TornadoCollection, op, asynchronize(sync_method, 'sync_collection'))


class TornadoCursor(object):
    def __init__(self, cursor):
        """
        @param cursor:  Synchronous pymongo.Cursor
        """
        self.sync_cursor = cursor
        self.started = False

    def get_more(self, callback):
        """
        Get a batch of data asynchronously, either performing an initial query
        or getting more data from an existing cursor.
        @param callback:    Optional function taking parameters (result, error)
        """
        # TODO: shorten somehow?
        check_callable(callback)
        assert not self.sync_cursor._Cursor__killed
        if self.started and not self.alive:
            raise InvalidOperation(
                "Can't call get_more() on an TornadoCursor that has been"
                " exhausted or killed."
            )

        def _get_more():
            # This is executed on child greenlet
            result, error = None, None
            try:
                self.started = True
                self.sync_cursor._refresh()

                # TODO: Make this accessible w/o underscore hack
                result = self.sync_cursor._Cursor__data
                self.sync_cursor._Cursor__data = []
            except Exception, e:
                error = e

            # Execute callback on main greenlet
            tornado.ioloop.IOLoop.instance().add_callback(
                lambda: callback(result, error)
            )

        greenlet.greenlet(_get_more).switch()

        # When the greenlet has sent the query on the socket, it will switch
        # back to the main greenlet, here, and we return to the caller.
        return None

    @property
    def alive(self):
        """Does this cursor have the potential to return more data?"""
        return bool(
            self.sync_cursor.alive and self.sync_cursor._Cursor__id
        )

    def close(self):
        """Explicitly close this cursor.
        """
        greenlet.greenlet(self.sync_cursor.close).switch()

    def __del__(self):
        if self.alive:
            self.close()
