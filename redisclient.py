#!/usr/bin/env python
#
# Copyright 2009 Phus Lu
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""Blocking and non-blocking Redis client implementations using IOStream."""

import collections
import cStringIO
import logging
import socket

from tornado.ioloop import IOLoop
from tornado.iostream import IOStream
from tornado.util import bytes_type

def encode(request):
    """Encode request (command, *args) to redis bulk bytes.

    Note that command is a string defined by redis.
    All elements in args should be a string.
    """
    assert isinstance(request, tuple)
    data = '*%d\r\n' % len(request) + ''.join(['$%d\r\n%s\r\n' % (len(str(x)), x) for x in request])
    return data

def decode(data):
    """Decode redis bulk bytes to python object."""
    assert isinstance(data, bytes_type)
    iodata = cStringIO.StringIO(data)
    c = iodata.read(1)
    if c == '+':
        return iodata.readline()[:-2]
    elif c == '-':
        raise RedisError(iodata.readline().rstrip(), data)
    elif c == ':':
        return int(iodata.readline())
    elif c == '$':
        number = int(iodata.readline())
        if number == -1:
            return None
        else:
            data = iodata.read(number)
            #iodata.read(2)
            return data
    elif c == '*':
        number = int(iodata.readline())
        if number == -1:
            return None
        else:
            result = []
            while number:
                c = iodata.read(1)
                if c == '$':
                    length  = int(iodata.readline())
                    element = iodata.read(length)
                    iodata.read(2)
                    result.append(element)
                else:
                    if c == ':':
                        element = int(iodata.readline())
                    else:
                        element = iodata.readline()[:-2]
                    result.append(element)
                number -= 1
            return result
    else:
        raise RedisError('bulk cannot startswith %r' % c, data)

class AsyncRedisClient(object):
    """An non-blocking Redis client.

    Example usage::

        import ioloop

        def handle_request(result):
            print 'Redis reply: %r' % result
            ioloop.IOLoop.instance().stop()

        redis_client = AsyncRedisClient(('127.0.0.1', 6379))
        redis_client.fetch(('set', 'foo', 'bar'), None)
        redis_client.fetch(('get', 'foo'), handle_request)
        ioloop.IOLoop.instance().start()

    This class implements a Redis client on top of Tornado's IOStreams.
    It does not currently implement all applicable parts of the Redis
    specification, but it does enough to work with major redis server APIs
    (mostly tested against the LIST/HASH/PUBSUB API so far).

    This class has not been tested extensively in production and
    should be considered somewhat experimental as of the release of
    tornado 1.2.  It is intended to become the default tornado
    AsyncRedisClient implementation.
    """

    def __init__(self, address, io_loop=None):
        """Creates a AsyncRedisClient.

        address is the tuple of redis server address that can be connect by
        IOStream. It can be to ('127.0.0.1', 6379).
        """
        self.address         = address
        self.io_loop         = io_loop or IOLoop.instance()
        self._callback_queue = collections.deque()
        self._callback       = None
        self._result_queue   = collections.deque()
        self.socket          = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.stream          = IOStream(self.socket, self.io_loop)
        self.stream.connect(self.address, self._wait_result)

    def close(self):
        """Destroys this redis client, freeing any file descriptors used.
        Not needed in normal use, but may be helpful in unittests that
        create and destroy redis clients.  No other methods may be called
        on the AsyncRedisClient after close().
        """
        self.stream.close()

    def fetch(self, request, callback):
        """Executes a request, calling callback with an redis `result`.

        The request shuold be a string tuple. like ('set', 'foo', 'bar')

        If an error occurs during the fetch, a `RedisError` exception will
        throw out. You can use try...except to catch the exception (if any)
        in the callback.
        """
        self._callback_queue.append(callback)
        self.stream.write(encode(request))

    def _wait_result(self):
        """Read a completed result data from the redis server."""
        self.stream.read_until('\r\n', self._on_read_first_line)

    def _maybe_callback(self):
        """Try call callback in _callback_queue when we read a redis result."""
        try:
            data           = self._data
            callback       = self._callback
            result_queue   = self._result_queue
            callback_queue = self._callback_queue
            if result_queue:
                result_queue.append(data)
                data = result_queue.popleft()
            if callback_queue:
                callback = self._callback = callback_queue.popleft()
            if callback:
                callback(decode(data))
        except Exception:
            logging.error('Uncaught callback exception', exc_info=True)
            self.close()
            raise
        finally:
            self._wait_result()

    def _on_read_first_line(self, data):
        self._data = data
        c = data[0]
        if c in ':+-':
            self._maybe_callback()
        elif c == '$':
            if data[:3] == '$-1':
                self._maybe_callback()
            else:
                length = int(data[1:])
                self.stream.read_bytes(length+2, self._on_read_bulk_body)
        elif c == '*':
            if data[1] in '-0' :
                self._maybe_callback()
            else:
                self._multibulk_number = int(data[1:])
                self.stream.read_until('\r\n', self._on_read_multibulk_bulk_head)

    def _on_read_bulk_body(self, data):
        self._data += data
        self._maybe_callback()

    def _on_read_multibulk_bulk_head(self, data):
        self._data += data
        c = data[0]
        if c == '$':
            length = int(data[1:])
            self.stream.read_bytes(length+2, self._on_read_multibulk_bulk_body)
        else:
            self._maybe_callback()

    def _on_read_multibulk_bulk_body(self, data):
        self._data += data
        self._multibulk_number -= 1
        if self._multibulk_number:
            self.stream.read_until('\r\n', self._on_read_multibulk_bulk_head)
        else:
            self._maybe_callback()

class RedisError(Exception):
    """Exception thrown for an unsuccessful Redis request.

    Attributes:

    data - Redis error data error code, e.g. -(ERR).
    """
    def __init__(self, message, data=None):
        self.data = data
        Exception.__init__(self, 'Redis Error: %s' % message)


def test():
    def handle_request(result):
        print 'Redis reply: %r' % result
        IOLoop.instance().stop()
    redis_client = AsyncRedisClient(('127.0.0.1', 6379))
    redis_client.fetch(('set','foo','bar'), handle_request)
    IOLoop.instance().start()

if __name__ == '__main__':
    test()
