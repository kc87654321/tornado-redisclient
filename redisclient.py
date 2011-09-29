"""non-blocking Redis client interfaces."""

import collections
import socket

from tornado.ioloop import IOLoop
from tornado.iostream import IOStream
from tornado.util import import_object, bytes_type

class AsyncRedisClient(object):

    stream_pool = {}

    def __init__(self, io_loop=None, address=('127.0.0.1', 6789)):
        self.io_loop = io_loop or IOLoop.instance()
        self.address = address
        if address not in self.stream_pool:
            self.stream_pool[address] = set([])
        self.stream_set = self.stream_pool[address]
        stream = None
        while self.stream_set:
            stream = self.stream_set.pop()
            if not stream.closed():
                break
        if stream is None:
            sock = socket.socket()
            stream = IOStream(sock)
            stream._connected = False
        else:
            stream._connected = True
        self.stream = stream

    def fetch(self, request, callback):
        self._request  = request
        self._callback = callback
        if not self.stream._connected:
            self.stream.connect(self.address, self._on_connect)
        else:
            self.stream.write(_request.raw, self._on_write)

    def _on_connect(self):
        self.stream._connected = True
        self.fetch(self._request, self._callback)

    def _on_write(self):
        self._data = ''
        self.stream.read_util('\r\n', self._on_read_first_line)

    def _on_read_first_line(self, data):
        bulk = data[0]
        if bulk == ':':
            self._callback(RedisResponse(data))
        elif bulk == '$':
            self._data_line_number = int(data[1:])
            self._data += data
            self.stream.read_util('\r\n', self._on_read_line)

    def _on_read_line(self, data):
        self._data_line_number -= 1
        if self._data_line_number:
            self._data += data
            self.stream.read_util('\r\n', self._on_read_line)
        else:
            self._callback(RedisResponse(self._data))

    def close(self):
        self.stream_set.add(self.stream)
        self.stream = None
        self.stream_set = None


class RedisRequest(object):

    def __init__(self, raw):
        self.raw = raw


class RedisResponse(object):

    def __init__(self, raw):
        self.raw = raw

class RedisPError(Exception):
    def __init__(self, code, message=None, response=None):
        self.code = code
        message = message or httplib.responses.get(code, "Unknown")
        self.response = response
        Exception.__init__(self, "Redis %d: %s" % (self.code, message))

def main():
    pass

if __name__ == "__main__":
    main()
