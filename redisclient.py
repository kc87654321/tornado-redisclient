"""non-blocking Redis client interfaces."""

import collections
import socket

from tornado.ioloop import IOLoop
from tornado.iostream import IOStream
from tornado.util import bytes_type

class RedisPError(Exception):
    def __init__(self, message=None):
        message = message or 'Unknown'
        Exception.__init__(self, 'Redis %s' % message)

def encode(request):
    '''print repr(encode(('SET', 'mykey', 123)))'''
    assert type(request) is tuple
    data = '*%d\r\n' % len(request) + ''.join(['$%d\r\n%s\r\n' % (len(str(x)), x) for x in request])
    return data

def decode(data):
    '''print decode('*4\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$5\r\nhello\r\n$5\r\nworld\r\n')'''
    assert type(data) is bytes_type
    c = data[0]
    if c == '+':
        return True
    elif c == '-':
        raise RedisPError(data[1:].rstrip())
    elif c == ':':
        return int(data[1:])
    elif c == '$':
        if data[:3] == '$-1':
            return None
        else:
            pos = data.find('\r\n')
            number = int(data[1:pos])
            return data[pos+2:pos+2+number]
    elif c == '*':
        if data[:3] == '*-1':
            return None
        else:
            result = []
            pos = data.find('\r\n')
            number = int(data[1:pos])
            pos1 = pos2 = pos + 2
            while number:
                pos2    = data.find('\r\n', pos1)
                length  = int(data[pos1+1:pos2])
                element = data[pos2+2:pos2+2+length]
                pos1    = pos2 + length + 4
                result.append(element)
                number -= 1
            return result
    else:
        raise RedisPError('Unknown Redis bulk startswith %r', c)

class AsyncRedisClient(object):
    '''http://ordinary.iteye.com/blog/1097456'''

    def __init__(self, address, io_loop=None):
        self.address        = address
        self.io_loop        = io_loop or IOLoop.instance()
        self._command_queue = collections.deque()
        self.socket         = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.stream         = IOStream(self.socket)
        self.stream.connect(self.address, self._process_command)

    def fetch(self, request, callback):
        self._command_queue.append((request, callback))

    def _process_command(self):
        if self._command_queue:
            self._request, self._callback = self._command_queue.popleft()
            data = encode(self._request)
            self.stream.write(data, self._on_write)

    def _execute_callback(self):
        result = decode(self._data)
        self._callback(result)
        if self._command_queue:
            self._process_command()

    def _on_write(self):
        self._data = bytes_type()
        self.stream.read_until(bytes_type('\r\n'), self._on_read_first_line)

    def _on_read_first_line(self, data):
        self._data = data
        c = data[0]
        if c in '+-:':
            self._execute_callback()
        elif c == '$':
            if data[:3] == '$-1':
                self._execute_callback()
            else:
                length = int(data[1:])
                self.stream.read_bytes(length+2, self._on_read_bulk_line)
        elif c == '*':
            if data[1] in '-0' :
                self._execute_callback()
            else:
                self._multibulk_number = int(data[1:])
                self.stream.read_until('\r\n', self._on_read_multibulk_linehead)

    def _on_read_bulk_line(self, data):
        self._data += data
        self._execute_callback()

    def _on_read_multibulk_linehead(self, data):
        self._data += data
        length = int(data[1:])
        self.stream.read_bytes(length+2, self.__on_read_multibulk_linebody)

    def __on_read_multibulk_linebody(self, data):
        self._data += data
        self._multibulk_number -= 1
        if self._multibulk_number:
            self.stream.read_until('\r\n', self._on_read_multibulk_linehead)
        else:
            self._execute_callback()

    def close(self):
        self.stream_set.add(self.stream)
        self.stream = None
        self.stream_set = None

def main():
    def handle_result(result):
        print 'Redis reply: %r' % result
    redis_client = AsyncRedisClient(('127.0.0.1', 6379))
    redis_client.fetch(('LPUSH', 'l', 1), handle_result)
    redis_client.fetch(('LPUSH', 'l', 2), handle_result)
    redis_client.fetch(('LRANGE', 'l', 0, -1), handle_result)
    IOLoop.instance().start()

if __name__ == "__main__":
    main()
