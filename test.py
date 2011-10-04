#!/usr/bin/env python
# coding:utf-8

import sys, os, re, time

def test_async():
    from redisclient import AsyncRedisClient
    from tornado.ioloop import IOLoop
    print time.time()
    redis_client = AsyncRedisClient(('127.0.0.1', 6379))
    redis_client.fetch(('set', 'c', 0), None)
    for i in xrange(10000):
        redis_client.fetch(('incr', 'c'), None)
    redis_client.fetch(('get', 'c'), lambda r:sys.stdout.write('%s, %r' % (time.time(), r)))
    IOLoop.instance().start()

def test_sync():
    import redis
    print time.time()
    redis_client = redis.Redis('127.0.0.1', 6379)
    redis_client.set('c', '0')
    for i in xrange(10000):
        redis_client.incr('c')
    print redis_client.get('c')
    print time.time()

def main():
    test_sync()

if __name__ == '__main__':
    main()

