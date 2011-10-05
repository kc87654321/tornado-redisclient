WARNING
===============
https://github.com/phus/tornado-redisclient - go there if you want working tornado-redisclient.

tornado-redisclient
========

Asynchronous [Redis](http://redis-db.com/) client that works within [Tornado](http://tornadoweb.org/) IOloop.


Usage
-----

    >>> from tornado import ioloop
    >>> from redisclient import AsyncRedisClient
    >>> def handle_request(result):
           print 'Redis reply: %r' % result
           ioloop.IOLoop.instance().stop()
    >>> redis_client = AsyncRedisClient(('127.0.0.1', 6379))
    >>> redis_client.fetch(('set', 'foo', 'bar'), None)
    >>> redis_client.fetch(('get', 'foo'), handle_request)
    >>> ioloop.IOLoop.instance().start()
    Redis reply: 'bar'


Credits
-------
tornado-redisclient is developed and maintained by [Phus Lu](mailto:phus.lu@gmail.com)

 * Inspiration: [tornado.httpclient](http://www.tornadoweb.org/documentation/httpclient.html)


License
-------
Apache License, Version 2.0

