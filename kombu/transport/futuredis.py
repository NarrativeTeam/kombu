from __future__ import absolute_import

from contextlib import contextmanager
from datetime import timedelta
from itertools import product
from itertools import cycle

from kombu.five import Empty
from anyjson import loads
import tornadoredis
from tornado.ioloop import IOLoop
from tornado.ioloop import PeriodicCallback
from tornado import gen

from . import virtual

try:
    import redis
except ImportError:  # pragma: no cover
    redis = None     # noqa

DEFAULT_PORT = 6379
DEFAULT_DB = 0
PRIORITY_STEPS = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


class TornadoPoller(object):

    def __init__(self, pool):
        self.pool = pool

    def get(self, queue_name):
        p = TornadoSingleQueuePoller(self.pool, queue_name)
        return IOLoop.current().run_sync(p.get)


class TornadoSingleQueuePoller(object):

    def __init__(self, pool, queue_name):
        self.pool = pool
        self.q = _Q(queue_name)

    def get_client(self):
        return tornadoredis.Client(connection_pool=self.pool)

    @contextmanager
    def disconnecting_clients(self, count):
        clients = [self.get_client() for _ in range(count)]
        try:
            yield clients
        finally:
            for client in clients:
                client.disconnect()

    @contextmanager
    def disconnecting_client(self):
        client = self.get_client()
        try:
            yield client
        finally:
            client.disconnect()

    @gen.coroutine
    def get(self):
        print "get message"
        io_loop = IOLoop.current()
        with self.disconnecting_client() as c:
            print id(c)
            to = io_loop.add_timeout(timedelta(milliseconds=100),
                                     self._move_from_dedup)
            popped = yield gen.Task(c.brpop, [self.q.undeduped], 0)
            io_loop.remove_timeout(to)
            print "done get message"
            raise gen.Return(popped[self.q.undeduped])

    @gen.engine
    def _move_from_dedup(self):
        print "move from dedup"
        io_loop = IOLoop.current()
        with self.disconnecting_client() as c:
            to = io_loop.add_timeout(timedelta(milliseconds=100),
                                     self._move_from_pri)
            print id(c)
            message = yield gen.Task(c.brpoplpush, self.q.pending_undedup,
                                     self.q.undeduped)
            # TODO sp: Remove message id from dedup hash.
            print "done move from dedup", message, type(message), self.q._name
            io_loop.remove_timeout(to)
            print "removed timeout"

    @gen.engine
    def _move_from_pri(self):
        with self.disconnecting_clients(len(PRIORITY_STEPS)) as clients:
            print [id(c) for c in clients]
            yield [gen.Task(c.rpoplpush, self.q.pri(pri),
                            self.q.pending_undedup)
                   for c, pri in zip(clients, PRIORITY_STEPS)]


class TornadoCycle(object):
    "Consume from a set of resources on a first come, first served basis. "

    def __init__(self, fun, resources, predicate=Exception):
        self.fun = fun
        self.resources = resources
        self.predicate = predicate

    def get(self):
        for r in self.resources:
            m = self.fun(r)
            if m:
                break
        return m, r

    def close(self):
        pass


class _Q(object):
    __slots__ = ('_name',)

    prefix = '_kombu:queue'

    def __init__(self, name):
        assert ":" not in name
        self._name = name

    def pri(self, pri):
        return self._prefixed('pri:{}'.format(pri))

    @property
    def undeduped(self):
        return self._prefixed('undeduped')

    @property
    def pending_undedup(self):
        return self._prefixed('pending_undedup')

    def _prefixed(self, s):
        return ":".join([self.prefix, self._name, s])


class Channel(virtual.Channel):
    queues = {}
    do_restore = False
    supports_fanout = False

    socket_timeout = None

    def __init__(self, *args, **kwargs):
        super(Channel, self).__init__(*args, **kwargs)
        self.sync = self._get_sync_client()
        self.poller = self._get_async_poller()

    def _get_sync_client(self):
        params = self._connparams()
        return redis.StrictRedis(**params)

    def _get_async_poller(self):
        params = self._connparams()
        params["max_connections"] = 500
        params.pop("socket_timeout", None)
        if params.pop("password", ""):
            raise ValueError("password not supported")
        if params.pop("db", 0) != 0:
            raise ValueError("db not supported")
        return TornadoPoller(tornadoredis.ConnectionPool(**params))

    def _has_queue(self, queue, **kwargs):
        assert ':' not in queue
        cmds = self.sync.pipeline()
        for pri in PRIORITY_STEPS:
            cmds = cmds.exists(_Q(queue).pri(pri))
        return any(cmds.execute())

    def _purge(self, queue):
        q = _Q(queue)
        cmds = self.sync.pipeline()
        for pri in PRIORITY_STEPS:
            priq = q.pri(pri)
            cmds = cmds.llen(priq).delete(priq)
        sizes = cmds.execute()
        return sum(sizes[::2])

    def _size(self, queue):
        cmds = self.sync.pipeline()
        q = _Q(queue)
        for pri in PRIORITY_STEPS:
            cmds = cmds.llen(q.pri(pri))
        sizes = cmds.execute()
        return sum(size for size in sizes if isinstance(size, int))

    def _connparams(self):
        conninfo = self.connection.client
        database = conninfo.virtual_host
        if not isinstance(database, int):
            if not database or database == '/':
                database = DEFAULT_DB
            elif database.startswith('/'):
                database = database[1:]
            try:
                database = int(database)
            except ValueError:
                raise ValueError(
                    'Database name must be int between 0 and limit - 1')
        host = conninfo.hostname or '127.0.0.1'
        connparams = {'host': host,
                      'port': conninfo.port or DEFAULT_PORT,
                      'db': database,
                      'password': conninfo.password,
                      'socket_timeout': self.socket_timeout}
        if host.split('://')[0] == 'socket':
            connparams.update({
                'connection_class': redis.UnixDomainSocketConnection,
                'path': host.split('://')[1]})
            connparams.pop('host', None)
            connparams.pop('port', None)
        return connparams

    def _reset_cycle(self):
        self._cycle = TornadoCycle(self.poller.get, self._active_queues, Empty)

    def _get(self, queue):
        q = _Q(queue)
        for pri in PRIORITY_STEPS:
            item = self.sync.rpop(q.pri(pri))
            if item:
                return loads(item)
        raise Empty()


class Transport(virtual.Transport):
    Channel = Channel

    #: memory backend state is global.
    state = virtual.BrokerState()

    driver_type = 'futuredis'
    driver_name = 'futuredis'

    def driver_version(self):
        return 'N/A'
