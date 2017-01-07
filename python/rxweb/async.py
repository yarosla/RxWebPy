# coding=utf-8
import asyncio
import logging

from rx import AnonymousObserver
from rx import Observer
from rx.concurrency import AsyncIOScheduler
from rx.core import Disposable
from rx.core import ObservableBase
from rx.disposables import SingleAssignmentDisposable, CompositeDisposable
from rx.disposables.anonymousdisposable import AnonymousDisposable
from rx.subjects import Subject

from rxweb.server import Connection

logger = logging.getLogger(__name__)


class AsyncConnection(asyncio.Protocol, Connection):
    def __init__(self, on_connect):
        super().__init__()
        self.on_connect = on_connect

    def connection_made(self, transport: asyncio.Transport):
        peername = transport.get_extra_info('peername')
        logger.info('Connection from %s', peername)
        self.transport = transport
        self.data_in = Subject()
        self.data_out = AnonymousObserver(self.on_data_out_next, self.on_data_out_error, self.on_data_out_completed)
        self.on_connect(self)

    def eof_received(self):
        logger.debug('data eof received')
        self.data_in.on_completed()

    def resume_writing(self):
        logger.debug('data resume')

    def pause_writing(self):
        logger.debug('data pause')

    def connection_lost(self, exc):
        logger.debug('data connection lost')
        self.data_in.on_error(exc)

    def data_received(self, data):
        logger.debug('data received: %s', data)
        self.data_in.on_next(data)

    def on_data_out_next(self, data):
        logger.debug('sending: %s', data)
        self.transport.write(data)

    def on_data_out_error(self, exception):
        logger.exception('data_out error: %r', exception)
        self.close()

    def on_data_out_completed(self):
        logger.info('data_out completed')
        self.close()

    def close(self):
        self.transport.close()


class AsyncListener(ObservableBase):
    def __init__(self, loop: asyncio.AbstractEventLoop, host: str, port: int):
        super().__init__()
        self.loop = loop
        self.host = host
        self.port = port
        self.observer = None  # type: Observer

    def _subscribe_core(self, observer: Observer):
        assert not self.observer
        self.observer = observer

        def create_connection():
            return AsyncConnection(self.observer.on_next)

        def dispose():
            logger.info('listener closed')
            self.server.close()
            self.loop.run_until_complete(self.server.wait_closed())
            self.server = None
            self.observer = None

        self.server = self.loop.create_server(create_connection, self.host, self.port)
        self.loop.create_task(self.server)
        return AnonymousDisposable.create(dispose)


class AsyncScheduler(AsyncIOScheduler):
    """A scheduler that schedules work via the asyncio mainloop."""

    # copied from RxPy with modification
    def schedule(self, action, state=None):
        """Schedules an action to be executed."""

        disposable = SingleAssignmentDisposable()

        def interval():
            disposable.disposable = self.invoke_action(action, state)

        handle = self.loop.call_soon_threadsafe(interval)  # the only modified line

        def dispose():
            handle.cancel()

        return CompositeDisposable(disposable, Disposable.create(dispose))
