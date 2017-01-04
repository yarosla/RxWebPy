# coding=utf-8
import logging
import re
import time
from typing import List

import pytest
from rx import Observable
from rx.concurrency import thread_pool_scheduler
from rx.concurrency.eventloopscheduler import event_loop_scheduler
from rx.core import ObserverBase
from rx.subjects import Subject

from rxweb.server import HttpRequest, Connection, HttpResponse, HttpServer, Dispatcher, Handler

logger = logging.getLogger(__name__)


def tapper(message):
    def tap(x):
        logger.debug('[tapped] %s: %r', message, x)
        return x

    return tap


def content_loader(request: HttpRequest) -> Observable:
    """Returns Mono<HttpRequest>, single value observable"""

    def collect_content(content: bytes):
        request.content_received = content
        return request

    if request.content_in:
        logger.debug('reducing content_in %s', request.content_in)
        return request.content_in \
            .reduce(lambda acc, x: acc + x, b'') \
            .map(tapper('reduced content')) \
            .map(collect_content)
    else:
        logger.debug('no content_in')
        return Observable.just(request)


class RequestSubscriberStub(ObserverBase):
    def __init__(self):
        super().__init__()
        self.results = []
        self.completed = False
        self.error = None

    def _on_next_core(self, request: HttpRequest):
        self.results.append((request.method, request.url, request.content_received))

    def _on_completed_core(self):
        self.completed = True

    def _on_error_core(self, error):
        self.error = error


class ConnectionStub(Connection):
    def __init__(self, input: List[bytes]):
        self.output = b''  # type: bytes

        def subscriber(b: bytes):
            self.output += b

        data_out = Subject()
        data_out.subscribe(subscriber)

        super().__init__(Observable.from_(input), data_out)


def test_subscriber():
    subscriber = RequestSubscriberStub()

    Observable.just(HttpRequest('GET', '/aaa')) \
        .map(tapper('subscriber')) \
        .map(content_loader) \
        .map(tapper('handled')) \
        .merge_all() \
        .map(tapper('merged')) \
        .subscribe(subscriber)

    assert subscriber.results == [('GET', '/aaa', None)]
    assert subscriber.completed
    assert subscriber.error is None


@pytest.mark.parametrize('input', [
    [b'GET /aaa\r\nHost: aaa\r\n\r\n'],
])
def test_http_error(input):
    subscriber = RequestSubscriberStub()

    Observable \
        .from_(input) \
        .http_parse() \
        .subscribe(subscriber)

    logger.debug('results: %s', subscriber.results)
    assert subscriber.results == []
    assert isinstance(subscriber.error, HttpResponse)
    assert subscriber.error.status_code == 400


@pytest.mark.parametrize('input', [
    [b'GET /aaa HTTP/1.1\r\nHost: aaa\r\n\r\n'],
    [b'GET /aaa H', b'TTP/1.1\r\nHost: aaa\r\n\r\n'],
    [b'GET /aaa HTTP/1.1\r\n', b'Host: aaa\r\n\r\n'],
    [b'GET /aaa HTTP/1.1\r\n', b'Host: aaa\r\n\r', b'\n'],
    [b'GET /aaa HTTP/1.1\r\nHost: aaa\r\n\r\n12345'],
])
def test_http_get_1(input):
    subscriber = RequestSubscriberStub()

    Observable \
        .from_(input) \
        .http_parse() \
        .map(tapper('parsed')) \
        .flat_map(content_loader) \
        .map(tapper('merged')) \
        .subscribe(subscriber)

    logger.debug('results: %s', subscriber.results)
    assert subscriber.error is None
    assert subscriber.results == [(b'GET', b'/aaa', None)]


@pytest.mark.parametrize('input', [
    [b'GET /aaa HTTP/1.1\r\nHost: aaa\r\n\r\nGET /bbb HTTP/1.1\r\nHost: aaa\r\n\r\n'],
    [b'GET /aaa HTTP/1.1\r\nHost: aaa\r\n\r\n', b'GET /bbb HTTP/1.1\r\nHost: aaa\r\n\r\n'],
    [b'GET /aaa HTTP/1.1\r\nHost: aaa\r\n\r\nG', b'ET /bbb HTTP/1.1\r\nHost: aaa\r\n\r\n'],
    [b'GET /aaa HTTP/1.1\r\nHost: aaa\r\n\r', b'\nGET /bbb HTTP/1.1\r\nHost: aaa\r\n\r\n'],
])
def test_http_get_2(input):
    subscriber = RequestSubscriberStub()

    Observable \
        .from_(input) \
        .http_parse() \
        .flat_map(content_loader) \
        .subscribe(subscriber)

    assert subscriber.error is None
    assert subscriber.results == [(b'GET', b'/aaa', None), (b'GET', b'/bbb', None)]


@pytest.mark.parametrize('input', [
    [b'POST /aaa HTTP/1.1\r\nContent-Length: 3\r\n\r\n123'],
    [b'POST /aaa H', b'TTP/1.1\r\nContent-Length: 3\r\n\r\n123'],
    [b'POST /aaa HTTP/1.1\r\n', b'Content-Length: 3\r\n\r\n123'],
    [b'POST /aaa HTTP/1.1\r\n', b'Content-Length: 3\r\n\r\n', b'123'],
    [b'POST /aaa HTTP/1.1\r\n', b'Content-Length: 3\r\n\r', b'\n', b'1', b'2', b'3'],
    [b'POST /aaa HTTP/1.1\r\nContent-Length: 3\r\n\r\n12345'],
])
def test_http_post_1(input):
    subscriber = RequestSubscriberStub()

    Observable \
        .from_(input) \
        .http_parse() \
        .flat_map(content_loader) \
        .subscribe(subscriber)

    assert subscriber.error is None
    assert subscriber.results == [(b'POST', b'/aaa', b'123')]


@pytest.mark.parametrize('input', [
    [b'POST /aaa HTTP/1.1\r\nContent-Length: 3\r\n\r\n123', b'POST /bbb HTTP/1.1\r\nContent-Length: 3\r\n\r\n456'],
    [b'POST /aaa HTTP/1.1\r\nContent-Length: 3\r\n\r\n123POST /bbb HTTP/1.1\r\nContent-Length: 3\r\n\r\n456'],
    [b'POST /aaa HTTP/1.1\r\nContent-Length: 3\r\n\r\n123POST', b' /bbb HTTP/1.1\r\nContent-Length: 3\r\n\r\n456'],
    [b'POST /aaa HTTP/1.1\r\nContent-Length: 3\r\n\r\n123POST /bbb HTTP/1.1\r\nContent-Length: 3\r\n\r\n', b'456'],
])
def test_http_post_2(input):
    subscriber = RequestSubscriberStub()

    Observable \
        .from_(input) \
        .http_parse() \
        .flat_map(content_loader) \
        .subscribe(subscriber)

    assert subscriber.error is None
    assert subscriber.results == [(b'POST', b'/aaa', b'123'), (b'POST', b'/bbb', b'456')]


@pytest.mark.parametrize('input', [
    [b'POST /aaa HTTP/1.1\r\nContent-Length: 3\r\n\r\n123', b'GET /bbb HTTP/1.1\r\n\r\n'],
])
def test_connection(input):
    def responder(request: HttpRequest):
        response = request.url
        return b'HTTP/1.1 200 OK\r\nContent-Length: ' + str(len(response)).encode() + b'\r\n\r\n' + response

    def acceptor(conn: Connection):
        logger.debug('accepted %r', conn)
        connection_finalizer = Subject()

        def signal_complete():
            logger.debug('completed %r', conn)
            connection_finalizer.on_next(conn)  # hand over connection for closing
            connection_finalizer.on_completed()

        try:
            conn.data_in \
                .map(tapper('data_in')) \
                .http_parse() \
                .map(tapper('parsed')) \
                .flat_map(content_loader) \
                .map(tapper('merged')) \
                .map(responder) \
                .finally_action(signal_complete) \
                .subscribe(conn.data_out)
        except:
            logger.exception('exception')

        return connection_finalizer

    conn = ConnectionStub(input)
    Observable.just(conn) \
        .map(tapper('listener')) \
        .flat_map(acceptor) \
        .map(tapper('listener accepted')) \
        .subscribe(lambda x: logger.debug('finalize connection %r', x), lambda x: x,
            lambda: logger.debug('listener complete'))

    assert conn.output == b'HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n/aaa' \
                          b'HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n/bbb'


class PostHandler(Handler):
    def handle(self, request, next_handlers):
        return self.load_content(request) \
            .map(lambda content: HttpResponse(200, b'OK', content=b'[' + content + b']'))


class GetHandler(Handler):
    def handle(self, request, next_handlers):
        return Observable.just(HttpResponse(200, b'OK', content=b'bbbb'))


class PostHandlerThreadPooled(Handler):
    def handle(self, request, next_handlers):
        def process(content: bytes) -> HttpResponse:
            logger.debug('start processing %s', content)
            time.sleep(1)
            logger.debug('end processing %s', content)
            return HttpResponse(200, b'OK', content=b'[' + content + b']')

        return self.load_content(request) \
            .observe_on(thread_pool_scheduler) \
            .map(process) \
            .observe_on(event_loop_scheduler)


@pytest.mark.parametrize('input', [
    [b'POST /aaa HTTP/1.1\r\nContent-Length: 3\r\n\r\n123', b'GET /bbb HTTP/1.1\r\n\r\n'],
    [b'POST /aaa HTTP/1.1\r\nContent-Length: 3\r\n\r\n123GET /bbb HTTP/1.1\r\n\r\n'],
    [b'POST /aaa HTTP/1.1\r\nContent-Length: 3\r\n\r\n123G', b'ET /bbb HTTP/1.1\r\n\r\n'],
    [b'POST /aaa HTTP/1.1\r\nContent-Length: ', b'3\r\n\r\n123', b'GET /bbb/1 HTTP/1.1\r\n\r\n'],
])
def test_http_server(input):
    dispatcher = Dispatcher()
    dispatcher.register_handlers(b'/aaa', PostHandler())
    dispatcher.register_handlers(b'/bbb', GetHandler())

    conn = ConnectionStub(input)
    server = HttpServer(Observable.just(conn), dispatcher)
    server.serve()

    assert conn.output == \
           b'HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: 5\r\n\r\n[123]' \
           b'HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: 4\r\n\r\nbbbb'


@pytest.mark.parametrize('input', [
    [b'POST /aaa HTTP/1.1\r\nContent-Length: 3\r\n\r\n123', b'GET /bbb HTTP/1.1\r\n\r\n'],
    [b'POST /aaa HTTP/1.1\r\nContent-Length: 3\r\n\r\n123GET /bbb HTTP/1.1\r\n\r\n'],
    [b'POST /aaa HTTP/1.1\r\nContent-Length: 3\r\n\r\n123G', b'ET /bbb HTTP/1.1\r\n\r\n'],
    [b'POST /aaa HTTP/1.1\r\nContent-Length: ', b'3\r\n\r\n123', b'GET /bbb HTTP/1.1\r\n\r\n'],
])
def test_404(input):
    dispatcher = Dispatcher()

    conn = ConnectionStub(input)
    server = HttpServer(Observable.just(conn), dispatcher)
    server.serve()

    assert conn.output == \
           b'HTTP/1.1 404 Not Found\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: 19\r\n\r\nNo handler for /aaa' \
           b'HTTP/1.1 404 Not Found\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: 19\r\n\r\nNo handler for /bbb'


@pytest.mark.parametrize('input', [
    [b'POST aaa HTTP/1.1\r\nContent-Length: 3\r\n\r\n123'],
    [b'POST /aaa\r\nContent-Length: 3\r\n\r\n123'],
    [b'POST /aaa HTTP/1.1\r\nContent-Length: 3\r\n\r\n12'],
])
def test_bad_request(input):
    dispatcher = Dispatcher()
    dispatcher.register_handlers(b'/aaa', PostHandler())

    conn = ConnectionStub(input)
    server = HttpServer(Observable.just(conn), dispatcher)
    server.serve()

    logger.debug('bad response: %s', conn.output)
    assert re.match(
            rb'HTTP/1.1 400 Bad Request \([^\)]+\)\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: 0\r\n\r\n$',
            conn.output)


@pytest.mark.parametrize('input', [
    [b'POST /aaa HTTP/1.1\r\nContent-Length: 3\r\n\r\n123', b'GET /bbb HTTP/1.1\r\n\r\n'],
    [b'POST /aaa HTTP/1.1\r\nContent-Length: 3\r\n\r\n123GET /bbb HTTP/1.1\r\n\r\n'],
    [b'POST /aaa HTTP/1.1\r\nContent-Length: 3\r\n\r\n123G', b'ET /bbb HTTP/1.1\r\n\r\n'],
    [b'POST /aaa HTTP/1.1\r\nContent-Length: ', b'3\r\n\r\n123', b'GET /bbb/1 HTTP/1.1\r\n\r\n'],
])
def test_scheduled_http_server(input):
    dispatcher = Dispatcher()
    dispatcher.register_handlers(b'/aaa', PostHandlerThreadPooled())
    dispatcher.register_handlers(b'/bbb', GetHandler())

    conn = ConnectionStub(input)
    listener = Observable.just(conn, scheduler=event_loop_scheduler)
    server = HttpServer(listener, dispatcher)
    server.serve()

    time.sleep(1.1)
    logger.debug('verify %s', conn.output)
    assert conn.output == \
           b'HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: 5\r\n\r\n[123]' \
           b'HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: 4\r\n\r\nbbbb'


@pytest.mark.parametrize('input', [
    [
        b'POST /aaa HTTP/1.1\r\nContent-Length: 3\r\n\r\n123',
        b'POST /aaa HTTP/1.1\r\nContent-Length: 3\r\n\r\n456',
        b'POST /aaa HTTP/1.1\r\nContent-Length: 3\r\n\r\n789',
        b'GET /bbb HTTP/1.1\r\n\r\n'
    ],
])
def test_parallel_processing(input):
    dispatcher = Dispatcher()
    dispatcher.register_handlers(b'/aaa', PostHandlerThreadPooled())
    dispatcher.register_handlers(b'/bbb', GetHandler())

    conn1 = ConnectionStub(input)
    conn2 = ConnectionStub(input)
    listener = Observable.of(conn1, conn2, scheduler=event_loop_scheduler)
    server = HttpServer(listener, dispatcher)
    server.serve()

    time.sleep(3.5)  # 6 slow requests over 2 connections => shall complete in 3 seconds
    for conn in (conn1, conn2):
        logger.debug('verify %s', conn.output)
        assert conn.output == \
               b'HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: 5\r\n\r\n[123]' \
               b'HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: 5\r\n\r\n[456]' \
               b'HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: 5\r\n\r\n[789]' \
               b'HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: 4\r\n\r\nbbbb'
