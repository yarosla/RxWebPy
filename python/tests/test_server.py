# coding=utf-8
import logging

import pytest
from rx import Observable
from rx.core import ObserverBase
from rx.subjects import Subject

from rxweb.server import HttpRequest, Connection, HttpResponse

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

    assert subscriber.results == [(b'POST', b'/aaa', b'123'), (b'POST', b'/bbb', b'456')]


@pytest.mark.parametrize('input', [
    [b'POST /aaa HTTP/1.1\r\nContent-Length: 3\r\n\r\n123', b'GET /bbb HTTP/1.1\r\n\r\n'],
])
def test_connection(input):
    results = []

    def subscriber(b: bytes):
        results.append(b)

    def responder(request: HttpRequest):
        response = request.url
        return b'200 OK\r\nContent-Length: ' + str(len(response)).encode('utf-8') + b'\r\n\r\n' + response

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

    data_in = Observable.from_(input)
    data_out = Subject()
    data_out.reduce(lambda acc, x: acc + x, b'').subscribe(subscriber)
    conn = Connection(data_in, data_out)
    Observable.just(conn) \
        .map(tapper('listener')) \
        .flat_map(acceptor) \
        .map(tapper('listener accepted')) \
        .subscribe(lambda x: logger.debug('finalize connection %r', x), lambda x: x,
            lambda: logger.debug('listener complete'))

    assert results == [b'200 OK\r\nContent-Length: 4\r\n\r\n/aaa200 OK\r\nContent-Length: 4\r\n\r\n/bbb']
