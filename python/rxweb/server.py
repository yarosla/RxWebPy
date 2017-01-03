# coding=utf-8

import logging

from rx import AnonymousObservable
from rx import Observable
from rx import Observer
from rx.disposables import RefCountDisposable
from rx.disposables import SingleAssignmentDisposable
from rx.internal import extensionmethod
from rx.internal.utils import add_ref
from rx.subjects import Subject

logger = logging.getLogger(__name__)


class Connection:
    def __init__(self, data_in: Observable, data_out: Observer):
        self.data_in = data_in
        self.data_out = data_out


class HttpResponse(Exception):
    def __init__(self, status_code, status_text, content=None, headers=None,
                 content_out=None, keep_alive=True, conn=None):
        super(HttpResponse, self).__init__(str(status_code) + ' ' + str(status_text))
        self.conn = conn
        # self.response_out=response_out or http_writer
        self.status_code = status_code
        self.status_text = status_text
        self.content = content
        self.keep_alive = keep_alive
        if content_out:
            self.content_out = content_out
            self.headers = headers or ('Content-Type: text/plain; charset=utf-8', 'Connection: close')
            self.keep_alive = False
        else:
            content = content or ''
            self.content_out = Observable.just(content)
            self.headers = headers or ('Content-Type: text/plain; charset=utf-8',
            'Content-Length: ' + str(len(content)))
        self.headers_out = Observable.just(self.render_headers())

    def render_headers(self):
        return 'HTTP/1.1 ' + str(self.status_code) + ' ' + self.status_text + '\r\n' \
               + '\r\n'.join(self.headers) + '\r\n\r\n'

    def __str__(self):
        return '<HttpResponse: %d %s>' % (self.status_code, self.status_text)


class HttpRequest:
    def __init__(self, method: bytes, url: bytes, content_length: int = 0):
        self.method = method
        self.url = url
        self.content_length = content_length
        self.content_in = None  # type: Observable
        self.content_received = None  # type: bytes


class HttpParser(Observer):
    def __init__(self, observer: Observer, ref_count_disposable: RefCountDisposable):
        self.ref_count_disposable = ref_count_disposable
        self.observer = observer
        self.buffer = bytearray()
        self.receiving_headers = True
        self.content_remaining = 0
        self.content_in = None  # type: Subject

    def on_next(self, b: bytes):
        if self.receiving_headers:
            self.receive_headers(b)
        else:
            self.receive_body(b)

    def receive_headers(self, b: bytes):
        self.buffer += b
        eor = self.buffer.find(b'\r\n\r\n')
        if eor >= 0:
            # self.read_timer_off()
            headers = self.buffer[:eor]
            remainder = self.buffer[eor + 4:]
            self.buffer = bytearray()
            try:
                request = self.parse(headers)
                self.content_remaining = request.content_length
                if self.content_remaining:
                    self.receiving_headers = False
                    self.content_in = Subject()
                    request.content_in = add_ref(self.content_in, self.ref_count_disposable)
                    self.observer.on_next(request)
                    if remainder:
                        self.receive_body(bytes(remainder))
                else:
                    self.observer.on_next(request)
                    if remainder:
                        self.receive_headers(bytes(remainder))
            except HttpResponse as e:
                self.observer.on_error(e)

    def receive_body(self, b: bytes):
        n = len(b)
        if n > self.content_remaining:
            self.content_in.on_next(b[:self.content_remaining])
            remainder = b[self.content_remaining:]
        else:
            self.content_in.on_next(b)
            remainder = None
        self.content_remaining -= n
        if self.content_remaining <= 0:
            self.content_in.on_completed()
            self.content_in = None
            self.receiving_headers = True
            if remainder:
                self.receive_headers(remainder)
                # self.read_timer_off()

    def on_error(self, exception):
        if self.receiving_headers:
            self.observer.on_error(exception)
        else:
            self.content_in.on_error(exception)

    def on_completed(self):
        if self.receiving_headers:
            self.observer.on_completed()
        else:
            self.content_in.on_error(HttpResponse(400, 'Bad Request'))

    def parse(self, headers: bytearray):
        lines = headers.split(b'\r\n')
        first_line = lines[0].split()
        if len(first_line) != 3:
            raise HttpResponse(400, 'Bad Request')
        content_length = 0
        for line in lines[1:]:
            key, value = (v.strip() for v in line.split(b':', 1))
            key = key.lower()
            if key == b'content-length':
                content_length = int(value)
                # self.read_timer_on()
        return HttpRequest(bytes(first_line[0]), bytes(first_line[1]), content_length)


@extensionmethod(Observable)
def http_parse(self):
    source = self

    def subscribe(observer):
        m = SingleAssignmentDisposable()
        ref_count_disposable = RefCountDisposable(m)
        parser = HttpParser(observer, ref_count_disposable)
        m.disposable = source.subscribe(parser)
        return ref_count_disposable

    return AnonymousObservable(subscribe)
