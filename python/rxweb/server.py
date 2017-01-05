# coding=utf-8

import logging
from typing import List, Optional

from rx import AnonymousObservable
from rx import Observable
from rx import Observer
from rx.core import ObservableBase
from rx.disposables import AnonymousDisposable
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

    def close(self):
        pass


class HttpResponse(Exception):
    def __init__(self, status_code: int, status_text: bytes, headers=None,
                 content: bytes = None, content_out: Observable = None):
        super(HttpResponse, self).__init__(str(status_code) + ' ' + str(status_text))
        self.status_code = status_code  # type: int
        self.status_text = status_text  # type: bytes
        if content_out:
            self.content_out = content_out
            self.headers = headers or (b'Content-Type: text/plain; charset=utf-8', b'Connection: close')
        else:
            content = content or b''
            self.content_out = Observable.just(content)
            self.headers = headers or \
                           (b'Content-Type: text/plain; charset=utf-8',
                           b'Content-Length: ' + str(len(content)).encode())

    def serialize_headers(self) -> bytes:
        return b'HTTP/1.1 ' + str(self.status_code).encode() + b' ' + self.status_text + b'\r\n' \
               + b'\r\n'.join(self.headers) + b'\r\n\r\n'

    def serialize(self) -> Observable:
        return self.content_out.start_with(self.serialize_headers())

    def __repr__(self):
        return '<HttpResponse: %d %s>' % (self.status_code, self.status_text)


class BadRequest(HttpResponse):
    def __init__(self, message: bytes = None):
        super().__init__(400, b'Bad Request' + (b' (' + message + b')' if message else b''))


class NotFound(HttpResponse):
    def __init__(self, content: bytes = None):
        super().__init__(404, b'Not Found', content=content)


class HttpRequest:
    def __init__(self, method: bytes, url: bytes, content_length: int = 0):
        self.method = method
        self.url = url
        self.content_length = content_length
        self.content_in = None  # type: Observable
        self.content_received = None  # type: bytes


class BufferedSubject(ObservableBase, Observer):
    def __init__(self):
        super().__init__()
        self.buffer = []
        self.observer = None  # type: Observer
        self.error = None
        self.completed = False
        self.disposed = False

    def on_next(self, item):
        self.check_disposed()
        if self.observer:
            self.observer.on_next(item)
        else:
            self.buffer.append(item)

    def on_completed(self):
        if self.observer:
            self.observer.on_completed()
        else:
            self.completed = True

    def on_error(self, error):
        if self.observer:
            self.observer.on_error(error)
        else:
            self.error = error

    def _subscribe_core(self, observer: Observer):
        self.observer = observer
        # flush buffer
        for item in self.buffer:
            observer.on_next(item)
        self.buffer = None
        if self.error:
            observer.on_error(self.error)
        elif self.completed:
            observer.on_completed()
        return AnonymousDisposable.create(self.dispose)

    def dispose(self):
        logger.debug('disposing BufferedSubject')
        self.observer = None
        self.disposed = True

    def check_disposed(self):
        if self.disposed:
            logger.error('BufferedSubject already disposed')
            raise Exception('BufferedSubject already disposed')


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
                    self.content_in = BufferedSubject()
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
            self.observer.on_error(BadRequest(b'premature end of data stream'))

    def parse(self, headers: bytearray):
        lines = headers.split(b'\r\n')
        first_line = lines[0].split()
        if len(first_line) != 3:
            raise BadRequest(b'first line must contain three words')
        method, url, protocol = [bytes(p) for p in first_line]
        if not url.startswith(b'/'):
            raise BadRequest(b'url must start with /; got ' + url)
        content_length = 0
        for line in lines[1:]:
            key, value = (v.strip() for v in line.split(b':', 1))
            key = key.lower()
            if key == b'content-length':
                content_length = int(value)
                # self.read_timer_on()
        return HttpRequest(method, url, content_length)


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


class Handler:
    dispatcher = None  # type: Dispatcher

    def handle(self, request, next_handlers):
        # type: (HttpRequest, List[HttpRequest]) -> Optional[Observable]
        return self.pass_to_next(request, next_handlers)

    def pass_to_next(self, request, next_handlers):
        # type: (HttpRequest, List[HttpRequest]) -> Optional[Observable]
        if next_handlers:
            next_handler = next_handlers[0]  # type: Handler
            return next_handler.handle(request, next_handlers[1:])
        return None

    def load_content(self, request):
        # type: (HttpRequest) -> Observable
        if request.content_in:
            return request.content_in \
                .map(tapper('content_in')) \
                .reduce(lambda acc, x: acc + x, b'')
        else:
            return Observable.just(None)


class Dispatcher:
    def __init__(self):
        self.handlers = {}

    def register_handlers(self, url_prefix: bytes, *handlers: List[Handler]):
        self.handlers[url_prefix] = handlers
        for handler in handlers:
            handler.dispatcher = self

    def handle_request(self, request: HttpRequest) -> Observable:
        try:
            url = request.url
            assert url and url.startswith(b'/')

            # strip query string
            query_index = url.find(b'?')
            if query_index >= 0:
                url = url[:query_index]

            while url:
                handlers = self.handlers.get(url)
                if handlers:
                    result = handlers[0].handle(request, handlers[1:])
                    if result:
                        return result
                url = url[:url.rfind(b'/')]
        except Exception as e:
            logger.exception('failed to handle %r', e)

        return self.default_handler(request)

    def default_handler(self, request: HttpRequest):
        return Observable.just(NotFound(b'No handler for ' + request.url))


def tapper(message):
    def tap(x):
        logger.debug('[tapped] %s: %r', message, x)
        return x

    return tap


class HttpServer:
    def __init__(self, listener: Observable, dispatcher: Dispatcher):
        self.listener = listener
        self.dispatcher = dispatcher

    def accept_connection(self, conn: Connection) -> Observable:
        logger.debug('connection accepted %r', conn)
        connection_finalizer = Subject()

        def signal_complete():
            connection_finalizer.on_next(conn)  # hand over connection for closing
            connection_finalizer.on_completed()

        def on_error(exception=None):
            if isinstance(exception, HttpResponse):
                return Observable.just(exception)
            return Observable.empty()

        try:
            conn.data_in \
                .http_parse() \
                .map(tapper('parsed')) \
                .map(self.dispatcher.handle_request) \
                .concat_all() \
                .map(tapper('handled')) \
                .on_error_resume_next(on_error) \
                .map(tapper('error handled')) \
                .map(self.serialize_response) \
                .concat_all() \
                .map(tapper('serialized')) \
                .finally_action(signal_complete) \
                .map(tapper('out')) \
                .subscribe(conn.data_out)
        except Exception as e:
            logger.exception('exception while processing connection %r', e)

        return connection_finalizer.as_observable()

    def finalize_connection(self, conn: Connection):
        logger.debug('connection complete %r', conn)
        conn.close()

    def serialize_response(self, response: HttpResponse) -> Observable:
        try:
            return response.serialize()
        except Exception as e:
            logger.exception('failed to serialize response %r', e)

    def serve(self):
        self.listener \
            .flat_map(self.accept_connection) \
            .subscribe(self.finalize_connection)
