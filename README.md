RxWeb ­ is a prototype HTTP server based on RxPy framework.

The project serves educational purpose (as RxPy example).
It can also be used as a basis for further development (not necessarily in Python).

Architecture
============

Listener is an observable that emits incoming connections.

Every `Connection` includes `data_in` observable and `data_out` observer, both operating on `bytes`.

`data_in` stream is processed by custom `http_parse` rx-operator that transforms incoming `bytes` 
into outgoing `HttpRequest`s.

`HttpRequest` might contain `content_in` observable carrying eg. POST request body.

`Handler` is a base class for custom request handlers. `Handler.handle()` takes `HttpRequest` 
and produces observable of `HttpResponse`.

There are sample handlers for subrequests and filters in test code.

`HttpResponse` contains `content_out` stream. It is serialized into `bytes`.

`Dispatcher` is a registry of handlers.

`HttpServer` wires it all up.

All described above is not bound to specific I/O library.

As an example, project includes sample server backed by `asyncio`.
 
Notes
=====

- Written in Python 3.6.
- HTTP support is very basic, not ready for real world.
- Contributions, comments, ideas welcome.
 