# coding=utf-8
import asyncio
import logging
import time

from rx import Observable
from rx.concurrency import thread_pool_scheduler

from rxweb.async import AsyncScheduler, AsyncListener
from rxweb.server import HttpServer, Dispatcher, Handler, HttpResponse

logging.basicConfig(level=logging.DEBUG, format='%(asctime)-15s %(levelname)-5s %(name)-17s %(threadName)s %(message)s')
logger = logging.getLogger(__name__)


class GetHandler(Handler):
    def handle(self, request, next_handlers):
        return Observable.just(HttpResponse(200, b'OK', content=b'Hello, RxWeb!'))


class PostHandlerSync(Handler):
    def __init__(self, scheduler):
        super().__init__()
        self.scheduler = scheduler

    def handle(self, request, next_handlers):
        # use thread_pool_scheduler to process requests
        def process(content: bytes) -> HttpResponse:
            logger.debug('start processing %s', content)
            time.sleep(1)
            logger.debug('end processing %s', content)
            return HttpResponse(200, b'OK', content=b'[' + (content or b'-empty-') + b']')

        return self.load_content(request) \
            .observe_on(thread_pool_scheduler) \
            .map(process) \
            .observe_on(self.scheduler)


class PostHandlerAsync(Handler):
    def __init__(self, loop):
        super().__init__()
        self.loop = loop  # type: AbstractEventLoop

    def handle(self, request, next_handlers):
        # use asyncio event loop & coroutine to process requests
        async def process(content: bytes) -> HttpResponse:
            logger.debug('start processing %s', content)
            await asyncio.sleep(1)
            logger.debug('end processing %s', content)
            return HttpResponse(200, b'OK', content=b'[' + (content or b'-empty-') + b']')

        return self.load_content(request) \
            .flat_map(lambda content: Observable.from_future(self.loop.create_task(process(content))))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    scheduler = AsyncScheduler(loop)
    listener = AsyncListener(loop, '127.0.0.1', 8888)

    dispatcher = Dispatcher()
    dispatcher.register_handlers(b'/hello', GetHandler())
    dispatcher.register_handlers(b'/post/sync', PostHandlerSync(scheduler))
    dispatcher.register_handlers(b'/post/async', PostHandlerAsync(loop))

    HttpServer(listener, dispatcher, scheduler).serve()
    try:
        loop.run_forever()
    finally:
        loop.close()
