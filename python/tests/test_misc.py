# coding=utf-8
import asyncio
import logging
import time
import types

logger = logging.getLogger(__name__)


# Tests here have nothing to do with HTTP server.
# Just a playground.


async def coro1(a):
    return a + 2


async def coro2(a):
    b = await coro1(a)
    return b - 2


@types.coroutine
def coro3(a):
    b = yield a + 2
    return (b or 0) - 2


@types.coroutine
async def coro4(a):
    b = yield a + 2
    yield (b or 0) - 2


def test_coro1():
    r = coro1(9)
    logger.debug('coro1 %r', r)
    ai = r.__await__()
    logger.debug('coro1 ai %r', ai)
    try:
        next(ai)
    except StopIteration as e:
        assert e.value == 11

    for i in coro1(5).__await__():
        logger.debug('coro1 iter %r', i)
    else:
        logger.debug('coro1 no iter')

    try:
        coro1(3).send(None)
    except StopIteration as e:
        assert e.value == 5


def test_coro2():
    r = coro2(9)
    logger.debug('coro2 %r', r)
    ai = r.__await__()
    logger.debug('coro2 ai %r', ai)
    try:
        next(ai)
    except StopIteration as e:
        assert e.value == 9

    for i in coro2(5).__await__():
        logger.debug('coro2 iter %r', i)
    else:
        logger.debug('coro2 no iter')

    try:
        coro2(3).send(None)
    except StopIteration as e:
        assert e.value == 3


def test_coro3():
    r = coro3(9)
    logger.debug('coro3 %r', r)
    try:
        v = next(r)
        assert v == 11
        next(r)
    except StopIteration as e:
        assert e.value == -2

    try:
        r = coro3(3)
        v = r.send(None)
        assert v == 5
        r.send(6)
    except StopIteration as e:
        assert e.value == 4

    def gen(n):
        v = yield from coro3(n)
        assert v == -2

    vv = [v for v in gen(15)]
    assert vv == [17]


def test_coro4():
    async def a_test_coro4():
        r = coro4(9)
        logger.debug('coro4 %r', r)
        ai = r.__aiter__()
        logger.debug('coro4 ai %r', ai)
        try:
            v = await r.__anext__()
            logger.debug('coro4 anext %r', v)
            assert v == 11
            v = await r.__anext__()
            logger.debug('coro4 anext %r', v)
            assert v == -2
            v = await r.__anext__()
            logger.debug('coro4 anext %r', v)
            assert False
        except StopAsyncIteration as e:
            logger.exception('coro4 StopAsyncIteration %r', e)

        try:
            r = coro4(3)
            v = await r.asend(None)
            assert v == 5
            v = await r.asend(6)
            assert v == 4
            await r.asend(33)
            assert False
        except StopAsyncIteration as e:
            logger.exception('coro4 StopAsyncIteration %r', e)

        vv = [v async for v in coro4(44)]
        assert vv == [46, -2]

    loop = asyncio.get_event_loop()
    loop.run_until_complete(a_test_coro4())


def test_future():
    loop = asyncio.get_event_loop()

    async def target(x: int) -> int:
        loop.run_in_executor(None, time.sleep, 0.1)
        return x + 1

    def intermediate(x: int) -> asyncio.Future:
        return loop.create_task(target(x))

    async def main():
        future = intermediate(5)
        logger.debug('intermediate future = %r', future)
        value = await future
        assert value == 6

    loop.create_task(main())
    loop.call_later(0.5, loop.stop)
    loop.run_forever()
