# coding=utf-8

import logging

from rx import Observer
from rx.core import ObservableBase
from rx.disposables import AnonymousDisposable

logger = logging.getLogger(__name__)


def tapper(message):
    def tap(x):
        logger.debug('[tapped] %s: %r', message, x)
        return x

    return tap


class BufferedSubject(ObservableBase, Observer):
    """
    Subject that buffers content until subscribed.
    Not strictly a Subject as it allows only a single subscription.
    """

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
            logger.debug('buffering %r', item)
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
