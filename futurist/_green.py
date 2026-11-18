#    Copyright (C) 2015 Yahoo! Inc. All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from __future__ import annotations

import functools
import sys
from types import ModuleType
from typing import TYPE_CHECKING

from futurist import _utils

if TYPE_CHECKING:
    from collections.abc import Callable
    import threading as stdlib_threading

    from eventlet import greenpool
    from eventlet import greenthread
    from eventlet import queue as greenqueue


def _require_eventlet(what: str) -> None:
    """Refuse to go further when eventlet is not installed.

    The accessors below are only reached from the green executors and
    futures, which already refuse to be constructed without eventlet, so
    this is a backstop. It reports the cause rather than letting an
    ImportError escape from somewhere deeper.
    """
    if not _utils.EVENTLET_AVAILABLE:
        raise RuntimeError(f'Eventlet is needed to use {what}')


@functools.cache
def get_pool_class() -> type[greenpool.GreenPool]:
    """eventlet's green pool class."""
    _require_eventlet('a green executor')
    from eventlet import greenpool

    return greenpool.GreenPool  # type: ignore[no-any-return]


@functools.cache
def get_queue_class() -> type[greenqueue.Queue[_utils.WorkItem]]:
    """eventlet's green queue class, as we use it: a queue of work items."""
    _require_eventlet('a green executor')
    from eventlet import queue

    return queue.Queue  # type: ignore[no-any-return]


@functools.cache
def get_spawn() -> Callable[..., greenthread.GreenThread]:
    """eventlet's greenthread spawn function."""
    _require_eventlet('a green thread')
    from eventlet import greenthread

    return greenthread.spawn  # type: ignore[no-any-return]


@functools.cache
def _green_threading() -> ModuleType:
    """eventlet's green threading module."""
    _require_eventlet('green threading primitives')
    from eventlet.green import threading as greenthreading

    return greenthreading  # type: ignore[no-any-return]


def is_monkey_patched(mod: str) -> bool:
    # Nothing can have been patched before eventlet was imported.
    if 'eventlet' not in sys.modules:
        return False
    from eventlet import patcher

    return bool(patcher.is_monkey_patched(mod))


class GreenThreading:
    @staticmethod
    def event_object() -> stdlib_threading.Event:
        return _green_threading().Event()  # type: ignore

    @staticmethod
    def lock_object() -> stdlib_threading.Lock:
        return _green_threading().Lock()  # type: ignore

    @staticmethod
    def rlock_object() -> stdlib_threading.RLock:
        return _green_threading().RLock()  # type: ignore

    @staticmethod
    def condition_object(
        lock: stdlib_threading.Lock | stdlib_threading.RLock | None = None,
    ) -> stdlib_threading.Condition:
        return _green_threading().Condition(lock=lock)  # type: ignore


# Constructing this imports nothing; its methods import eventlet when they
# are called, and raise if it is missing. It is not optional, so callers do
# not have to narrow it before every use.
threading: GreenThreading = GreenThreading()


class GreenWorker:
    def __init__(
        self,
        work: _utils.WorkItem,
        work_queue: greenqueue.Queue[_utils.WorkItem],
    ) -> None:
        self.work = work
        self.work_queue = work_queue

    def __call__(self) -> None:
        from eventlet.queue import Empty

        # Run our main piece of work.
        try:
            self.work.run()
        except SystemExit as e:
            exc_info = sys.exc_info()
            try:
                while True:
                    try:
                        w = self.work_queue.get_nowait()
                    except Empty:
                        break

                    try:
                        w.fail(exc_info)
                    finally:
                        self.work_queue.task_done()
            finally:
                del exc_info
                raise e

        # Consume any delayed work before finishing (this is how we finish
        # work that was to big for the pool size, but needs to be finished
        # no matter).
        while True:
            try:
                w = self.work_queue.get_nowait()
            except Empty:
                break
            else:
                try:
                    w.run()
                finally:
                    self.work_queue.task_done()
