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

import sys
from typing import TYPE_CHECKING, Any

from futurist import _utils

if TYPE_CHECKING:
    import threading as stdlib_threading

    from eventlet import queue as greenqueue

# eventlet is imported when a green helper is first used, not when this
# module is imported, so that processes which never use one do not pay for
# it. Pool, Queue and spawn are looked up by __getattr__ below on first use.
Pool: Any
Queue: Any
spawn: Any


def __getattr__(name: str) -> Any:
    global Pool, Queue, spawn

    if name not in ('Pool', 'Queue', 'spawn'):
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    if _utils.EVENTLET_AVAILABLE:
        from eventlet import greenpool
        from eventlet import greenthread
        from eventlet import queue

        # Aliases that we use and only expose vs the whole of eventlet...
        Pool = greenpool.GreenPool
        Queue = queue.Queue
        spawn = greenthread.spawn
    else:
        Pool = Queue = spawn = None
    return globals()[name]


def is_monkey_patched(mod: str) -> bool:
    # Nothing can have been patched before eventlet was imported.
    if 'eventlet' not in sys.modules:
        return False
    from eventlet import patcher

    return bool(patcher.is_monkey_patched(mod))


class GreenThreading:
    @staticmethod
    def event_object() -> stdlib_threading.Event:
        from eventlet.green import threading as greenthreading

        return greenthreading.Event()  # type: ignore

    @staticmethod
    def lock_object() -> stdlib_threading.Lock:
        from eventlet.green import threading as greenthreading

        return greenthreading.Lock()  # type: ignore

    @staticmethod
    def rlock_object() -> stdlib_threading.RLock:
        from eventlet.green import threading as greenthreading

        return greenthreading.RLock()  # type: ignore

    @staticmethod
    def condition_object(
        lock: stdlib_threading.Lock | stdlib_threading.RLock | None = None,
    ) -> stdlib_threading.Condition:
        from eventlet.green import threading as greenthreading

        return greenthreading.Condition(lock=lock)  # type: ignore


# Creating this imports nothing; its methods import eventlet when called.
threading: GreenThreading | None = (
    GreenThreading() if _utils.EVENTLET_AVAILABLE else None
)


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
