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

import contextlib
import functools
from collections.abc import Callable, Generator, Iterable
from typing import Any, NamedTuple, TypeVar

from concurrent import futures
from concurrent.futures import _base

import futurist
from futurist import _utils

try:
    from eventlet.green import threading as greenthreading
except ImportError:
    greenthreading = None


_F = TypeVar('_F', bound=futures.Future[Any])


class DoneAndNotDoneFutures(NamedTuple):
    """Named tuple returned from ``wait_for*`` calls."""

    done: set[futures.Future[Any]]
    not_done: set[futures.Future[Any]]


_DONE_STATES = frozenset(
    [
        _base.CANCELLED_AND_NOTIFIED,
        _base.FINISHED,
    ]
)


@contextlib.contextmanager
def _acquire_and_release_futures(
    fs: Iterable[futures.Future[Any]],
) -> Generator[None, None, None]:
    # Do this to ensure that we always get the futures in the same order (aka
    # always acquire the conditions in the same order, no matter what; a way
    # to avoid dead-lock).
    sorted_fs = sorted(fs, key=id)
    with contextlib.ExitStack() as stack:
        for fut in sorted_fs:
            stack.enter_context(fut._condition)
        yield


_WaitFunc = Callable[
    [Iterable[futures.Future[Any]], float | None], DoneAndNotDoneFutures
]


def _ensure_eventlet(func: _WaitFunc) -> _WaitFunc:
    """Decorator that verifies we have the needed eventlet components."""

    @functools.wraps(func)
    def wrapper(
        fs: Iterable[futures.Future[Any]], timeout: float | None = None
    ) -> DoneAndNotDoneFutures:
        if not _utils.EVENTLET_AVAILABLE or greenthreading is None:
            raise RuntimeError('Eventlet is needed to wait on green futures')
        return func(fs, timeout)

    return wrapper


def _wait_for(
    fs: Iterable[futures.Future[Any]],
    no_green_return_when: str,
    on_all_green_cb: _WaitFunc,
    caller_name: str,
    timeout: float | None = None,
) -> DoneAndNotDoneFutures:
    fs_list = list(fs)
    green_fs = sum(1 for f in fs_list if isinstance(f, futurist.GreenFuture))
    if not green_fs:
        done, not_done = futures.wait(
            fs_list, timeout=timeout, return_when=no_green_return_when
        )
        return DoneAndNotDoneFutures(done, not_done)
    else:
        non_green_fs = len(fs_list) - green_fs
        if non_green_fs:
            raise RuntimeError(
                f"Can not wait on {green_fs} green futures and {non_green_fs}"
                " non-green futures in the same"
                f" `{caller_name}` call"
            )
        else:
            return on_all_green_cb(fs_list, timeout)


def wait_for_all(
    fs: Iterable[futures.Future[Any]], timeout: float | None = None
) -> DoneAndNotDoneFutures:
    """Wait for all of the futures to complete.

    Works correctly with both green and non-green futures (but not both
    together, since this can't be guaranteed to avoid dead-lock due to how
    the waiting implementations are different when green threads are being
    used).

    Returns pair (done futures, not done futures).
    """
    return _wait_for(
        fs,
        futures.ALL_COMPLETED,
        _wait_for_all_green,
        'wait_for_all',
        timeout=timeout,
    )


def wait_for_any(
    fs: Iterable[futures.Future[Any]], timeout: float | None = None
) -> DoneAndNotDoneFutures:
    """Wait for one (**any**) of the futures to complete.

    Works correctly with both green and non-green futures (but not both
    together, since this can't be guaranteed to avoid dead-lock due to how
    the waiting implementations are different when green threads are being
    used).

    Returns pair (done futures, not done futures).
    """
    return _wait_for(
        fs,
        futures.FIRST_COMPLETED,
        _wait_for_any_green,
        'wait_for_any',
        timeout=timeout,
    )


class _AllGreenWaiter:
    """Provides the event that ``_wait_for_all_green`` blocks on."""

    def __init__(self, pending: int) -> None:
        self.event: Any = greenthreading.Event()
        self.lock: Any = greenthreading.Lock()
        self.pending = pending

    def _decrement_pending(self) -> None:
        with self.lock:
            self.pending -= 1
            if self.pending <= 0:
                self.event.set()

    def add_result(self, future: futures.Future[Any]) -> None:
        self._decrement_pending()

    def add_exception(self, future: futures.Future[Any]) -> None:
        self._decrement_pending()

    def add_cancelled(self, future: futures.Future[Any]) -> None:
        self._decrement_pending()


class _AnyGreenWaiter:
    """Provides the event that ``_wait_for_any_green`` blocks on."""

    def __init__(self) -> None:
        self.event: Any = greenthreading.Event()

    def add_result(self, future: futures.Future[Any]) -> None:
        self.event.set()

    def add_exception(self, future: futures.Future[Any]) -> None:
        self.event.set()

    def add_cancelled(self, future: futures.Future[Any]) -> None:
        self.event.set()


def _partition_futures(
    fs: Iterable[futures.Future[Any]],
) -> tuple[set[futures.Future[Any]], set[futures.Future[Any]]]:
    done: set[futures.Future[Any]] = set()
    not_done: set[futures.Future[Any]] = set()
    for f in fs:
        if f._state in _DONE_STATES:
            done.add(f)
        else:
            not_done.add(f)
    return done, not_done


_W = TypeVar('_W', _AllGreenWaiter, _AnyGreenWaiter)


def _create_and_install_waiters(
    fs: Iterable[futures.Future[Any]],
    waiter_cls: type[_W],
    *args: Any,
    **kwargs: Any,
) -> _W:
    waiter = waiter_cls(*args, **kwargs)
    for f in fs:
        f._waiters.append(waiter)  # type: ignore[arg-type]
    return waiter


@_ensure_eventlet
def _wait_for_all_green(
    fs: Iterable[futures.Future[Any]], timeout: float | None = None
) -> DoneAndNotDoneFutures:
    fs_list = list(fs)
    if not fs_list:
        return DoneAndNotDoneFutures(set(), set())

    with _acquire_and_release_futures(fs_list):
        done, not_done = _partition_futures(fs_list)
        if len(done) == len(fs_list):
            return DoneAndNotDoneFutures(done, not_done)
        waiter = _create_and_install_waiters(
            not_done, _AllGreenWaiter, len(not_done)
        )
    waiter.event.wait(timeout)
    for f in not_done:
        with f._condition:
            f._waiters.remove(waiter)  # type: ignore[arg-type]

    with _acquire_and_release_futures(fs_list):
        done, not_done = _partition_futures(fs_list)
        return DoneAndNotDoneFutures(done, not_done)


@_ensure_eventlet
def _wait_for_any_green(
    fs: Iterable[futures.Future[Any]], timeout: float | None = None
) -> DoneAndNotDoneFutures:
    fs_list = list(fs)
    if not fs_list:
        return DoneAndNotDoneFutures(set(), set())

    with _acquire_and_release_futures(fs_list):
        done, not_done = _partition_futures(fs_list)
        if done:
            return DoneAndNotDoneFutures(done, not_done)
        waiter = _create_and_install_waiters(fs_list, _AnyGreenWaiter)

    waiter.event.wait(timeout)
    for f in fs_list:
        with f._condition:
            f._waiters.remove(waiter)  # type: ignore[arg-type]

    with _acquire_and_release_futures(fs_list):
        done, not_done = _partition_futures(fs_list)
        return DoneAndNotDoneFutures(done, not_done)
