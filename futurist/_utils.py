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

from collections.abc import Callable, Generator
from concurrent import futures
import contextlib
import multiprocessing
import sys
import threading
from time import monotonic
import traceback
from types import TracebackType
from typing import Any, ParamSpec, TypeAlias, TypeVar

_P = ParamSpec('_P')
_R = TypeVar('_R')

now = monotonic

try:
    import eventlet as _eventlet  # noqa

    EVENTLET_AVAILABLE = True
except ImportError:
    EVENTLET_AVAILABLE = False


ExcInfoT: TypeAlias = tuple[
    type[BaseException] | None,
    BaseException | None,
    TracebackType | None,
]


class WorkItem:
    """A thing to be executed by an executor."""

    def __init__(
        self,
        future: futures.Future[_R],
        fn: Callable[_P, _R],
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> None:
        self.future = future
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def run(self) -> None:
        if not self.future.set_running_or_notify_cancel():
            return
        try:
            result = self.fn(*self.args, **self.kwargs)
        except SystemExit as e:
            try:
                self.fail()
            finally:
                raise e
        except BaseException:
            self.fail()
        else:
            self.future.set_result(result)

    def fail(self, exc_info: ExcInfoT | None = None) -> None:
        exc_type, exc_value, exc_tb = exc_info or sys.exc_info()
        try:
            self.future.set_exception(exc_value)
        finally:
            if exc_info is None:
                del exc_type, exc_value, exc_tb


class Failure:
    """Object that captures a exception (and its associated information)."""

    exc_info: ExcInfoT
    traceback: str | None

    def __init__(self, retain_tb: bool) -> None:
        exc_info = sys.exc_info()
        if not any(exc_info):
            raise RuntimeError(
                "No active exception being handled, can "
                "not create a failure which represents"
                "nothing"
            )

        try:
            if retain_tb:
                self.exc_info = exc_info
                self.traceback = None
            else:
                self.exc_info = (exc_info[0], exc_info[1], None)
                self.traceback = "".join(traceback.format_exception(*exc_info))
        finally:
            del exc_info

    @property
    def exc_type(self) -> type[BaseException]:
        exc_type = self.exc_info[0]
        assert exc_type is not None
        return exc_type

    @property
    def exc_value(self) -> BaseException:
        exc_value = self.exc_info[1]
        assert exc_value is not None
        return exc_value


def get_callback_name(cb: Callable[..., Any]) -> str:
    """Tries to get a callbacks fully-qualified name.

    If no name can be produced ``repr(cb)`` is called and returned.
    """
    segments = []
    try:
        segments.append(cb.__qualname__)
    except AttributeError:
        try:
            segments.append(cb.__name__)
        except AttributeError:
            pass

    if not segments:
        return repr(cb)

    try:
        # When running under sphinx it appears this can be none?
        if cb.__module__:
            segments.insert(0, cb.__module__)
    except AttributeError:
        pass

    return ".".join(segments)


def get_optimal_thread_count(default: int = 5) -> int:
    """Try to guess optimal thread count for current system."""
    try:
        return multiprocessing.cpu_count() * 5
    except NotImplementedError:
        return default


def get_optimal_process_count(default: int = 1) -> int:
    """Try to guess optimal process count for current system."""
    try:
        return multiprocessing.cpu_count()
    except NotImplementedError:
        return default


class Barrier:
    """A class that ensures active <= 0 occur before unblocking."""

    def __init__(
        self, cond_cls: type[threading.Condition] = threading.Condition
    ) -> None:
        self._active = 0
        self._cond = cond_cls()

    @property
    def active(self) -> int:
        return self._active

    def incr(self) -> None:
        with self._cond:
            self._active += 1
            self._cond.notify_all()

    @contextlib.contextmanager
    def decr_cm(self) -> Generator[int, None, None]:
        with self._cond:
            self._active -= 1
            try:
                yield self._active
            finally:
                self._cond.notify_all()

    def decr(self) -> None:
        with self._cond:
            self._active -= 1
            self._cond.notify_all()

    def wait(self) -> None:
        with self._cond:
            while self._active > 0:
                self._cond.wait()
