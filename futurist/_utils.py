# -*- coding: utf-8 -*-

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

import inspect
import multiprocessing
import sys
import threading
import traceback

from monotonic import monotonic as now  # noqa
import six

try:
    import eventlet as _eventlet  # noqa
    EVENTLET_AVAILABLE = True
except ImportError:
    EVENTLET_AVAILABLE = False


class WorkItem(object):
    """A thing to be executed by a executor."""

    def __init__(self, future, fn, args, kwargs):
        self.future = future
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def run(self):
        if not self.future.set_running_or_notify_cancel():
            return
        try:
            result = self.fn(*self.args, **self.kwargs)
        except BaseException:
            exc_type, exc_value, exc_tb = sys.exc_info()
            try:
                if six.PY2:
                    self.future.set_exception_info(exc_value, exc_tb)
                else:
                    self.future.set_exception(exc_value)
            finally:
                del(exc_type, exc_value, exc_tb)
        else:
            self.future.set_result(result)


class Failure(object):
    """Object that captures a exception (and its associated information)."""

    def __init__(self, retain_tb):
        exc_info = sys.exc_info()
        if not any(exc_info):
            raise RuntimeError("No active exception being handled, can"
                               " not create a failure which represents"
                               " nothing")
        try:
            if retain_tb:
                self.exc_info = tuple(exc_info)
                self.traceback = None
            else:
                self.exc_info = (exc_info[0], exc_info[1], None)
                self.traceback = "".join(traceback.format_exception(*exc_info))
        finally:
            del exc_info


def get_callback_name(cb):
    """Tries to get a callbacks fully-qualified name.

    If no name can be produced ``repr(cb)`` is called and returned.
    """
    segments = []
    try:
        segments.append(cb.__qualname__)
    except AttributeError:
        try:
            segments.append(cb.__name__)
            if inspect.ismethod(cb):
                try:
                    # This attribute doesn't exist on py3.x or newer, so
                    # we optionally ignore it... (on those versions of
                    # python `__qualname__` should have been found anyway).
                    segments.insert(0, cb.im_class.__name__)
                except AttributeError:
                    pass
        except AttributeError:
            pass
    if not segments:
        return repr(cb)
    else:
        try:
            # When running under sphinx it appears this can be none?
            if cb.__module__:
                segments.insert(0, cb.__module__)
        except AttributeError:
            pass
        return ".".join(segments)


def get_optimal_thread_count(default=2):
    """Try to guess optimal thread count for current system."""
    try:
        return multiprocessing.cpu_count() + 1
    except NotImplementedError:
        # NOTE(harlowja): apparently may raise so in this case we will
        # just setup two threads since it's hard to know what else we
        # should do in this situation.
        return default


class Barrier(object):
    """A class that ensures active <= 0 occur before unblocking."""

    def __init__(self, cond_cls=threading.Condition):
        self._active = 0
        self._cond = cond_cls()

    def incr(self):
        with self._cond:
            self._active += 1
            self._cond.notify_all()

    def decr(self):
        with self._cond:
            self._active -= 1
            self._cond.notify_all()

    def wait(self):
        with self._cond:
            while self._active > 0:
                self._cond.wait()
