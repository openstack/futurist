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

import heapq
import inspect
import logging
import threading

import six

from futurist import _utils

LOG = logging.getLogger(__name__)

_REQUIRED_ATTRS = ('_is_periodic', '_periodic_spacing',
                   '_periodic_run_immediately')


def _check_attrs(obj):
    """Checks that a periodic function/method has all the expected attributes.

    This will return the expected attributes that were **not** found.
    """
    missing_attrs = []
    for attr_name in _REQUIRED_ATTRS:
        if not hasattr(obj, attr_name):
            missing_attrs.append(attr_name)
    return missing_attrs


def periodic(spacing, run_immediately=False):
    """Tags a method/function as wanting/able to execute periodically.

    :param spacing: how often to run the decorated function (required)
    :type spacing: float/int
    :param run_immediately: option to specify whether to run
                            immediately or wait until the spacing provided has
                            elapsed before running for the first time
    :type run_immediately: boolean
    """

    if spacing <= 0:
        raise ValueError("Periodicity/spacing must be greater than"
                         " zero instead of %s" % spacing)

    def wrapper(f):
        f._is_periodic = True
        f._periodic_spacing = spacing
        f._periodic_run_immediately = run_immediately

        @six.wraps(f)
        def decorator(*args, **kwargs):
            return f(*args, **kwargs)

        return decorator

    return wrapper


class _Schedule(object):
    """Internal heap-based structure that maintains the schedule/ordering.

    This stores a heap composed of the following (next_run, index) where
    next_run is the next desired runtime for the callback that is stored
    somewhere with the index provided. The index is saved so that if two
    functions with the same next_run time are inserted, that the one with
    the smaller index is preferred (it is also saved so that on pop we can
    know what the index of the callback we should call is).
    """

    def __init__(self):
        self._ordering = []

    def push(self, next_run, index):
        heapq.heappush(self._ordering, (next_run, index))

    def push_next(self, cb, index, now=None):
        if now is None:
            now = _utils.now()
        next_run = now + cb._periodic_spacing
        self.push(next_run, index)

    def __len__(self):
        return len(self._ordering)

    def pop(self):
        return heapq.heappop(self._ordering)


def _build(callables):
    schedule = _Schedule()
    now = None
    immediates = []
    # Reverse order is used since these are later popped off (and to
    # ensure the popping order is first -> last we need to append them
    # in the opposite ordering last -> first).
    for i, (cb, args, kwargs) in _utils.reverse_enumerate(callables):
        if cb._periodic_run_immediately:
            immediates.append(i)
        else:
            if now is None:
                now = _utils.now()
            schedule.push_next(cb, i, now=now)
    return immediates, schedule


def _safe_call(cb, args, kwargs, kind, log):
    try:
        cb(*args, **kwargs)
    except Exception:
        how_often = cb._periodic_spacing
        log.exception("Failed to call %s '%r' (it runs every %0.2f"
                      " seconds)", kind, cb, how_often)


class PeriodicWorker(object):
    """Calls a collection of callables periodically (sleeping as needed...).

    NOTE(harlowja): typically the :py:meth:`.start` method is executed in a
    background thread so that the periodic callables are executed in
    the background/asynchronously (using the defined periods to determine
    when each is called).
    """

    _NO_OP_ARGS = ()
    _NO_OP_KWARGS = {}

    @classmethod
    def create(cls, objects, exclude_hidden=True, log=None):
        """Automatically creates a worker by analyzing object(s) methods.

        Only picks up methods that have been tagged/decorated with
        the :py:func:`.periodic` decorator (does not match against private
        or protected methods unless explicitly requested to).

        :param objects: the objects to introspect for decorated members
        :type objects: iterable
        :param exclude_hidden: exclude hidden members (ones that start with
                               an underscore)
        :type exclude_hidden: bool
        :param log: logger to use when creating a new worker (defaults
                    to the module logger if none provided), it is currently
                    only used to report callback failures (if they occur)
        :type log: logger
        """
        callables = []
        for obj in objects:
            for (name, member) in inspect.getmembers(obj):
                if name.startswith("_") and exclude_hidden:
                    continue
                if six.callable(member):
                    missing_attrs = _check_attrs(member)
                    if not missing_attrs:
                        # These do not support custom args, kwargs...
                        callables.append((member,
                                          cls._NO_OP_ARGS,
                                          cls._NO_OP_KWARGS))
        return cls(callables, log=log)

    def __init__(self, callables, log=None):
        """Creates a new worker using the given periodic callables.

        :param callables: a iterable of tuple objects previously decorated
                          with the :py:func:`.periodic` decorator, each item
                          in the iterable is expected to be in the format
                          of ``(cb, args, kwargs)`` where ``cb`` is the
                          decorated function and ``args`` and ``kwargs`` are
                          any positional and keyword arguments to send into
                          the callback when it is activated (both ``args``
                          and ``kwargs`` may be provided as none to avoid
                          using them)
        :type callables: iterable
        :param log: logger to use when creating a new worker (defaults
                    to the module logger if none provided), it is currently
                    only used to report callback failures (if they occur)
        :type log: logger
        """

        self._tombstone = threading.Event()
        self._callables = []
        for (cb, args, kwargs) in callables:
            if not six.callable(cb):
                raise ValueError("Periodic callback %r must be callable" % cb)
            missing_attrs = _check_attrs(cb)
            if missing_attrs:
                raise ValueError("Periodic callback %r missing required"
                                 " attributes %s" % (cb, missing_attrs))
            if cb._is_periodic:
                # Ensure these aren't none and if so replace them with
                # something more appropriate...
                if args is None:
                    args = self._NO_OP_ARGS
                if kwargs is None:
                    kwargs = self._NO_OP_KWARGS
                self._callables.append((cb, args, kwargs))
        self._immediates, self._schedule = _build(self._callables)
        self._log = log or LOG

    def __len__(self):
        return len(self._callables)

    def start(self):
        """Starts running (will not return until :py:meth:`.stop` is called).

        NOTE(harlowja): If this worker has no contained callables this raises
        a runtime error and does not run since it is impossible to periodically
        run nothing.
        """
        if not self._callables:
            raise RuntimeError("A periodic worker can not start"
                               " without any callables")
        while not self._tombstone.is_set():
            if self._immediates:
                # Run & schedule its next execution.
                index = self._immediates.pop()
                cb, args, kwargs = self._callables[index]
                _safe_call(cb, args, kwargs, 'immediate', self._log)
                self._schedule.push_next(cb, index)
            else:
                # Figure out when we should run next (by selecting the
                # minimum item from the heap, where the minimum should be
                # the callable that needs to run next and has the lowest
                # next desired run time).
                now = _utils.now()
                next_run, index = self._schedule.pop()
                when_next = next_run - now
                if when_next <= 0:
                    # Run & schedule its next execution.
                    cb, args, kwargs = self._callables[index]
                    _safe_call(cb, args, kwargs, 'periodic', self._log)
                    self._schedule.push_next(cb, index, now=now)
                else:
                    # Gotta wait...
                    self._schedule.push(next_run, index)
                    self._tombstone.wait(when_next)

    def stop(self):
        """Sets the tombstone (this stops any further executions)."""
        self._tombstone.set()

    def reset(self):
        """Resets the tombstone and re-queues up any immediate executions."""
        self._tombstone.clear()
        self._immediates, self._schedule = _build(self._callables)
