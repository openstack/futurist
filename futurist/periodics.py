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

import functools
import heapq
import inspect
import logging
import threading

import six

import futurist
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


class PeriodicWorker(object):
    """Calls a collection of callables periodically (sleeping as needed...).

    NOTE(harlowja): typically the :py:meth:`.start` method is executed in a
    background thread so that the periodic callables are executed in
    the background/asynchronously (using the defined periods to determine
    when each is called).
    """

    #: Max amount of time to wait when running (forces a wakeup when elapsed).
    MAX_LOOP_IDLE = 30

    _NO_OP_ARGS = ()
    _NO_OP_KWARGS = {}

    @classmethod
    def create(cls, objects, exclude_hidden=True,
               log=None, executor_factory=None):
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
        :param executor_factory: factory method that can be used to generate
                                 executor objects that will be used to
                                 run the periodic callables (if none is
                                 provided one will be created that uses
                                 the :py:class:`~futurist.SynchronousExecutor`
                                 class)
        :type executor_factory: callable
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
        return cls(callables, log=log, executor_factory=executor_factory)

    def __init__(self, callables, log=None, executor_factory=None):
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
        :param executor_factory: factory method that can be used to generate
                                 executor objects that will be used to
                                 run the periodic callables (if none is
                                 provided one will be created that uses
                                 the :py:class:`~futurist.SynchronousExecutor`
                                 class)
        :type executor_factory: callable
        """
        self._tombstone = threading.Event()
        self._waiter = threading.Condition()
        self._dead = threading.Event()
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
        if executor_factory is None:
            executor_factory = lambda: futurist.SynchronousExecutor()
        self._executor_factory = executor_factory

    def __len__(self):
        return len(self._callables)

    def _run(self, executor):
        """Main worker run loop."""

        def _on_done(kind, cb, index, fut, now=None):
            try:
                # NOTE(harlowja): accessing the result will cause it to
                # raise (so that we can log if it had/has failed).
                _r = fut.result()  # noqa
            except Exception:
                how_often = cb._periodic_spacing
                self._log.exception("Failed to call %s '%r' (it runs every"
                                    " %0.2f seconds)", kind, cb, how_often)
            finally:
                with self._waiter:
                    self._schedule.push_next(cb, index, now=now)
                    self._waiter.notify_all()

        while not self._tombstone.is_set():
            if self._immediates:
                # Run & schedule its next execution.
                try:
                    index = self._immediates.pop()
                except IndexError:
                    pass
                else:
                    cb, args, kwargs = self._callables[index]
                    now = _utils.now()
                    fut = executor.submit(cb, *args, **kwargs)
                    # Note that we are providing the time that it started
                    # at, so this implies that the rescheduling time will
                    # *not* take into account how long it took to actually
                    # run the callback... (for better or worse).
                    fut.add_done_callback(functools.partial(_on_done,
                                                            'immediate',
                                                            cb, index,
                                                            now=now))
            else:
                # Figure out when we should run next (by selecting the
                # minimum item from the heap, where the minimum should be
                # the callable that needs to run next and has the lowest
                # next desired run time).
                with self._waiter:
                    while (not self._schedule and
                           not self._tombstone.is_set()):
                        self._waiter.wait(self.MAX_LOOP_IDLE)
                    if self._tombstone.is_set():
                        break
                    now = _utils.now()
                    next_run, index = self._schedule.pop()
                    when_next = next_run - now
                    if when_next <= 0:
                        # Run & schedule its next execution.
                        cb, args, kwargs = self._callables[index]
                        fut = executor.submit(cb, *args, **kwargs)
                        # Note that we are providing the time that it started
                        # at, so this implies that the rescheduling time will
                        # *not* take into account how long it took to actually
                        # run the callback... (for better or worse).
                        fut.add_done_callback(functools.partial(_on_done,
                                                                'periodic',
                                                                cb, index,
                                                                now=now))
                    else:
                        # Gotta wait...
                        self._schedule.push(next_run, index)
                        when_next = min(when_next, self.MAX_LOOP_IDLE)
                        self._waiter.wait(when_next)

    def start(self):
        """Starts running (will not return until :py:meth:`.stop` is called).

        NOTE(harlowja): If this worker has no contained callables this raises
        a runtime error and does not run since it is impossible to periodically
        run nothing.
        """
        if not self._callables:
            raise RuntimeError("A periodic worker can not start"
                               " without any callables")
        executor = self._executor_factory()
        self._dead.clear()
        try:
            self._run(executor)
        finally:
            executor.shutdown()
            self._dead.set()
            if hasattr(executor, 'statistics'):
                self._log.debug("Stopped running periodically: %s",
                                executor.statistics)

    def stop(self):
        """Sets the tombstone (this stops any further executions)."""
        with self._waiter:
            self._tombstone.set()
            self._waiter.notify_all()

    def reset(self):
        """Resets the workers internal state."""
        self._tombstone.clear()
        self._dead.clear()
        self._immediates, self._schedule = _build(self._callables)

    def wait(self, timeout=None):
        """Waits for the :py:func:`.start` method to gracefully exit.

        An optional timeout can be provided, which will cause the method to
        return within the specified timeout. If the timeout is reached, the
        returned value will be False.

        :param timeout: Maximum number of seconds that the :meth:`.wait`
                        method should block for
        :type timeout: float/int
        """
        self._dead.wait(timeout)
        return self._dead.is_set()
