#    Copyright (C) 2014 Yahoo! Inc. All Rights Reserved.
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

import abc
from collections.abc import Callable
import functools
import heapq
import logging
import multiprocessing.context
import queue
import threading
import time
from typing import Any, Generic, ParamSpec, Self, TypeVar

from concurrent import futures as _futures
from concurrent.futures import process as _process

from debtcollector import removals

from futurist import _green
from futurist import _thread
from futurist import _utils

_P = ParamSpec('_P')
_R = TypeVar('_R')
T = TypeVar('T')

LOG = logging.getLogger(__name__)

TimeoutError = _futures.TimeoutError
CancelledError = _futures.CancelledError


class RejectedSubmission(Exception):
    """Exception raised when a submitted call is rejected (for some reason)."""


# NOTE(harlowja): Allows for simpler access to this type...
Future = _futures.Future


class _Gatherer:
    def __init__(
        self,
        submit_func: Callable[..., _futures.Future[Any]],
        lock_factory: Callable[[], threading.Lock],
        start_before_submit: bool = False,
    ) -> None:
        self._submit_func = submit_func
        self._stats_lock = lock_factory()
        self._stats = ExecutorStatistics()
        self._start_before_submit = start_before_submit

    @property
    def statistics(self) -> ExecutorStatistics:
        return self._stats

    def clear(self) -> None:
        with self._stats_lock:
            self._stats = ExecutorStatistics()

    def _capture_stats(
        self, started_at: float, fut: _futures.Future[Any]
    ) -> None:
        """Capture statistics

        :param started_at: when the activity the future has performed
                           was started at
        :param fut: future object
        """
        # If time somehow goes backwards, make sure we cap it at 0.0 instead
        # of having negative elapsed time...
        elapsed = max(0.0, _utils.now() - started_at)
        with self._stats_lock:
            # Use a new collection and lock so that all mutations are seen as
            # atomic and not overlapping and corrupting with other
            # mutations (the clone ensures that others reading the current
            # values will not see a mutated/corrupted one). Since futures may
            # be completed by different threads we need to be extra careful to
            # gather this data in a way that is thread-safe...
            (failures, executed, runtime, cancelled) = (
                self._stats.failures,
                self._stats.executed,
                self._stats.runtime,
                self._stats.cancelled,
            )
            if fut.cancelled():
                cancelled += 1
            else:
                executed += 1
                if fut.exception() is not None:
                    failures += 1
                runtime += elapsed
            self._stats = ExecutorStatistics(
                failures=failures,
                executed=executed,
                runtime=runtime,
                cancelled=cancelled,
            )

    def submit(
        self, fn: Callable[_P, _R], *args: _P.args, **kwargs: _P.kwargs
    ) -> _futures.Future[_R]:
        """Submit work to be executed and capture statistics."""
        if self._start_before_submit:
            started_at = _utils.now()
        fut = self._submit_func(fn, *args, **kwargs)
        if not self._start_before_submit:
            started_at = _utils.now()
        fut.add_done_callback(
            functools.partial(self._capture_stats, started_at)
        )
        return fut


class ThreadPoolExecutor(_futures.Executor):
    """Executor that uses a thread pool to execute calls asynchronously.

    It gathers statistics about the submissions executed for post-analysis...

    Note that this executor never shrinks its thread pool, which will cause
    the pool to eventually reach its maximum capacity defined by max_workers.
    Check :py:class:`DynamicThreadPoolExecutor` for an alternative.

    See: https://docs.python.org/dev/library/concurrent.futures.html
    """

    threading = _thread.Threading()

    def __init__(
        self,
        max_workers: int | None = None,
        check_and_reject: Callable[[Self, int], None] | None = None,
    ) -> None:
        """Initializes a thread pool executor.

        :param max_workers: maximum number of workers that can be
                            simultaneously active at the same time, further
                            submitted work will be queued up when this limit
                            is reached.
        :type max_workers: int
        :param check_and_reject: a callback function that will be provided
                                 two position arguments, the first argument
                                 will be this executor instance, and the second
                                 will be the number of currently queued work
                                 items in this executors backlog; the callback
                                 should raise a :py:class:`.RejectedSubmission`
                                 exception if it wants to have this submission
                                 rejected.
        :type check_and_reject: callback
        """
        if max_workers is None:
            max_workers = _utils.get_optimal_thread_count()

        if max_workers <= 0:
            raise ValueError("max_workers must be greater than zero")

        self._max_workers = max_workers
        self._work_queue: queue.Queue[_utils.WorkItem] = queue.Queue()
        self._shutdown_lock = threading.RLock()
        self._shutdown = False
        self._workers: list[_thread.ThreadWorker] = []
        self._check_and_reject = check_and_reject or (lambda e, waiting: None)
        self._gatherer = _Gatherer(self._submit, self.threading.lock_object)

    @property
    def statistics(self) -> ExecutorStatistics:
        """:class:`.ExecutorStatistics` about the executors executions."""
        return self._gatherer.statistics

    @property
    def alive(self) -> bool:
        """Accessor to determine if the executor is alive/active."""
        return not self._shutdown

    @property
    def queue_size(self) -> int:
        """The current size of the queue.

        This value represents the number of tasks that are waiting for a free
        worker thread.
        """
        return self._work_queue.qsize()

    @property
    def num_workers(self) -> int:
        """The current number of worker threads."""
        return len(self._workers)

    def get_num_idle_workers(self) -> int:
        """Get the number of currently idle threads.

        A thread is idle if it's waiting for new tasks from the queue.

        This method is required to obtain the shutdown lock it provides
        an accurate count.
        """
        with self._shutdown_lock:
            return sum(1 for w in self._workers if w.idle)

    def _add_thread(self) -> None:
        w = _thread.ThreadWorker.create_and_register(self, self._work_queue)
        # Always save it before we start (so that even if we fail
        # starting it we can correctly join on it).
        self._workers.append(w)
        w.start()

    def _maybe_spin_up(self) -> bool:
        """Spin up a worker if needed."""
        if not self._workers or len(self._workers) < self._max_workers:
            self._add_thread()
            return True
        return False

    def shutdown(
        self, wait: bool = True, *, cancel_futures: bool = False
    ) -> None:
        with self._shutdown_lock:
            if not self._shutdown:
                self._shutdown = True
                if wait:
                    # Wait for all queued work to complete using queue.join()
                    # This will block until all work items have been processed
                    # and task_done() has been called for each
                    self._work_queue.join()
                for w in self._workers:
                    w.stop()
        if wait:
            for w in self._workers:
                w.join()

    def _submit(
        self, fn: Callable[_P, _R], *args: _P.args, **kwargs: _P.kwargs
    ) -> Future[_R]:
        f: Future[_R] = Future()
        self._maybe_spin_up()
        self._work_queue.put(_utils.WorkItem(f, fn, *args, **kwargs))
        return f

    def submit(  # type: ignore[override]
        self, fn: Callable[_P, _R], *args: _P.args, **kwargs: _P.kwargs
    ) -> Future[_R]:
        """Submit some work to be executed (and gather statistics)."""
        # NOTE(dtantsur): DynamicThreadPoolExecutor relies on this lock for
        # its complex logic around thread management. If you ever decide to
        # remove it, please add a lock there instead.
        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError(
                    'Can not schedule new futures after being shutdown'
                )
            self._check_and_reject(self, self._work_queue.qsize())  # type: ignore[arg-type]
            return self._gatherer.submit(fn, *args, **kwargs)


class DynamicThreadPoolExecutor(ThreadPoolExecutor):
    """Executor that creates or removes threads on demand.

    As new work is scheduled on the executor, it will try to keep the
    proportion of busy threads within the provided range (between 40% and 80%
    by default). A busy thread is a thread that is not waiting on the
    task queue.

    Each time a task is submitted, the executor makes a decision whether to
    grow or shrink the pool. It takes the proportion of the number of busy
    threads to the total number of threads and compares it to shrink_threshold
    and grow_threshold.

    Initially, the pool is empty, so submitting a task always result in one
    new thread. Since min_workers must be greater than zero, at least one
    thread will always be available after this point.

    Once the proportion of busy threads reaches grow_threshold (e.g. 4 out of 5
    with the default grow_threshold of 0.8), a new thread is created when a
    task is submitted. If on submitting a task a proportion of busy threads is
    below shrink_threshold (e.g. only 2 out of 5), one idle thread is stopped.

    The values of grow_threshold and shrink_threshold are different to prevent
    the number of threads from oscilating on reaching grow_threshold.

    If threads are not created often in your application, the number of idle
    threads may stay high for a long time. To avoid it, you can call
    :py:meth:`.maintain` periodically to keep the number of threads within
    the thresholds.

    """

    def __init__(
        self,
        max_workers: int | None = None,
        check_and_reject: Callable[[Self, int], None] | None = None,
        min_workers: int = 1,
        grow_threshold: float = 0.8,
        shrink_threshold: float = 0.4,
    ) -> None:
        """Initializes a thread pool executor.

        :param max_workers: maximum number of workers that can be
                            simultaneously active at the same time, further
                            submitted work will be queued up when this limit
                            is reached.
        :type max_workers: int
        :param check_and_reject: a callback function that will be provided
                                 two position arguments, the first argument
                                 will be this executor instance, and the second
                                 will be the number of currently queued work
                                 items in this executors backlog; the callback
                                 should raise a :py:class:`.RejectedSubmission`
                                 exception if it wants to have this submission
                                 rejected.
        :type check_and_reject: callback
        :param min_workers: the minimum number of workers that can be reached
                            when shrinking the pool. Note that the pool always
                            starts at zero workers and will be smaller than
                            min_workers until enough workers are created.
                            At least one thread is required.
        :type max_workers: int
        :param grow_threshold: minimum proportion of busy threads to total
                               threads for the pool to grow.
        :type grow_threshold: float
        :param shrink_threshold: maximum proportion of busy threads to total
                                 threads for the pool to shrink.
        :type shrink_threshold: float
        """
        super().__init__(
            max_workers=max_workers, check_and_reject=check_and_reject
        )
        if min_workers <= 0:
            raise ValueError('min_workers must be greater than zero')
        if max_workers and min_workers >= max_workers:
            raise ValueError('min_workers must be less than max_workers')
        self._min_workers = min_workers

        if grow_threshold <= 0 or grow_threshold > 1.0:
            raise ValueError('grow_threshold must be within (0, 1]')
        if shrink_threshold < 0 or shrink_threshold >= 1.0:
            raise ValueError('shrink_threshold must be within [0, 1)')
        if shrink_threshold >= grow_threshold:
            raise ValueError(
                'shrink_threshold must be less than grow_threshold'
            )
        self._grow_threshold = grow_threshold
        self._shrink_threshold = shrink_threshold

        self._dead_workers: list[_thread.ThreadWorker] = []

    def _drop_thread(self) -> bool:
        new_workers: list[_thread.ThreadWorker] = []
        idle_worker: _thread.ThreadWorker | None = None
        for i, w in enumerate(self._workers):
            if w.idle:
                new_workers = self._workers[i + 1 :]
                idle_worker = w
                break
            new_workers.append(w)

        if idle_worker is None:
            # Should not actually happen but races are possible; do nothing
            LOG.warning(
                'No idle worker thread to delete when shrinking pool %r', self
            )
            return False

        idle_worker.stop()
        self._workers = new_workers
        self._dead_workers.append(idle_worker)
        return True

    def _maybe_spin_up(self) -> bool:
        nthreads = self.num_workers
        if nthreads < self._min_workers:
            self._add_thread()
            return True

        # NOTE(dtantsur): here we count the number of threads that are
        # doing something (i.e. are not waiting on the queue) plus the
        # number of tasks in the queue. In theory, if there are idle
        # workers, the queue should be empty. But race conditions are
        # possible when workers do not pick up tasks quickly enough,
        # especially in the presence of CPU-bound tasks.
        idle = self.get_num_idle_workers()
        busy = (nthreads - idle + self.queue_size) / nthreads
        if busy >= self._grow_threshold and nthreads < self._max_workers:
            LOG.debug(
                'Creating a new worker thread for pool %r '
                '(%d thread(s) idle, queue size %d, total %d thread(s))',
                self,
                idle,
                self.queue_size,
                nthreads,
            )
            self._add_thread()
            return True
        elif busy <= self._shrink_threshold and nthreads > self._min_workers:
            LOG.debug(
                'Deleting a worker thread from pool %r '
                '(%d thread(s) idle, queue size %d, total %d thread(s))',
                self,
                idle,
                self.queue_size,
                nthreads,
            )
            return self._drop_thread()

        return False

    def maintain(self) -> None:
        """Keep the number of threads within the expected range.

        If too many idle threads are running, they are deleted.
        Additionally, deleted workers are joined to free up resources.
        """
        # NOTE(dtantsur): this call can potentially run for some time, so
        # avoid taking shutdown_lock once and holding it for the entire
        # duration (blocking any new tasks from being added).
        keep_going = True
        while keep_going:
            if self._shutdown:
                return

            with self._shutdown_lock:
                keep_going = self._maybe_spin_up()

            time.sleep(0)

        # NOTE(dtantsur): copy the value of _dead_workers to prevent races with
        # other invocations for maintain or shutdown.
        with self._shutdown_lock:
            dead_workers = self._dead_workers
            self._dead_workers = []

        for w in dead_workers:
            w.join()

    def shutdown(
        self, wait: bool = True, *, cancel_futures: bool = False
    ) -> None:
        super().shutdown(wait=wait, cancel_futures=cancel_futures)
        if wait:
            for w in self._dead_workers:
                w.join()


class ProcessPoolExecutor(_process.ProcessPoolExecutor):
    """Executor that uses a process pool to execute calls asynchronously.

    It gathers statistics about the submissions executed for post-analysis...

    See: https://docs.python.org/dev/library/concurrent.futures.html
    """

    threading = _thread.Threading()

    def __init__(
        self,
        max_workers: int | None = None,
        mp_context: multiprocessing.context.BaseContext | None = None,
    ) -> None:
        if max_workers is None:
            max_workers = _utils.get_optimal_process_count()
        if max_workers <= 0:
            raise ValueError("Max workers must be greater than zero")
        super().__init__(max_workers=max_workers, mp_context=mp_context)
        self._gatherer = _Gatherer(
            # Since our submit will use this gatherer we have to reference
            # the parent submit, bound to this instance (which is what we
            # really want to use anyway).
            super().submit,
            self.threading.lock_object,
        )

    @property
    def alive(self) -> bool:
        """Accessor to determine if the executor is alive/active."""
        return not self._shutdown_thread

    @property
    def statistics(self) -> ExecutorStatistics:
        """:class:`.ExecutorStatistics` about the executors executions."""
        return self._gatherer.statistics

    def submit(  # type: ignore[override]
        self, fn: Callable[_P, _R], *args: _P.args, **kwargs: _P.kwargs
    ) -> Future[_R]:
        """Submit some work to be executed (and gather statistics)."""
        return self._gatherer.submit(fn, *args, **kwargs)


class SynchronousExecutor(_futures.Executor):
    """Executor that uses the caller to execute calls synchronously.

    This provides an interface to a caller that looks like an executor but
    will execute the calls inside the caller thread instead of executing it
    in a external process/thread for when this type of functionality is
    useful to provide...

    It gathers statistics about the submissions executed for post-analysis...
    """

    threading = _thread.Threading()

    @removals.removed_kwarg(
        'green',
        message="Eventlet support is deprecated. "
        "Please migrate your code and stop enforcing "
        "its usage.",
    )
    def __init__(
        self,
        green: bool = False,
        run_work_func: Callable[[_utils.WorkItem], None] = lambda work: (
            work.run()
        ),
    ) -> None:
        """Synchronous executor constructor.

        :param green: when enabled this forces the usage of greened lock
                      classes and green futures (so that the internals of this
                      object operate correctly under eventlet)
        :type green: bool
        :param run_work_func: callable that takes a single work item and
                              runs it (typically in a blocking manner)
        :param run_work_func: callable
        """
        if green and not _utils.EVENTLET_AVAILABLE:
            raise RuntimeError(
                'Eventlet is needed to use a green synchronous executor'
            )
        if not callable(run_work_func):
            raise ValueError("Run work parameter expected to be callable")
        self._run_work_func = run_work_func
        self._shutoff = False
        if green:
            self.threading = _green.threading  # type: ignore[assignment]
            self._future_cls: type[Future[Any]] = GreenFuture
        else:
            self._future_cls = Future
        self._run_work_func = run_work_func
        self._gatherer = _Gatherer(
            self._submit,
            self.threading.lock_object,
            start_before_submit=True,
        )

    @property
    def alive(self) -> bool:
        """Accessor to determine if the executor is alive/active."""
        return not self._shutoff

    def shutdown(
        self, wait: bool = True, *, cancel_futures: bool = False
    ) -> None:
        self._shutoff = True

    def restart(self) -> None:
        """Restarts this executor (*iff* previously shutoff/shutdown).

        NOTE(harlowja): clears any previously gathered statistics.
        """
        if self._shutoff:
            self._shutoff = False
            self._gatherer.clear()

    @property
    def statistics(self) -> ExecutorStatistics:
        """:class:`.ExecutorStatistics` about the executors executions."""
        return self._gatherer.statistics

    def submit(  # type: ignore[override]
        self, fn: Callable[_P, _R], *args: _P.args, **kwargs: _P.kwargs
    ) -> Future[_R]:
        """Submit some work to be executed (and gather statistics)."""
        if self._shutoff:
            raise RuntimeError(
                'Can not schedule new futures after being shutdown'
            )
        return self._gatherer.submit(fn, *args, **kwargs)

    def _submit(
        self, fn: Callable[_P, _R], *args: _P.args, **kwargs: _P.kwargs
    ) -> Future[_R]:
        fut: Future[_R] = self._future_cls()
        self._run_work_func(_utils.WorkItem(fut, fn, *args, **kwargs))
        return fut


@removals.removed_class(
    "GreenFuture",
    message="Eventlet support is deprecated. "
    "Please migrate your code and stop using Green "
    "future.",
)
class GreenFuture(Future[Any]):
    __doc__ = Future.__doc__

    def __init__(self) -> None:
        super().__init__()
        if not _utils.EVENTLET_AVAILABLE:
            raise RuntimeError('Eventlet is needed to use a green future')
        # NOTE(harlowja): replace the built-in condition with a greenthread
        # compatible one so that when getting the result of this future the
        # functions will correctly yield to eventlet. If this is not done then
        # waiting on the future never actually causes the greenthreads to run
        # and thus you wait for infinity.
        if not _green.is_monkey_patched('thread'):
            assert _green.threading is not None
            self._condition = _green.threading.condition_object()


@removals.removed_class(
    "GreenThreadPoolExecutor",
    message="Eventlet support is deprecated. "
    "Please migrate your code and stop using Green "
    "executor.",
)
class GreenThreadPoolExecutor(_futures.Executor):
    """Executor that uses a green thread pool to execute calls asynchronously.

    See: https://docs.python.org/dev/library/concurrent.futures.html
    and http://eventlet.net/doc/modules/greenpool.html for information on
    how this works.

    It gathers statistics about the submissions executed for post-analysis...
    """

    threading = _green.threading

    def __init__(
        self,
        max_workers: int = 1000,
        check_and_reject: Callable[[Self, int], None] | None = None,
    ) -> None:
        """Initializes a green thread pool executor.

        :param max_workers: maximum number of workers that can be
                            simulatenously active at the same time, further
                            submitted work will be queued up when this limit
                            is reached.
        :type max_workers: int
        :param check_and_reject: a callback function that will be provided
                                 two position arguments, the first argument
                                 will be this executor instance, and the second
                                 will be the number of currently queued work
                                 items in this executors backlog; the callback
                                 should raise a :py:class:`.RejectedSubmission`
                                 exception if it wants to have this submission
                                 rejected.
        :type check_and_reject: callback
        """
        if not _utils.EVENTLET_AVAILABLE:
            raise RuntimeError('Eventlet is needed to use a green executor')
        if max_workers <= 0:
            raise ValueError("Max workers must be greater than zero")
        self._max_workers = max_workers
        self._pool: Any = _green.Pool(self._max_workers)
        self._delayed_work: Any = _green.Queue()
        self._check_and_reject = check_and_reject or (lambda e, waiting: None)
        assert self.threading is not None
        self._shutdown_lock: Any = self.threading.lock_object()
        self._shutdown = False
        self._gatherer = _Gatherer(self._submit, self.threading.lock_object)

    @property
    def alive(self) -> bool:
        """Accessor to determine if the executor is alive/active."""
        return not self._shutdown

    @property
    def statistics(self) -> ExecutorStatistics:
        """:class:`.ExecutorStatistics` about the executors executions."""
        return self._gatherer.statistics

    def submit(  # type: ignore[override]
        self, fn: Callable[_P, _R], *args: _P.args, **kwargs: _P.kwargs
    ) -> Future[_R]:
        """Submit some work to be executed (and gather statistics).

        :param args: non-keyworded arguments
        :type args: list
        :param kwargs: key-value arguments
        :type kwargs: dictionary
        """
        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError(
                    'Can not schedule new futures after being shutdown'
                )
            self._check_and_reject(self, self._delayed_work.qsize())  # type: ignore[arg-type]
            return self._gatherer.submit(fn, *args, **kwargs)

    def _submit(
        self, fn: Callable[_P, _R], *args: _P.args, **kwargs: _P.kwargs
    ) -> Future[_R]:
        f: Future[_R] = GreenFuture()
        work = _utils.WorkItem(f, fn, *args, **kwargs)
        if not self._spin_up(work):
            self._delayed_work.put(work)
        return f

    def _spin_up(self, work: _utils.WorkItem) -> bool:
        """Spin up a greenworker if less than max_workers.

        :param work: work to be given to the greenworker
        :returns: whether a green worker was spun up or not
        :rtype: boolean
        """
        alive = self._pool.running() + self._pool.waiting()
        if alive < self._max_workers:
            self._pool.spawn_n(_green.GreenWorker(work, self._delayed_work))
            return True
        return False

    def shutdown(
        self, wait: bool = True, *, cancel_futures: bool = False
    ) -> None:
        with self._shutdown_lock:
            if not self._shutdown:
                self._shutdown = True
                shutoff = True
            else:
                shutoff = False
        if wait and shutoff:
            self._delayed_work.join()
            self._pool.waitall()


class ExecutorStatistics:
    """Holds *immutable* information about a executors executions."""

    __slots__ = ['_failures', '_executed', '_runtime', '_cancelled']

    _REPR_MSG_TPL = (
        "<ExecutorStatistics object at 0x%(ident)x"
        " (failures=%(failures)s,"
        " executed=%(executed)s, runtime=%(runtime)0.2f,"
        " cancelled=%(cancelled)s)>"
    )

    def __init__(
        self,
        failures: int = 0,
        executed: int = 0,
        runtime: float = 0.0,
        cancelled: int = 0,
    ) -> None:
        self._failures = failures
        self._executed = executed
        self._runtime = runtime
        self._cancelled = cancelled

    @property
    def failures(self) -> int:
        """How many submissions ended up raising exceptions.

        :returns: how many submissions ended up raising exceptions
        :rtype: number
        """
        return self._failures

    @property
    def executed(self) -> int:
        """How many submissions were executed (failed or not).

        :returns: how many submissions were executed
        :rtype: number
        """
        return self._executed

    @property
    def runtime(self) -> float:
        """Total runtime of all submissions executed (failed or not).

        :returns: total runtime of all submissions executed
        :rtype: number
        """
        return self._runtime

    @property
    def cancelled(self) -> int:
        """How many submissions were cancelled before executing.

        :returns: how many submissions were cancelled before executing
        :rtype: number
        """
        return self._cancelled

    @property
    def average_runtime(self) -> float:
        """The average runtime of all submissions executed.

        :returns: average runtime of all submissions executed
        :rtype: number
        :raises: ZeroDivisionError when no executions have occurred.
        """
        return self._runtime / self._executed

    def __repr__(self) -> str:
        return self._REPR_MSG_TPL % (
            {
                'ident': id(self),
                'failures': self._failures,
                'executed': self._executed,
                'runtime': self._runtime,
                'cancelled': self._cancelled,
            }
        )


class DelayedExecutorMixinBase(_futures.Executor, abc.ABC, Generic[T]):
    """Mixin that adds submit_after(delay, fn, ...) to any Executor subclass.

    Maintains a single background scheduler thread that keeps a min-heap of
    pending tasks ordered by deadline.  When a task's deadline arrives the
    scheduler submits it to the underlying executor.  Only one extra thread is
    required regardless of how many delayed tasks are pending, making this
    suitable for use with native threads where thread creation is expensive.

    Concrete subclasses must implement the factory / threading primitives:
    ``_get_condition_object``, ``_get_future_object``, ``_start_thread``,
    ``_join_thread``, and ``_is_scheduler_alive``.

    .. note::

        When combining this mixin with
        :class:`~futurist.ProcessPoolExecutor`, you **must** use the
        ``spawn`` multiprocessing start method (e.g.
        ``mp_context=multiprocessing.get_context('spawn')``).  The
        scheduler runs a background thread in the parent process; using
        the ``fork`` or ``forkserver`` start methods can leave
        threading primitives (conditions, locks) in a corrupted state in
        child worker processes, leading to hangs or deadlocks.
    """

    class Task(Generic[_R]):
        """A single delayed task entry in the scheduler priority queue.

        Instances are ordered by ``deadline`` so that the min-heap in
        :class:`DelayedExecutorMixinBase` always pops the task whose
        deadline is closest.
        """

        def __init__(
            self,
            fn: Callable[..., _R],
            args: tuple[Any, ...],
            kwargs: dict[str, Any],
            future: Future[_R],
            delay: float,
        ) -> None:
            self.fn = fn
            self.args = args
            self.kwargs = kwargs
            self.future = future
            self.deadline = time.monotonic() + delay

        @property
        def _remaining_delay(self) -> float:
            return self.deadline - time.monotonic()

        def __repr__(self) -> str:
            return (
                f"Task={self.fn}, "
                f"remaining_delay={self._remaining_delay}, "
                f"future={self.future}"
            )

        def __lt__(self, other: object) -> bool:
            if not isinstance(other, DelayedExecutorMixinBase.Task):
                return NotImplemented
            return self.deadline < other.deadline

    class _SentinelTask(Task[Any]):
        """Sentinel pushed to the queue to signal the scheduler to exit.

        Inherits :class:`Task` so the scheduler queue can be typed as
        ``list[Task]`` and ordering falls out naturally from deadline
        comparison.  Setting ``deadline`` to ``float('inf')`` guarantees
        the sentinel always sorts after every real task, so all pending
        work is drained before the scheduler thread notices the shutdown
        signal.
        """

        def __init__(self) -> None:
            self.deadline = float("inf")

        def __repr__(self) -> str:
            return "SentinelTask"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self._queue_changed = self._get_condition_object()
        # A min-heap of Task instances (including the _SentinelTask
        # sentinel, which inherits Task and always sorts last).
        self._queue: list[DelayedExecutorMixinBase.Task[Any]] = []

        self._sentinel = self._SentinelTask()

        self._shutdown_requested = False
        self._cancel_futures_on_shutdown = False

        self._scheduler = self._start_thread(self._schedule)

    def _task_wrapper(self, task: Task[Any]) -> None:
        try:
            task.future.set_result(task.fn(*task.args, **task.kwargs))
        except BaseException as e:
            task.future.set_exception(e)

    def _schedule(self) -> None:
        """Run the scheduler, submitting delayed tasks as they become due."""
        while True:
            task_to_submit: DelayedExecutorMixinBase.Task[Any] | None = None
            with self._queue_changed:
                LOG.debug("Waiting for task")
                self._queue_changed.wait_for(lambda: bool(self._queue))
                task: DelayedExecutorMixinBase.Task[Any] = heapq.heappop(
                    self._queue
                )
                LOG.debug("%s received", task)

                if task is self._sentinel:
                    # We are being shutdown. As the sentinel always sorts
                    # after every real task we know all pending tasks have
                    # been submitted to the inner executor.  Shut down the
                    # inner executor now so resources are freed whether or
                    # not the caller passes wait=True.
                    LOG.debug("Sentinel task received, exiting scheduler")
                    super().shutdown(
                        wait=False,
                        cancel_futures=self._cancel_futures_on_shutdown,
                    )
                    break

                # task is now the one with the closest deadline
                if task.future.cancelled():
                    # task is cancelled, just start over
                    LOG.debug("%s was cancelled while in the queue", task)
                    continue

                # wait for the deadline or an outside trigger
                LOG.debug("Waiting for the deadline of %s", task)
                changed = self._queue_changed.wait(task._remaining_delay)
                LOG.debug(
                    "Awaken while waiting on %s, queue changed=%s",
                    task,
                    changed,
                )

                if changed:
                    # Something changed before the deadline.
                    if task.future.cancelled():
                        # nothing to do with this task just start over
                        LOG.debug("%s is cancelled, skipping", task)
                        continue
                    else:
                        # Reinsert the task to the queue and start over; this
                        # allows handling a newer task with a closer deadline.
                        heapq.heappush(self._queue, task)
                        LOG.debug("Queue changed, re-scheduling %s", task)
                        continue
                else:
                    # We hit the deadline of the task so if it is not
                    # cancelled then we can execute it.
                    run_it = task.future.set_running_or_notify_cancel()
                    if not run_it:
                        # Task is cancelled just start over
                        LOG.debug("%s is cancelled, skipping", task)
                        continue
                    # Mark for submission outside the lock to avoid holding
                    # the condition while dispatching to the inner executor.
                    task_to_submit = task

            # Submit outside the lock to avoid holding the condition
            # longer than needed and to let concurrent submit_after
            # calls proceed without waiting for this dispatch.
            if task_to_submit is not None:
                try:
                    self.submit(self._task_wrapper, task_to_submit)
                    LOG.debug("Task submitted %s", task_to_submit)
                except BaseException as e:
                    LOG.debug("Failed to submit %s: %s", task_to_submit, e)
                    task_to_submit.future.set_exception(e)

        LOG.debug("Scheduler thread finished")

    def submit_after(
        self,
        delay: float,
        fn: Callable[_P, _R],
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _futures.Future[_R]:
        """Schedule *fn* to run after *delay* seconds.

        :param delay: Number of seconds to wait before executing *fn*.
            Must be a non-negative value.
        :param fn: Callable to execute after the delay.
        :param args: Positional arguments forwarded to *fn*.
        :param kwargs: Keyword arguments forwarded to *fn*.
        :returns: A :class:`~concurrent.futures.Future` that will be
            resolved with the return value of *fn*, or with an exception
            if *fn* raises.  The future can be cancelled while the task
            is still waiting for its deadline.
        :raises RuntimeError: If the executor has already been shut down.
        :raises ValueError: If *delay* is negative.
        """
        if delay < 0:
            raise ValueError(
                f"delay must be a non-negative number, got {delay!r}"
            )
        with self._queue_changed:
            if self._shutdown_requested:
                raise RuntimeError(
                    "Cannot schedule new futures after being shutdown"
                )

            task = self.Task(
                fn, args, kwargs, self._get_future_object(), delay
            )
            heapq.heappush(self._queue, task)
            self._queue_changed.notify_all()
            LOG.debug("%s is queued", task)
            return task.future

    def shutdown(
        self, wait: bool = True, *, cancel_futures: bool = False
    ) -> None:
        """Shutdown the executor.

        :param wait: If ``True`` (the default) block until the scheduler
            thread and all submitted work have completed.
        :param cancel_futures: If ``True``, cancel any delayed tasks that
            are still waiting for their deadline before shutting down.
        """
        LOG.debug("Shutdown requested")
        with self._queue_changed:
            self._cancel_futures_on_shutdown = cancel_futures
            if cancel_futures:
                for item in self._queue:
                    if item is not self._sentinel:
                        item.future.cancel()
            if not self._shutdown_requested:
                # Ensure that our thread wakes at least one more time to allow
                # it to exit by queuing up a sentinel task after the shutdown
                # condition is set. This task won't be executed.
                heapq.heappush(self._queue, self._sentinel)
                # We only want to queue 1 sentinel even if multiple shutdown
                # calls happen.
                self._shutdown_requested = True
                self._queue_changed.notify_all()
                LOG.debug("Shutdown signalled to the scheduler thread")

        # If wait is set we need to wait for our sentinel to be processed and
        # therefore our thread to exit.  The scheduler thread shuts down the
        # inner executor when it processes the sentinel; calling
        # super().shutdown(wait=True) here then waits for any still-running
        # inner-executor workers to finish.
        if wait:
            LOG.debug("Waiting for the scheduler thread to finish")
            self._join_thread(self._scheduler)
            LOG.debug("Waiting for the internal executor to finish")
            super().shutdown(wait=wait, cancel_futures=cancel_futures)
        # With wait=False we return immediately.  The scheduler thread is
        # still running, will submit any remaining delayed tasks to the
        # inner executor, and will then shut down the inner executor itself.

    @property
    def alive(self) -> bool:
        """``True`` while the scheduler or inner executor is still active.
        Returns ``False`` only once both have fully stopped.  The
        ``or super().alive`` term is defensive: in normal operation the
        scheduler calls ``super().shutdown()`` before exiting so
        ``super().alive`` is already ``False`` when the scheduler dies, but
        if the scheduler exits abnormally without that call, the inner
        executor may still be running and this ensures we report it.

        Note: ``alive`` is not defined on the stdlib
        :class:`~concurrent.futures.Executor` base class, but every futurist
        executor exposes it.  This mixin is designed to be combined with a
        futurist executor, so ``super().alive`` will always resolve correctly
        at runtime even though the static type checker cannot verify it.
        """
        return self._is_scheduler_alive() or super().alive  # type: ignore[misc]

    @abc.abstractmethod
    def _get_condition_object(self) -> threading.Condition:
        raise NotImplementedError

    @abc.abstractmethod
    def _get_future_object(self) -> Future[Any]:
        raise NotImplementedError

    @abc.abstractmethod
    def _start_thread(self, fn: Callable[[], None]) -> T:
        raise NotImplementedError

    @abc.abstractmethod
    def _join_thread(self, t: T) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def _is_scheduler_alive(self) -> bool:
        raise NotImplementedError


class GreenDelayedExecutorMixin(DelayedExecutorMixinBase[Any]):
    """Variant of :class:`DelayedExecutorMixinBase` that uses eventlet
    primitives.

    This allows the mixin to work correctly in environments where eventlet is
    present but the standard library has *not* been monkey-patched: all
    threading primitives (conditions, threads, futures) are substituted with
    their eventlet equivalents so that the scheduler integrates properly with
    the eventlet event loop.

    Example usage::

        class DelayedGreenExecutor(
            GreenDelayedExecutorMixin, GreenThreadPoolExecutor
        ):
            pass


        executor = DelayedGreenExecutor(max_workers=4)
        fut = executor.submit_after(0.5, my_func, arg1, kwarg=val)
        result = fut.result()
        executor.shutdown()
    """

    @staticmethod
    def _get_condition_object() -> threading.Condition:
        if _green.threading is None:
            raise RuntimeError(
                "GreenDelayedExecutorMixin requires eventlet to be installed"
            )
        return _green.threading.condition_object()

    @staticmethod
    def _get_future_object() -> GreenFuture:
        return GreenFuture()

    @staticmethod
    def _start_thread(fn: Callable[[], None]) -> Any:
        if _green.spawn is None:
            raise RuntimeError(
                "GreenDelayedExecutorMixin requires eventlet to be installed"
            )
        return _green.spawn(fn)

    @staticmethod
    def _join_thread(t: Any) -> None:
        t.wait()

    def _is_scheduler_alive(self) -> bool:
        # GreenThread exposes a ``dead`` attribute rather than ``is_alive()``.
        return not self._scheduler.dead


class DelayedExecutorMixin(DelayedExecutorMixinBase[threading.Thread]):
    """Variant of :class:`DelayedExecutorMixinBase` that uses native threads.

    Use this class together with any standard-library
    :class:`~concurrent.futures.Executor` subclass (e.g.
    :class:`~futurist.ThreadPoolExecutor`) to add
    :meth:`~DelayedExecutorMixinBase.submit_after` support backed by a single
    daemon scheduler thread.

    Example usage::

        class DelayedThreadExecutor(DelayedExecutorMixin, ThreadPoolExecutor):
            pass


        executor = DelayedThreadExecutor(max_workers=4)
        fut = executor.submit_after(0.5, my_func, arg1, kwarg=val)
        result = fut.result()
        executor.shutdown()
    """

    @staticmethod
    def _get_condition_object() -> threading.Condition:
        return threading.Condition()

    @staticmethod
    def _get_future_object() -> Future[Any]:
        return Future()

    @staticmethod
    def _start_thread(fn: Callable[[], None]) -> threading.Thread:
        t = threading.Thread(
            target=fn,
            name="futurist-delayed-scheduler",
            daemon=True,
        )
        t.start()
        return t

    @staticmethod
    def _join_thread(t: threading.Thread) -> None:
        t.join()

    def _is_scheduler_alive(self) -> bool:
        t: threading.Thread = self._scheduler
        return t.is_alive()
