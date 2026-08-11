# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from concurrent import futures
import multiprocessing
import threading
import time
import unittest
from unittest import mock

from eventlet.green import threading as green_threading
import testscenarios

import futurist
from futurist import rejection
from futurist.tests import base


# Module level functions need to be used since the process pool
# executor can not access instance or lambda level functions (since those
# are not pickleable).


def returns_one():
    return 1


def returns_args(foo, bar):
    return foo + bar


def returns_clock():
    return time.monotonic()


def blows_up():
    raise RuntimeError("no worky")


def delayed(wait_secs):
    time.sleep(wait_secs)


def delayed_with_result(task_id):
    time.sleep(0.1)
    return task_id


class TestExecutors(testscenarios.TestWithScenarios, base.TestCase):
    scenarios = [
        (
            'sync',
            {
                'executor_cls': futurist.SynchronousExecutor,
                'restartable': True,
                'executor_kwargs': {},
            },
        ),
        (
            'green_sync',
            {
                'executor_cls': futurist.SynchronousExecutor,
                'restartable': True,
                'executor_kwargs': {'green': True},
            },
        ),
        (
            'green',
            {
                'executor_cls': futurist.GreenThreadPoolExecutor,
                'restartable': False,
                'executor_kwargs': {},
            },
        ),
        (
            'thread',
            {
                'executor_cls': futurist.ThreadPoolExecutor,
                'restartable': False,
                'executor_kwargs': {},
            },
        ),
        (
            'thread_dyn',
            {
                'executor_cls': futurist.DynamicThreadPoolExecutor,
                'restartable': False,
                'executor_kwargs': {},
            },
        ),
        (
            'process',
            {
                'executor_cls': futurist.ProcessPoolExecutor,
                'restartable': False,
                'executor_kwargs': {
                    'mp_context': multiprocessing.get_context('spawn'),
                },
            },
        ),
    ]

    def setUp(self):
        super().setUp()
        self.executor = self.executor_cls(**self.executor_kwargs)

    def tearDown(self):
        super().tearDown()
        self.executor.shutdown()
        self.executor = None

    def test_run_one(self):
        fut = self.executor.submit(returns_one)
        self.assertEqual(1, fut.result())
        self.assertTrue(fut.done())

    def test_blows_up(self):
        fut = self.executor.submit(blows_up)
        self.assertRaises(RuntimeError, fut.result)
        self.assertIsInstance(fut.exception(), RuntimeError)

    def test_gather_stats(self):
        self.executor.submit(blows_up)
        self.executor.submit(delayed, 0.2)
        self.executor.submit(returns_one)
        self.executor.shutdown()

        self.assertEqual(3, self.executor.statistics.executed)
        self.assertEqual(1, self.executor.statistics.failures)
        self.assertGreaterEqual(
            self.executor.statistics.runtime,
            # It appears that the thread run loop
            # may call this before 0.2 seconds (or 0.2
            # will not be represented as a float correctly)
            # is really up so accommodate for that
            # happening...
            0.199,
        )

    def test_post_shutdown_raises(self):
        executor = self.executor_cls(**self.executor_kwargs)
        executor.shutdown()
        self.assertRaises(RuntimeError, executor.submit, returns_one)

    def test_shutdown_waits_for_all_tasks(self):
        num_tasks = 3
        futures = []
        for i in range(num_tasks):
            future = self.executor.submit(delayed_with_result, i)
            futures.append(future)

        self.executor.shutdown(wait=True)

        results = []
        for future in futures:
            self.assertTrue(future.done())
            results.append(future.result())

        self.assertEqual(len(results), num_tasks)
        self.assertEqual(set(results), set(range(num_tasks)))

    def test_restartable(self):
        if not self.restartable:
            raise unittest.SkipTest("not restartable")
        else:
            executor = self.executor_cls(**self.executor_kwargs)
            fut = executor.submit(returns_one)
            self.assertEqual(1, fut.result())
            executor.shutdown()
            self.assertEqual(1, executor.statistics.executed)

            self.assertRaises(RuntimeError, executor.submit, returns_one)

            executor.restart()
            self.assertEqual(0, executor.statistics.executed)
            fut = executor.submit(returns_one)
            self.assertEqual(1, fut.result())
            self.assertEqual(1, executor.statistics.executed)
            executor.shutdown()

    def test_alive(self):
        with self.executor_cls(**self.executor_kwargs) as executor:
            self.assertTrue(executor.alive)
        self.assertFalse(executor.alive)

    def test_done_callback(self):
        happy_completed = []
        unhappy_completed = []

        def on_done(fut):
            if fut.exception():
                unhappy_completed.append(fut)
            else:
                happy_completed.append(fut)

        for i in range(0, 10):
            if i % 2 == 0:
                fut = self.executor.submit(returns_one)
            else:
                fut = self.executor.submit(blows_up)
            fut.add_done_callback(on_done)

        self.executor.shutdown()
        self.assertEqual(10, len(happy_completed) + len(unhappy_completed))
        self.assertEqual(5, len(unhappy_completed))
        self.assertEqual(5, len(happy_completed))


class TestRejection(testscenarios.TestWithScenarios, base.TestCase):
    rejector = rejection.reject_when_reached(1)

    scenarios = [
        (
            'green',
            {
                'executor_cls': futurist.GreenThreadPoolExecutor,
                'executor_kwargs': {
                    'check_and_reject': rejector,
                    'max_workers': 1,
                },
                'event_cls': green_threading.Event,
            },
        ),
        (
            'thread',
            {
                'executor_cls': futurist.ThreadPoolExecutor,
                'executor_kwargs': {
                    'check_and_reject': rejector,
                    'max_workers': 1,
                },
                'event_cls': threading.Event,
            },
        ),
    ]

    def setUp(self):
        super().setUp()
        self.executor = self.executor_cls(**self.executor_kwargs)
        self.addCleanup(self.executor.shutdown, wait=True)

    def test_rejection(self):
        ev = self.event_cls()
        ev_thread_started = self.event_cls()
        self.addCleanup(ev.set)

        def wait_until_set(check_delay):
            ev_thread_started.set()
            while not ev.is_set():
                ev.wait(check_delay)

        # 1 worker + 1 item of backlog
        self.executor.submit(wait_until_set, 0.1)
        # ensure the above thread has started before doing anything
        # else.
        ev_thread_started.wait()
        self.executor.submit(wait_until_set, 0.1)

        self.assertRaises(
            futurist.RejectedSubmission, self.executor.submit, returns_one
        )


@mock.patch.object(
    futurist.DynamicThreadPoolExecutor,
    '_add_thread',
    # Use the original function behind the scene
    side_effect=futurist.DynamicThreadPoolExecutor._add_thread,
    autospec=True,
)
class TestDynamicThreadPool(base.TestCase):
    def _new(self, *args, **kwargs):
        executor = futurist.DynamicThreadPoolExecutor(*args, **kwargs)
        self.addCleanup(executor.shutdown, wait=True)
        self.assertEqual(0, executor.queue_size)
        self.assertEqual(0, executor.num_workers)
        self.assertEqual(0, executor.get_num_idle_workers())
        self.assertEqual(0, len(executor._dead_workers))
        return executor

    def test_stays_at_min_worker(self, mock_add_thread):
        """Executing tasks sequentially: no growth beyond 1 thread."""
        executor = self._new(max_workers=3)
        for _i in range(10):
            executor.submit(lambda: None).result()
        self.assertEqual(0, executor.queue_size)
        self.assertEqual(1, executor.num_workers)
        self.assertEqual(1, executor.get_num_idle_workers())
        self.assertEqual(0, len(executor._dead_workers))
        self.assertEqual(1, mock_add_thread.call_count)

    def test_grow_and_shrink(self, mock_add_thread):
        """Executing tasks in parallel: grows and shrinks."""
        executor = self._new(max_workers=10)
        started = threading.Barrier(11)
        done = threading.Event()
        tasks = []

        self.addCleanup(started.abort)
        self.addCleanup(done.set)

        def task():
            started.wait()
            done.wait()

        for _i in range(10):
            tasks.append(executor.submit(task))

        started.wait()  # this ensures that all threads have been started
        self.assertEqual(0, executor.queue_size)
        self.assertEqual(10, executor.num_workers)
        self.assertEqual(0, executor.get_num_idle_workers())
        self.assertEqual(0, len(executor._dead_workers))
        self.assertEqual(10, mock_add_thread.call_count)

        done.set()  # this allows all threads to stop
        futures.wait(tasks)
        executor.maintain()
        self.assertEqual(0, executor.queue_size)
        self.assertEqual(1, executor.num_workers)
        self.assertEqual(1, executor.get_num_idle_workers())
        self.assertEqual(0, len(executor._dead_workers))

    def test_shutdown_waits_for_queued_tasks(self, mock_add_thread):
        results = []
        results_lock = threading.Lock()

        def slow_task(task_id):
            time.sleep(0.1)
            with results_lock:
                results.append(task_id)

        num_tasks = 5
        executor = self._new(max_workers=2, min_workers=1)
        for i in range(num_tasks):
            executor.submit(slow_task, i)

        executor.shutdown(wait=True)

        self.assertEqual(len(results), num_tasks)
        self.assertEqual(set(results), set(range(num_tasks)))


@mock.patch('futurist._thread.ThreadWorker.create_and_register', autospec=True)
class TestDynamicThreadPoolMaintain(base.TestCase):
    def test_ensure_one_worker(self, mock_create_thread):
        executor = futurist.DynamicThreadPoolExecutor()
        executor.maintain()
        self.assertEqual(1, len(executor._workers))
        created_worker = mock_create_thread.return_value
        created_worker.start.assert_called_once_with()
        created_worker.stop.assert_not_called()

    def test_ensure_min_workers(self, mock_create_thread):
        executor = futurist.DynamicThreadPoolExecutor(min_workers=42)
        executor.maintain()
        self.assertEqual(42, len(executor._workers))
        created_worker = mock_create_thread.return_value
        created_worker.start.assert_called_with()
        self.assertEqual(42, created_worker.start.call_count)
        created_worker.stop.assert_not_called()

    def test_too_many_idle_workers(self, mock_create_thread):
        executor = futurist.DynamicThreadPoolExecutor(min_workers=42)
        executor._workers = [mock.Mock(idle=True)] * 100
        executor.maintain()
        self.assertEqual(42, len(executor._workers))
        mock_create_thread.return_value.start.assert_not_called()
        self.assertEqual(58, executor._workers[0].stop.call_count)  # type: ignore[attr-defined]

    def test_all_busy_workers(self, mock_create_thread):
        executor = futurist.DynamicThreadPoolExecutor(max_workers=100)
        executor._workers = [mock.Mock(idle=False)] * 100
        executor.maintain()
        self.assertEqual(100, len(executor._workers))
        mock_create_thread.return_value.start.assert_not_called()
        executor._workers[0].stop.assert_not_called()  # type: ignore[attr-defined]

    def test_busy_workers_create_more(self, mock_create_thread):
        executor = futurist.DynamicThreadPoolExecutor(max_workers=200)
        executor._workers = [mock.Mock(idle=False)] * 100
        executor.maintain()
        # NOTE(dtantsur): once the executor reaches 125 threads, the ratio of
        # busy to total threads is exactly 100/125=0.8 (the default
        # grow_threshold). One more thread is created, resulting in 126.
        self.assertEqual(126, len(executor._workers))
        self.assertEqual(26, executor.get_num_idle_workers())
        created_worker = mock_create_thread.return_value
        created_worker.start.assert_called_with()
        self.assertEqual(26, created_worker.start.call_count)
        created_worker.stop.assert_not_called()

    def test_busy_workers_within_range(self, mock_create_thread):
        executor = futurist.DynamicThreadPoolExecutor()
        executor._workers = [mock.Mock(idle=i < 30) for i in range(100)]
        executor.maintain()
        self.assertEqual(100, len(executor._workers))
        mock_create_thread.return_value.start.assert_not_called()

    def test_busy_workers_and_large_queue(self, mock_create_thread):
        executor = futurist.DynamicThreadPoolExecutor(max_workers=200)
        executor._workers = [mock.Mock(idle=i < 30) for i in range(100)]
        for i in range(20):
            executor._work_queue.put(None)  # type: ignore[arg-type]
        executor.maintain()
        # NOTE(dtantsur): initial busy ratio is (70+20)/100=0.9. As workers
        # are added, it reaches (70+20)/113, which is just below 0.8.
        self.assertEqual(113, len(executor._workers))
        created_worker = mock_create_thread.return_value
        created_worker.start.assert_called_with()
        self.assertEqual(13, created_worker.start.call_count)


class _GreenDelayedExecutor(
    futurist.GreenDelayedExecutorMixin, futurist.GreenThreadPoolExecutor
):
    pass


class _ThreadDelayedExecutor(
    futurist.DelayedExecutorMixin, futurist.ThreadPoolExecutor
):
    pass


class TestDelayedExecutorMixin(testscenarios.TestWithScenarios, base.TestCase):
    executor_cls: type
    scenarios = [
        ('green', {'executor_cls': _GreenDelayedExecutor}),
        ('thread', {'executor_cls': _ThreadDelayedExecutor}),
    ]

    def setUp(self):
        super().setUp()
        self.executor = self.executor_cls(max_workers=1)
        self.addCleanup(self.executor.shutdown, wait=True)

    def test_tasks_ordered_by_earliest_deadline(self):
        def task(delay):
            return self.executor.Task(lambda: None, (), {}, None, delay)

        t10 = task(10)
        t9 = task(9)
        t11 = task(11)
        t8 = task(8)
        t1 = task(1)

        self.assertEqual(
            [t1, t8, t9, t10, t11], sorted([t10, t9, t11, t8, t1])
        )

    def test_execute_one(self):
        task = self.executor.submit_after(0.1, returns_args, 13, bar=42)

        self.assertEqual(13 + 42, task.result())

    def test_execute_two_sequential(self):
        task1 = self.executor.submit_after(0.1, returns_clock)
        task2 = self.executor.submit_after(0.1, returns_clock)

        t1_at = task1.result()
        t2_at = task2.result()
        self.assertLess(t1_at, t2_at)

    def test_submit_second_while_waiting_on_first_sequential(self):
        task1 = self.executor.submit_after(0.5, returns_clock)
        time.sleep(0.2)
        task2 = self.executor.submit_after(0.5, returns_clock)

        t1_at = task1.result()
        t2_at = task2.result()
        self.assertLess(t1_at, t2_at)

    def test_submit_second_preempts_first(self):
        task1 = self.executor.submit_after(0.3, returns_clock)
        task2 = self.executor.submit_after(0.1, returns_clock)

        t1_at = task1.result()
        t2_at = task2.result()
        self.assertLess(t2_at, t1_at)

    def test_submit_preempts_first_while_waiting(self):
        task1 = self.executor.submit_after(0.5, returns_clock)
        time.sleep(0.2)
        task2 = self.executor.submit_after(0.1, returns_clock)

        t1_at = task1.result()
        t2_at = task2.result()
        self.assertLess(t2_at, t1_at)

    def test_zero_delay_executes(self):
        """A zero-delay task executes without error."""
        task = self.executor.submit_after(0, returns_one)
        self.assertEqual(1, task.result(timeout=5))

    def test_multiple_tasks_execute_in_delay_order(self):
        """Three tasks with distinct delays finish in deadline order."""
        # Submit in non-deadline order to prove the scheduler sorts by
        # deadline, not by submission order: task_long is submitted first
        # but has the furthest deadline, so task_medium (submitted second)
        # must execute before it.
        task_long = self.executor.submit_after(0.3, returns_clock)
        task_medium = self.executor.submit_after(0.2, returns_clock)
        task_short = self.executor.submit_after(0.1, returns_clock)

        t_short = task_short.result()
        t_medium = task_medium.result()
        t_long = task_long.result()

        self.assertLess(t_short, t_medium)
        self.assertLess(t_medium, t_long)

    def test_delayed_even_with_idle_workers(self):
        """Non-zero delay is respected even when the executor has free workers.

        With max_workers=4, all worker slots are available immediately.
        The task must still wait for its full delay before executing.
        """
        executor = self.executor_cls(max_workers=4)
        self.addCleanup(executor.shutdown, wait=True)

        delay = 0.2
        start = time.monotonic()
        task = executor.submit_after(delay, returns_clock)
        ran_at = task.result(timeout=10)
        elapsed = ran_at - start

        self.assertGreaterEqual(
            elapsed,
            delay,
            "task ran before its scheduled delay elapsed",
        )

    def test_submit_after_raises_after_shutdown(self):
        """submit_after raises RuntimeError once the executor is shut down."""
        # Shut down the executor created in setUp and verify a subsequent
        # submit_after call raises RuntimeError.  The addCleanup will call
        # shutdown again, which is a safe no-op.
        self.executor.shutdown(wait=True)
        self.assertRaises(
            RuntimeError, self.executor.submit_after, 0, returns_one
        )

    def test_cancel_before_deadline_skips_execution(self):
        """Cancelling a future before its deadline prevents the callable
        from running.
        """
        call_count = []

        def fn():
            call_count.append(1)
            return 1

        task = self.executor.submit_after(0.5, fn)
        cancelled = task.cancel()

        self.assertTrue(cancelled)
        # Wait well past the deadline so the scheduler processes the
        # cancellation even on a loaded CI system (0.5s deadline + 0.5s
        # margin = 1.0s total).
        time.sleep(1.0)
        self.assertTrue(task.cancelled())
        self.assertEqual([], call_count)

    def test_exception_propagation(self):
        """An exception raised by the callable is reflected in
        future.exception().
        """
        task = self.executor.submit_after(0.1, blows_up)
        self.assertRaises(RuntimeError, task.result)
        self.assertIsInstance(task.exception(), RuntimeError)

    def test_concurrent_submissions_all_complete(self):
        """Many tasks submitted in rapid succession all complete."""
        num_tasks = 20
        futures_list = [
            self.executor.submit_after(0.1, returns_one)
            for _ in range(num_tasks)
        ]
        results = [f.result(timeout=30) for f in futures_list]
        self.assertEqual([1] * num_tasks, results)

    def test_executor_forwards_args_and_kwargs(self):
        """The callable is invoked with the exact positional and keyword
        args given to submit_after.
        """
        received = []

        def capture(pos, *, kw):
            received.append((pos, kw))
            return pos + kw

        task = self.executor.submit_after(0.1, capture, 7, kw=3)
        result = task.result()

        self.assertEqual(10, result)
        self.assertEqual([(7, 3)], received)

    def test_negative_delay_raises_value_error(self):
        """submit_after rejects negative delay values immediately."""
        self.assertRaises(
            ValueError, self.executor.submit_after, -1, returns_one
        )

    def test_double_shutdown_is_safe(self):
        """Calling shutdown() twice must not raise or deadlock."""
        self.executor.shutdown(wait=True)
        # Second call — must be a safe no-op.
        self.executor.shutdown(wait=True)

    def test_alive_false_after_no_wait_shutdown(self):
        self.executor.shutdown(wait=False)
        self.executor.shutdown(wait=True)  # drain
        self.assertFalse(self.executor.alive)

    def test_scheduler_thread_is_daemon(self):
        if not isinstance(self.executor._scheduler, threading.Thread):
            self.skipTest(
                "daemon attribute only applies to native threading.Thread; "
                "eventlet GreenThreads exit with the hub unconditionally"
            )
        self.assertTrue(
            self.executor._scheduler.daemon,
            "scheduler thread must be a daemon so the process can exit "
            "cleanly if shutdown() is never called",
        )

    def test_shutdown_cancel_futures_cancels_pending_tasks(self):
        """shutdown(cancel_futures=True) cancels tasks still in the queue."""
        # Use a very long delay so both tasks are guaranteed to remain in the
        # scheduler queue (i.e. well before their deadline) when shutdown is
        # called.
        task1 = self.executor.submit_after(100.0, returns_one)
        task2 = self.executor.submit_after(100.0, returns_one)

        self.executor.shutdown(cancel_futures=True, wait=True)

        self.assertTrue(task1.cancelled(), "task1 should have been cancelled")
        self.assertTrue(task2.cancelled(), "task2 should have been cancelled")

    def test_alive_true_before_shutdown(self):
        """The executor is alive as soon as it is created."""
        self.assertTrue(self.executor.alive)

    def test_alive_false_immediately_after_wait_true_shutdown(self):
        self.assertTrue(self.executor.alive)
        self.executor.shutdown(wait=True)
        self.assertFalse(self.executor.alive)

    def test_submit_after_raises_after_wait_false_shutdown(self):
        self.executor.shutdown(wait=False)
        self.assertRaises(
            RuntimeError, self.executor.submit_after, 0, returns_one
        )

    def test_task_cancelled_while_in_queue_is_skipped(self):
        call_count = []

        def fn():
            call_count.append(1)

        # quick (0.1 s) has a shorter deadline than slow (100 s).
        # The scheduler pops quick first, leaving slow in the queue.
        quick = self.executor.submit_after(0.1, returns_one)
        slow = self.executor.submit_after(100.0, fn)

        # Cancel slow while the scheduler is busy waiting on quick's deadline.
        # future.cancel() does NOT notify _queue_changed, so the scheduler
        # is not woken; slow simply sits in the queue as CANCELLED.
        self.assertTrue(slow.cancel())

        # Wait for quick to complete; the scheduler then loops and pops slow.
        self.assertEqual(1, quick.result(timeout=5))

        # Shut down and wait for the scheduler to drain fully rather than
        # sleeping for a fixed time, which would be racy on a loaded system.
        self.executor.shutdown(wait=True)

        self.assertTrue(slow.cancelled())
        self.assertEqual([], call_count, "slow callable must not have run")

    def test_task_cancelled_during_scheduler_wakeup_is_skipped(self):
        call_count = []

        def fn():
            call_count.append(1)
            return 1

        # task_a has a long deadline — the scheduler pops it and waits.
        task_a = self.executor.submit_after(5.0, fn)
        # Ensure the scheduler has had time to pop task_a and begin waiting.
        time.sleep(0.05)

        # Cancel task_a (does NOT wake the scheduler on its own).
        self.assertTrue(task_a.cancel())

        # Submitting task_b calls notify_all, waking the scheduler.
        # It finds changed=True and task_a.future.cancelled() → True,
        # so it takes the L963-966 branch: continue without executing task_a.
        task_b = self.executor.submit_after(0.1, returns_one)

        self.assertEqual(1, task_b.result(timeout=5))
        self.assertTrue(task_a.cancelled())
        self.assertEqual([], call_count, "task_a callable must not have run")

    def test_running_task_not_cancelled_by_cancel_futures(self):
        executor = self.executor_cls(max_workers=2)
        self.addCleanup(executor.shutdown, wait=True)

        # Use futures compatible with the executor's threading model as
        # synchronisation points.  In the green variant these are
        # GreenFuture objects whose .result() yields to the eventlet hub,
        # so the test and the worker interleave correctly without
        # time.sleep() blocking the entire event loop.
        running_signal = executor._get_future_object()
        hold_signal = executor._get_future_object()

        # Release hold_signal in cleanup so the worker is never left dangling
        # if the test fails before the explicit set_result() call below.
        def _release_hold():
            try:
                hold_signal.set_result(None)
            except Exception:
                pass  # already set by the test body

        self.addCleanup(_release_hold)

        def controlled_task():
            running_signal.set_result(True)  # signal: task is now running
            hold_signal.result(timeout=10)  # park until the test proceeds

        slow_future = executor.submit_after(0.0, controlled_task)
        # Block until controlled_task has actually started executing.  In the
        # green variant GreenFuture.result() yields to the hub so the green
        # worker can run and signal without blocking the event loop.
        running_signal.result(timeout=10)

        # At this point slow_future is in RUNNING state: the scheduler called
        # set_running_or_notify_cancel() on it before dispatching to the inner
        # executor, and controlled_task() is still parked at hold_signal.
        # It is no longer in the delay queue, so cancel_futures=True cannot
        # affect it.
        self.assertTrue(
            slow_future.running(),
            "slow_future must be in RUNNING state before shutdown is called",
        )

        # Release the parked task so that shutdown(wait=True) can complete.
        hold_signal.set_result(None)
        executor.shutdown(cancel_futures=True, wait=True)

        self.assertFalse(slow_future.cancelled())
        self.assertTrue(slow_future.done())

    def test_inner_submit_failure_propagated_to_future(self):
        with mock.patch.object(
            self.executor,
            'submit',
            side_effect=RuntimeError("injected dispatch failure"),
        ):
            future = self.executor.submit_after(0.0, returns_one)
            exc = future.exception(timeout=5)

        self.assertIsInstance(exc, RuntimeError)
        self.assertIn("injected dispatch failure", str(exc))

    def test_sentinel_task_has_no_future_attribute(self):
        self.assertFalse(
            hasattr(self.executor._sentinel, 'future'),
            "_SentinelTask must not define a .future attribute — "
            "the cancel-loop guard `if item is not self._sentinel` "
            "exists precisely because it is absent.",
        )
