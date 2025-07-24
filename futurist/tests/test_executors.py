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


def blows_up():
    raise RuntimeError("no worky")


def delayed(wait_secs):
    time.sleep(wait_secs)


class TestExecutors(testscenarios.TestWithScenarios, base.TestCase):
    scenarios = [
        ('sync', {'executor_cls': futurist.SynchronousExecutor,
                  'restartable': True, 'executor_kwargs': {}}),
        ('green_sync', {'executor_cls': futurist.SynchronousExecutor,
                        'restartable': True,
                        'executor_kwargs': {'green': True}}),
        ('green', {'executor_cls': futurist.GreenThreadPoolExecutor,
                   'restartable': False, 'executor_kwargs': {}}),
        ('thread', {'executor_cls': futurist.ThreadPoolExecutor,
                    'restartable': False, 'executor_kwargs': {}}),
        ('thread_dyn', {'executor_cls': futurist.DynamicThreadPoolExecutor,
                        'restartable': False, 'executor_kwargs': {}}),
        ('process', {'executor_cls': futurist.ProcessPoolExecutor,
                     'restartable': False, 'executor_kwargs': {}}),
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
        self.assertGreaterEqual(self.executor.statistics.runtime,
                                # It appears that the thread run loop
                                # may call this before 0.2 seconds (or 0.2
                                # will not be represented as a float correctly)
                                # is really up so accommodate for that
                                # happening...
                                0.199)

    def test_post_shutdown_raises(self):
        executor = self.executor_cls(**self.executor_kwargs)
        executor.shutdown()
        self.assertRaises(RuntimeError, executor.submit, returns_one)

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
        ('green', {'executor_cls': futurist.GreenThreadPoolExecutor,
                   'executor_kwargs': {'check_and_reject': rejector,
                                       'max_workers': 1},
                   'event_cls': green_threading.Event}),
        ('thread', {'executor_cls': futurist.ThreadPoolExecutor,
                    'executor_kwargs': {'check_and_reject': rejector,
                                        'max_workers': 1},
                    'event_cls': threading.Event}),
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

        self.assertRaises(futurist.RejectedSubmission,
                          self.executor.submit, returns_one)


@mock.patch.object(futurist.DynamicThreadPoolExecutor, '_add_thread',
                   # Use the original function behind the scene
                   side_effect=futurist.DynamicThreadPoolExecutor._add_thread,
                   autospec=True)
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
        self.assertEqual(58, executor._workers[0].stop.call_count)

    def test_all_busy_workers(self, mock_create_thread):
        executor = futurist.DynamicThreadPoolExecutor(max_workers=100)
        executor._workers = [mock.Mock(idle=False)] * 100
        executor.maintain()
        self.assertEqual(100, len(executor._workers))
        mock_create_thread.return_value.start.assert_not_called()
        executor._workers[0].stop.assert_not_called()

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
            executor._work_queue.put(None)
        executor.maintain()
        # NOTE(dtantsur): initial busy ratio is (70+20)/100=0.9. As workers
        # are added, it reaches (70+20)/113, which is just below 0.8.
        self.assertEqual(113, len(executor._workers))
        created_worker = mock_create_thread.return_value
        created_worker.start.assert_called_with()
        self.assertEqual(13, created_worker.start.call_count)
