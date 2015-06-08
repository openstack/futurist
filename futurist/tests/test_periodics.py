# -*- coding: utf-8 -*-

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

import contextlib
import threading
import time

import testscenarios

import futurist
from futurist import periodics
from futurist.tests import base


@periodics.periodic(1)
def every_one_sec(cb):
    cb()


@periodics.periodic(0.5)
def every_half_sec(cb):
    cb()


@contextlib.contextmanager
def create_destroy_thread(run_what, *args, **kwargs):
    t = threading.Thread(target=run_what, args=args, kwargs=kwargs)
    t.daemon = True
    t.start()
    try:
        yield
    finally:
        t.join()


class TestPeriodics(testscenarios.TestWithScenarios, base.TestCase):
    scenarios = [
        ('sync', {'executor_cls': futurist.SynchronousExecutor}),
        ('thread', {'executor_cls': futurist.ThreadPoolExecutor}),
    ]

    def test_worker(self):
        called = []

        def cb():
            called.append(1)

        callables = [
            (every_one_sec, (cb,), None),
            (every_half_sec, (cb,), None),
        ]
        executor_factory = lambda: self.executor_cls()
        w = periodics.PeriodicWorker(callables,
                                     executor_factory=executor_factory)
        with create_destroy_thread(w.start):
            time.sleep(2.0)
            w.stop()

        am_called = sum(called)
        self.assertGreaterEqual(am_called, 4)
