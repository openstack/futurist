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

import subprocess
import sys
import textwrap

from futurist.tests import base


class TestLazyEventlet(base.TestCase):
    def _run(self, code):
        # Run in a fresh interpreter: the test process has already imported
        # eventlet.
        cmd = [sys.executable, '-c', textwrap.dedent(code)]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        self.assertEqual(0, proc.returncode, proc.stderr)
        return proc.stdout.splitlines()

    def test_import_does_not_load_eventlet(self):
        out = self._run(
            """
            import sys

            import futurist
            from futurist import _green
            import futurist.periodics
            import futurist.waiters

            print('eventlet' in sys.modules)
            print(_green.is_monkey_patched('thread'))
            print('eventlet' in sys.modules)
            """
        )
        self.assertEqual(['False', 'False', 'False'], out)

    def test_green_executor_loads_eventlet(self):
        out = self._run(
            """
            import sys
            import warnings

            import futurist
            from futurist import waiters

            warnings.simplefilter('ignore', DeprecationWarning)
            with futurist.GreenThreadPoolExecutor(max_workers=1) as e:
                fs = [e.submit(sum, [i, 1]) for i in range(3)]
                done, _ = waiters.wait_for_all(fs)
            print(sorted(f.result() for f in done))
            print('eventlet' in sys.modules)
            """
        )
        self.assertEqual(['[1, 2, 3]', 'True'], out)

    def test_green_helpers_without_eventlet(self):
        out = self._run(
            """
            import sys
            import warnings

            class RefuseEventlet:
                def find_spec(self, fullname, path, target=None):
                    if fullname.partition('.')[0] == 'eventlet':
                        raise ImportError('eventlet is not allowed')

            sys.meta_path.insert(0, RefuseEventlet())

            import futurist
            from futurist import _green

            warnings.simplefilter('ignore', DeprecationWarning)
            for make in (
                futurist.GreenThreadPoolExecutor,
                futurist.GreenFuture,
                lambda: futurist.SynchronousExecutor(green=True),
            ):
                try:
                    make()
                except RuntimeError as e:
                    print(e)
            print(_green.Pool, _green.spawn, _green.threading)
            """
        )
        self.assertEqual(
            [
                'Eventlet is needed to use a green executor',
                'Eventlet is needed to use a green future',
                'Eventlet is needed to use a green synchronous executor',
                'None None None',
            ],
            out,
        )
