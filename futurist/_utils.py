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

import multiprocessing
import time

try:
    now = time.monotonic  # noqa
except AttributeError:
    try:
        # Try to use the pypi module if it's available (optionally...)
        from monotonic import monotonic as now  # noqa
    except (AttributeError, ImportError):
        # Ok fallback to the non-monotonic one...
        now = time.time  # noqa

try:
    import eventlet as _eventlet  # noqa
    EVENTLET_AVAILABLE = True
except ImportError:
    EVENTLET_AVAILABLE = False


def get_optimal_thread_count(default=2):
    """Try to guess optimal thread count for current system."""
    try:
        return multiprocessing.cpu_count() + 1
    except NotImplementedError:
        # NOTE(harlowja): apparently may raise so in this case we will
        # just setup two threads since it's hard to know what else we
        # should do in this situation.
        return default
