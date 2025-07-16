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

# Promote accessible items to this module namespace (for easy access).

from futurist._futures import Future
from futurist._futures import GreenFuture

from futurist._futures import CancelledError
from futurist._futures import TimeoutError

from futurist._futures import DynamicThreadPoolExecutor
from futurist._futures import GreenThreadPoolExecutor
from futurist._futures import ProcessPoolExecutor
from futurist._futures import SynchronousExecutor
from futurist._futures import ThreadPoolExecutor

from futurist._futures import RejectedSubmission

from futurist._futures import ExecutorStatistics


__all__ = [
    'Future', 'GreenFuture',

    'CancelledError', 'TimeoutError',

    'GreenThreadPoolExecutor', 'ProcessPoolExecutor',
    'SynchronousExecutor', 'ThreadPoolExecutor',
    'DynamicThreadPoolExecutor',

    'RejectedSubmission',

    'ExecutorStatistics',
]
