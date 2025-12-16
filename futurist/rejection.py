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

"""Executor rejection strategies."""

from collections.abc import Callable
from typing import TYPE_CHECKING

import futurist

if TYPE_CHECKING:
    from concurrent.futures import Executor


def reject_when_reached(
    max_backlog: int,
) -> Callable[['Executor', int], None]:
    """Returns a function that will raise when backlog goes past max size."""

    def _rejector(executor: 'Executor', backlog: int) -> None:
        if backlog >= max_backlog:
            raise futurist.RejectedSubmission(
                f"Current backlog {backlog} is not"
                " allowed to go"
                f" beyond {max_backlog}"
            )

    return _rejector
