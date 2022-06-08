# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import abc
from typing import Iterable

from opteryx.engine.query_statistics import QueryStatistics


class BasePlanNode(abc.ABC):

    _producers = None

    def __init__(self, statistics: QueryStatistics, **config):
        """
        This is the base class for nodes in the execution plan.

        The initializer accepts a QueryStatistics node which is populated by
        different nodes differently to record what happened during the query
        execution.
        """
        pass

    def __call__(self):
        return self.execute()

    def set_producers(self, producers):
        self._producers = producers

    @property
    def greedy(self):  # pragma: no cover
        """
        Returns True if this node needs all of the data before it can return
        """
        return False

    @property
    def name(self):  # pragma: no cover
        """
        Friendly Name of this node
        """
        return "no name"

    @property
    def config(self):  # pragma: no cover
        """
        Configuration for this node, used when showing execution plans
        """
        return "<not set>"
