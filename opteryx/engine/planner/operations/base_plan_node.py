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

"""
This is the base class for nodes in the execution plan.

The initializer accepts a QueryStatistics node which is populated by
different nodes differently to record what happened during the query
execution.
"""
import abc
from typing import Iterable
from opteryx.engine.query_statistics import QueryStatistics


class BasePlanNode(abc.ABC):
    @abc.abstractclassmethod
    def __init__(self, statistics: QueryStatistics, **config):
        pass

    @abc.abstractclassmethod
    def execute(self, data_pages: Iterable) -> Iterable:
        pass

    @property
    def greedy(self):
        return False
