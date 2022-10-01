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

import datetime

from typing import Any

from opteryx.models.query_statistics import QueryStatistics


class QueryProperties:
    """
    Hints and properties to use when executing queries.
    """

    read_only_properties = ("variables", "statistics", "cache")

    def __init__(self, config: dict = None):

        # this is empty unless it's set as part of the query
        self.variables: dict[str, Any] = {}

        # fmt:off
        if config is None:
            config = {}
        # query parameters - these can be overridden on a per-query basis

        # use the query optimizer
        self.enable_optimizer:bool = True
        # The maximum input frame size for JOINs
        self.internal_batch_size: int = int(config.get("INTERNAL_BATCH_SIZE", 500))
        # The maximum number of records to create in a CROSS JOIN frame
        self.max_join_size: int = int(config.get("MAX_JOIN_SIZE", 10000))
        # Approximate Page Size
        self.page_size: int = config.get("PAGE_SIZE", 64 * 1024 * 1024)
        # Internally split and merge pages
        self.enable_page_management: bool = True

        # cost values go here:
        #    costs are the approximate number of seconds to perform an action
        #    1 million times

        # fmt:on

        self.start_date = datetime.datetime.utcnow().date()
        self.end_date = datetime.datetime.utcnow().date()
        self.statistics = QueryStatistics()
        self.cache = None
