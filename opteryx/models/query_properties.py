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

from dataclasses import dataclass
import datetime

from typing import Any


@dataclass
class QueryProperties:
    """
    Hints and properties to use when executing queries.
    """

    read_only_properties = ("variables", "cache", "temporal_filters", "date", "qid")

    def __init__(self, qid, config: dict = None):

        # this is empty unless it's set as part of the query
        self.variables: dict[str, Any] = {}

        # fmt:off
        if config is None: # pragma: no cover
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
        self.enable_page_defragmentation: bool = True

        # cost values go here:
        #    costs are the approximate number of seconds to perform an action
        #    1 million times

        # fmt:on

        self.temporal_filters: list = []
        self.date = datetime.datetime.utcnow().date()
        self.cache = None
        self.qid = qid
