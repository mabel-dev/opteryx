# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import datetime
from dataclasses import dataclass
from typing import Any


@dataclass
class QueryProperties:
    """
    Hints and properties to use when executing queries.
    """

    def __init__(self, qid: str, variables):
        # this is empty unless it's set as part of the query
        self.variables: dict[str, Any] = variables
        self.temporal_filters: list = []
        self.date = datetime.datetime.utcnow().date()
        self.current_time = datetime.datetime.utcnow()
        self.cache = None
        self.qid = qid
        self.ctes: dict = {}
