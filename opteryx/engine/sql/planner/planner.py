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
Query Planner
-------------

This builds a DAG which describes a query.

This doesn't attempt to do optimization, this just decomposes the query.
"""

import re
from typing import Optional
from ....utils.token_labeler import TOKENS, Tokenizer

NODE_TYPES = {
    "select": "select",  # filter tuples
    "project": "project",  # filter fields
    "read": "read",  # read file
    "join": "join",  # cartesian join
    "union": "union",  # acculate records
    "rename": "rename",  # rename fields
    "sort": "sort",  # order records
    "distinct": "distinct",  # deduplicate
    "aggregate": "aggregation",  # calculate aggregations
    "evaluate": "evaluation",  # calculate evaluations
}


## expressions are split, ANDs are separate items, ORs are kept together

"""

SELECT * FROM TABLE => reader(TABLE) -> project(*)
SELECT field FROM TABLE => reader(TABLE) -> project(field)
SELECT field FROM TABLE WHERE field > 7 => reader(TABLE) -> select(field > 7) -> project(field)

"""
