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
This builds a logical plan can resolve the query from the user.

This doesn't attempt to do optimization, this just build a convenient plan which will
respond to the query correctly.

The effective order of operations must be:

    01. FROM
    02. JOIN
    03. WHERE
    04. GROUP BY
    05. HAVING
    06. SELECT
    07. DISTINCT
    08. ORDER BY
    09. OFFSET
    10. LIMIT

So we just build it in that order.
"""
import pyarrow

from opteryx import operators
from opteryx.exceptions import SqlError, UnsupportedSyntaxError


from opteryx.managers.planner.logical import builders, queries

from opteryx.models import Columns


def create_plan(ast, properties):

    last_query = None
    for query in ast:
        query_type = next(iter(query))
        builder = queries.QUERY_BUILDER.get(query_type)
        if builder is None:
            raise UnsupportedSyntaxError(f"Statement not supported `{query_type}`")
        last_query = builder(query, properties)
    return last_query
