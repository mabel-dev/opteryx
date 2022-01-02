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

import operator
from enum import Enum
from opteryx.utils.text import not_like, like, matches


def function_in(x, y):
    candidates = [i for i in y if str(i).strip() != ","]
    return x in candidates


def function_contains(x, y):
    return y in x




class SQL_KEYWORDS(str, Enum):
    ANALYZE = "ANALYZE"
    ASC = "ASC"
    CREATE_INDEX = "CREATE INDEX ON"
    DESC = "DESC"
    DISTINCT = "DISTINCT"
    EXPLAIN = "EXPLAIN"
    FROM = "FROM"
    GROUP_BY = "GROUP BY"
    HAVING = "HAVING"
    LIMIT = "LIMIT"
    NOOPT = "NOOPT"
    ORDER_BY = "ORDER BY"
    SELECT = "SELECT"
    SKIP = "SKIP"
    WHERE = "WHERE"
    WITH = "WITH"

class SQL_TOKENS(int, Enum):
    UNKNOWN = -1
    INTEGER = 0
    DOUBLE = 1
    LITERAL = 2
    ATTRIBUTE = 3
    BOOLEAN = 4
    TIMESTAMP = 5
    NULL = 6
    LEFTPARENTHESES = 7
    RIGHTPARENTHESES = 8
    COMMA = 9
    FUNCTION = 10
    AGGREGATOR = 11
    AS = 12
    EVERYTHING = 13
    OPERATOR = 14
    AND = 15
    OR = 16
    NOT = 17
    SUBQUERY = 18
    EMPTY = 19
    LIST = 20
    STRUCT = 21
    KEYWORD = 22
    RIGHTSTRUCT = 23
    LEFTSTRUCT = 24


# the order of the operators affects the regex, e.g. <> needs to be defined before
# < otherwise that will be matched and the > will be invalid syntax.
OPERATORS = {
    "<>": operator.ne,
    ">=": operator.ge,
    "<=": operator.le,
    ">": operator.gt,
    "<": operator.lt,
    "==": operator.eq,
    "!=": operator.ne,
    "=": operator.eq,
    "IS NOT": operator.is_not,
    "IS": operator.is_,
    "NOT LIKE": not_like,
    "LIKE": like,
    "MATCHES": matches,
    "IN": function_in,
    "CONTAINS": function_contains,
}
