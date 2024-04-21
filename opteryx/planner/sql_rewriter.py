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
~~~
                      ┌───────────┐
                      │   USER    │
         ┌────────────┤           ◄────────────┐
         │SQL         └───────────┘            │
  ───────┼─────────────────────────────────────┼──────
         │                                     │
   ┌─────▼─────┐                               │
   │ SQL       │                               │
   │  Rewriter │                               │
   └─────┬─────┘                               │
         │SQL                                  │Results
   ┌─────▼─────┐                         ┌─────┴─────┐
   │           │                         │           │
   │ Parser    │                         │ Executor  │
   └─────┬─────┘                         └─────▲─────┘
         │AST                                  │Plan
   ┌─────▼─────┐                         ╔═══════════╗
   │ AST       │                         ║Cost-Based ║
   │ Rewriter  │                         ║ Optimizer ║
   └─────┬─────┘                         ╚═════▲═════╝
         │AST                                  │Plan
   ┌─────▼─────┐      ┌───────────┐      ┌─────┴─────┐
   │ Logical   │ Plan │ Heuristic │ Plan │           │
   │   Planner ├──────► Optimizer ├──────► Binder    │
   └───────────┘      └───────────┘      └─────▲─────┘
                                               │Schemas
                                         ┌─────┴─────┐
                                         │           │
                                         │ Catalogue │
                                         └───────────┘
~~~

The SQL Rewriter does the following:
- strips comments
- normalizes whitespace
- temporal extraction (this is non-standard and not part of the parser)

This compensates for missing temporal table support in the SQL parser (sqlparser-rs).
This is relatively complex for what it appears to be doing - it needs to account for
a number of situations whilst being able to reconstruct the SQL query as the parser
would expect it.

For information on temporal tables see:
https://blog.devgenius.io/a-query-in-time-introduction-to-sql-server-temporal-tables-145ddb1355d9

This supports the following syntaxes:

- FOR <timestamp>
- FOR DATES BETWEEN <timestamp> AND <timestamp>
- FOR DATES IN <range>
- FOR DATES SINCE <timestamp>

"""

import datetime
import re
from typing import List
from typing import Tuple

from opteryx.exceptions import InvalidTemporalRangeFilterError
from opteryx.utils import dates

COLLECT_RELATION = [
    r"FROM",
    r"INNER\sJOIN",
    r"CROSS\sJOIN",
    r"LEFT\sJOIN",
    r"LEFT\sOUTER\sJOIN",
    r"RIGHT\sJOIN",
    r"RIGHT\sOUTER\sJOIN",
    r"FULL\sJOIN",
    r"FULL\sOUTER\sJOIN",
    r"JOIN",
    r"CREATE\sTABLE",
    r"ANALYZE\sTABLE",
]

COLLECT_TEMPORAL = [r"FOR"]

STOP_COLLECTING = [
    r"GROUP\sBY",
    r"HAVING",
    r"LIKE",
    r"LIMIT",
    r"OFFSET",
    r"ON",
    r"ORDER\sBY",
    r"SHOW",
    r"SELECT",
    r"WHERE",
    r"WITH",
    r"USING",
    r";",
    r",",
]

COLLECT_ALIAS = [r"AS"]

BOUNDARIES = [r"(", r")"]

SQL_PARTS = (
    COLLECT_RELATION
    + COLLECT_TEMPORAL
    + STOP_COLLECTING
    + COLLECT_ALIAS
    + [r"DATES\sIN\s\w+", r"DATES\sBETWEEN\s[^\r\n\t\f\v]AND\s[^\r\n\t\f\v]", r"DATES\sSINCE\s\w+"]
)

COMBINE_WHITESPACE_REGEX = re.compile(r"\r\n\t\f\v+")

# states for the collection algorithm
WAITING: int = 1
RELATION: int = 4
TEMPORAL: int = 16
ALIAS: int = 64


def sql_parts(string):
    """
    Split a SQL statement into clauses
    """
    keywords = re.compile(
        r"(\,|\(|\)|\;|\t|\n|"
        + r"|".join([r"\b" + i.replace(r" ", r"\s") + r"\b" for i in SQL_PARTS])
        + r")",
        re.IGNORECASE,
    )
    quoted_strings = re.compile(r"(\"(?:\\.|[^\"])*\"|\'(?:\\.|[^\'])*\'|`(?:\\.|[^`])*`)")

    parts = []
    for part in quoted_strings.split(string):
        if part and part[-1] in ("'", '"', "`"):
            parts.append(part)
        else:
            for subpart in keywords.split(part):
                subpart = subpart.strip()
                if subpart:
                    parts.append(subpart)

    return parts


def parse_range(fixed_range):  # pragma: no cover
    fixed_range = fixed_range.upper()
    now = datetime.datetime.utcnow().replace(minute=0, second=0, microsecond=0)

    if fixed_range in ("PREVIOUS_MONTH", "LAST_MONTH"):
        # end the day before the first of this month
        end = now.replace(day=1) - datetime.timedelta(days=1)
        # start the first day of that month
        start = end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    elif fixed_range == "THIS_MONTH":
        # start the first day of this month
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        # end today
        end = now
    else:
        if parse_date(fixed_range):
            raise InvalidTemporalRangeFilterError(
                f"`THIS_MONTH`, `LAST_MONTH` expected, got `{fixed_range}`"
            )
        raise InvalidTemporalRangeFilterError(f"Unknown temporal range `{fixed_range}`")

    return start, end


def parse_date(date, end: bool = False):  # pragma: no cover
    if not date:
        return None

    now = datetime.datetime.utcnow().replace(minute=0, second=0, microsecond=0)

    if date == "TODAY":
        return now
    if date == "YESTERDAY":
        return (now - datetime.timedelta(days=1)).replace(hour=0)

    weekdays = ["MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"]
    if date in weekdays:
        # Find the weekday number (0=Monday, 1=Tuesday, ..., 6=Sunday)
        target_weekday = weekdays.index(date)
        # Find the current weekday number
        current_weekday = now.weekday()

        # Calculate how many days to subtract to get the last occurrence of the target weekday
        days_to_subtract = (current_weekday - target_weekday) % 7
        if days_to_subtract == 0:
            # If today is the target weekday, adjust to get the last week's same day
            days_to_subtract = 7

        # Calculate the most recent date for the target weekday
        most_recent_day = (now - datetime.timedelta(days=days_to_subtract)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        return most_recent_day

    if date[0] == date[-1] and date[0] in ("'", '"', "`"):
        date = date[1:-1]

    parsed = dates.parse_iso(date)
    if parsed and end and len(date) <= 12:
        return parsed.replace(hour=23, minute=59)

    return parsed


def _temporal_extration_state_machine(parts: List[str]) -> Tuple[List[Tuple[str, str]], str]:
    """
    Utilizes a state machine to extract the temporal information from the query
    and maintain the relation to filter information.

    Parameters:
        parts: List[str]
            SQL statement parts

    Returns:
        Tuple containing two lists, first with the temporal filters, second with the remaining SQL parts.
    """
    """
    we use a four state machine to extract the temporal information from the query
    and maintain the relation to filter information.

    We separate out the two key parts of the algorithm, first we determine the state,
    then we work out if the state transition means we should do something.

    We're essentially using a bit mask to record state and transitions.
    """

    state = WAITING
    relation = ""
    temporal = ""
    query_collector = []
    temporal_range_collector = []
    for part in parts:
        # record the current state
        transition = [state]
        comparable_part = part.upper().replace(" ", r"\s")

        # work out what our current state is
        if comparable_part in BOUNDARIES:
            state = WAITING
        if comparable_part in STOP_COLLECTING:
            state = WAITING
        if comparable_part in COLLECT_RELATION:
            state = RELATION
        if comparable_part in COLLECT_TEMPORAL:
            state = TEMPORAL
        if comparable_part in COLLECT_ALIAS:
            state = ALIAS
        transition.append(state)

        # based on what the state was and what it is now, do something
        if transition == [TEMPORAL, TEMPORAL]:
            temporal = (temporal + " " + part).strip()
        elif (
            transition
            in (
                [WAITING, WAITING],
                [TEMPORAL, RELATION],
                [RELATION, RELATION],
                [RELATION, WAITING],
                [ALIAS, RELATION],
                [ALIAS, WAITING],  # probably
            )
            and relation
        ):
            temporal_range_collector.append((relation, temporal))
            relation = ""
            temporal = ""
            if comparable_part == ",":
                state = RELATION
        elif transition == [RELATION, RELATION]:
            relation = part
        elif transition == [WAITING, TEMPORAL]:
            raise InvalidTemporalRangeFilterError(
                "Temporal `FOR` statements must directly follow the dataset they apply to."
            )

        if state != TEMPORAL:
            query_collector.append(part)

    # if we're at the end of we have a relation, emit it
    if relation:
        temporal_range_collector.append((relation, temporal))

    return temporal_range_collector, " ".join(query_collector)


def extract_temporal_filters(sql):  # pragma: no cover
    import shlex

    parts = sql_parts(sql)

    # extract the raw temporal information
    initial_collector, sql = _temporal_extration_state_machine(parts)

    final_collector = []

    for relation, for_date_string in initial_collector:
        start_date = None
        end_date = None

        for_date_string = for_date_string.upper()
        for_date = parse_date(for_date_string)

        if for_date:
            start_date = for_date
            end_date = for_date
            if for_date_string in ("TODAY", "YESTERDAY") or len(for_date_string) <= 12:
                start_date = start_date.replace(hour=0)
                end_date = end_date.replace(hour=23, minute=59)

        elif for_date_string.startswith("DATES BETWEEN "):
            parts = shlex.split(for_date_string)

            if len(parts) != 5:
                raise InvalidTemporalRangeFilterError(
                    "Invalid temporal range, expected format `FOR DATES BETWEEN <start> AND <end>`."
                )
            if parts[3] != "AND":
                raise InvalidTemporalRangeFilterError(
                    f"Invalid temporal range, expected `AND`, found `{parts[3]}`."
                )

            start_date = parse_date(parts[2])
            end_date = parse_date(parts[4], end=True)

            if start_date is None:
                raise InvalidTemporalRangeFilterError(
                    f"Invalid temporal range, expected a date for start of range, found `{parts[2]}`."
                )
            if end_date is None:
                raise InvalidTemporalRangeFilterError(
                    f"Invalid temporal range, expected a date for end of range, found `{parts[4]}`."
                )
            if start_date > end_date:
                raise InvalidTemporalRangeFilterError(
                    "Invalid temporal range, start of range is after end of range."
                )

        elif for_date_string.startswith("DATES IN "):
            parts = shlex.split(for_date_string)
            start_date, end_date = parse_range(parts[2])

        elif for_date_string.startswith("DATES SINCE "):
            parts = shlex.split(for_date_string)
            start_date = parse_date(parts[2])
            end_date = datetime.datetime.utcnow().replace(minute=0, second=0, microsecond=0)

        elif for_date_string:
            raise InvalidTemporalRangeFilterError(
                f"Unable to interpret temporal filter `{for_date_string}`"
            )

        final_collector.append(
            (
                relation,
                start_date,
                end_date,
            )
        )

    # we've rewritten the sql so make it sqlparser-rs compatible
    return sql, final_collector


def do_sql_rewrite(statement):
    return extract_temporal_filters(statement)
