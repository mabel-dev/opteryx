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
This compensates for missing temporal table support in the SQL parser (sqlparser-rs).

For information on temporal tables see:
https://blog.devgenius.io/a-query-in-time-introduction-to-sql-server-temporal-tables-145ddb1355d9

This supports the following syntaxes

- FOR TODAY
- FOR YESTERDAY
- FOR <timestamp>
- FOR DATES BETWEEN <timestamp> AND <timestamp>
- FOR DATES IN <range>

"""
import datetime
import re

from opteryx.exceptions import SqlError
from opteryx.utils import dates

SQL_PARTS = [
    r"SELECT",
    r"FROM",
    r"FOR",
    r"WHERE",
    r"GROUP\sBY",
    r"HAVING",
    r"ORDER\sBY",
    r"LIMIT",
    r"OFFSET",
    r"INNER\sJOIN",
    r"CROSS\sJOIN",
    r"LEFT\sJOIN",
    r"LEFT\sOUTER\sJOIN",
    r"RIGHT\sJOIN",
    r"RIGHT\sOUTER\sJOIN",
    r"FULL\sJOIN",
    r"FULL\sOUTER\sJOIN",
    r"JOIN",
    r"WITH",
    r"SHOW",
    r"ON",
    r"USING",
]

COMBINE_WHITESPACE_REGEX = re.compile(r"\s+")


def clean_statement(string):  # pragma: no cover
    """
    Remove carriage returns and all whitespace to single spaces
    """
    return COMBINE_WHITESPACE_REGEX.sub(" ", string).strip().upper()


def sql_parts(string):  # pragma: no cover
    """
    Split a SQL statement into clauses
    """
    reg = re.compile(
        r"(\(|\)|,|;|"
        + r"|".join([r"\b" + i.replace(r" ", r"\s") + r"\b" for i in SQL_PARTS])
        + r")",
        re.IGNORECASE,
    )
    parts = reg.split(string)
    return [part.strip() for part in parts if part.strip() != ""]


def remove_comments(string):  # pragma: no cover
    """
    Remove comments from the string
    """
    # first group captures quoted strings (double, single or back tick)
    # second group captures comments (//single-line or /* multi-line */)
    pattern = r"(\"[^\"]\"|\'[^\']\'|\`[^\`]\`)|(/\*[^\*/]*\*/|--[^\r\n]*$)"
    regex = re.compile(pattern, re.MULTILINE | re.DOTALL)

    def _replacer(match):
        # if the 2nd group (capturing comments) is not None,
        # it means we have captured a non-quoted (real) comment string.
        if match.group(2) is not None:
            return ""  # so we will return empty to remove the comment
        # otherwise, we will return the 1st group
        return match.group(1)  # captured quoted-string

    return regex.sub(_replacer, string)


def _subtract_one_month(in_date):  # pragma: no cover
    day = in_date.day
    end_of_previous_month = in_date.replace(day=1) - datetime.timedelta(days=1)
    while True:
        try:
            return end_of_previous_month.replace(day=day)
        except:
            day -= 1
            if day < 0:
                raise ValueError("Unable to determine previous month")


def parse_range(fixed_range):  # pragma: no cover
    fixed_range = fixed_range.upper()
    today = datetime.datetime.utcnow().date()

    if fixed_range in ("PREVIOUS_MONTH", "LAST_MONTH"):
        # end the day before the first of this month
        end = today.replace(day=1) - datetime.timedelta(days=1)
        # start the first day of that month
        start = end.replace(day=1)
    elif fixed_range == "THIS_MONTH":
        # start the first day of this month
        start = today.replace(day=1)
        # end today
        end = today
    elif fixed_range in ("PREVIOUS_CYCLE", "LAST_CYCLE"):
        # if we're before the 21st
        if today.day < 22:
            # end the 21st of last month
            end = _subtract_one_month(today).replace(day=21)
            # start the 22nd of the month before
            start = _subtract_one_month(end).replace(day=22)
        else:
            # end the 21st of this month
            end = today.replace(day=21)
            # start the 22nd of the month before
            start = _subtract_one_month(end).replace(day=22)
    elif fixed_range == "THIS_CYCLE":
        # if we're before the 21st
        if today.day < 22:
            # end today
            end = today
            # start the 22nd of last month
            start = _subtract_one_month(today).replace(day=22)
        else:
            # end the today
            end = today
            # start the 22nd of this month
            start = today.replace(day=22)

    else:
        raise SqlError(f"Unknown temporal range `{fixed_range}`")

    return start, end


def parse_date(date):  # pragma: no cover

    date = date.upper()
    today = datetime.datetime.utcnow().date()

    if date == "TODAY":
        return today
    if date == "YESTERDAY":
        return today - datetime.timedelta(days=1)

    parsed_date = dates.parse_iso(date[1:-1])
    if parsed_date:
        return parsed_date.date()


def extract_temporal_filters(sql):  # pragma: no cover

    # prep the statement, by normalizing it
    clean_sql = remove_comments(sql)
    clean_sql = clean_statement(clean_sql)
    parts = sql_parts(clean_sql)

    today = datetime.datetime.utcnow().date()
    clearing_regex = None
    start_date = today
    end_date = today

    try:
        pos = parts.index("FOR")  # this fails when there is no temporal clause
        for_date_string = parts[pos + 1]
        for_date = parse_date(for_date_string)

        if for_date:
            start_date = for_date
            end_date = for_date
            clearing_regex = (
                r"(\bFOR[\n\r\s]+" + for_date_string.replace("'", r"\'") + r"(?!\S))"
            )
        elif for_date_string.startswith("DATES BETWEEN "):
            parts = for_date_string.split(" ")
            start_date = parse_date(parts[2])
            end_date = parse_date(parts[4])

            if start_date is None or end_date is None:
                raise SqlError("Unrecognized temporal range values.")
            if start_date > end_date:
                raise SqlError(
                    "Invalid temporal range, start of range is after end of range."
                )

            clearing_regex = (
                r"(FOR[\n\r\s]+DATES[\n\r\s]+BETWEEN[\n\r\s]+"
                + parts[2]
                + r"[\n\r\s]+AND[\n\r\s]+"
                + parts[4]
                + r"(?!\S))"
            )
        elif for_date_string.startswith("DATES IN "):
            parts = for_date_string.split(" ")
            start_date, end_date = parse_range(parts[2])

            clearing_regex = (
                r"(FOR[\n\r\s]+DATES[\n\r\s]+IN[\n\r\s]+" + parts[2] + r"(?!\S))"
            )

        if clearing_regex:
            regex = re.compile(clearing_regex, re.MULTILINE | re.DOTALL | re.IGNORECASE)
            sql = regex.sub("\n-- FOR STATEMENT REMOVED\n", sql)

    except SqlError as sql_error:
        raise sql_error
    except Exception as e:
        pass

    return start_date, end_date, sql
