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
This compensates for missing temporal table support in the SQL parser.

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
]


def clean_statement(string):
    """
    Remove carriage returns and all whitespace to single spaces
    """
    _RE_COMBINE_WHITESPACE = re.compile(r"\s+")
    return _RE_COMBINE_WHITESPACE.sub(" ", string).strip().upper()


def sql_parts(string):
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


def remove_comments(string):
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


def _subtract_one_month(in_date):
    day = in_date.day
    end_of_previous_month = in_date.replace(day=1) - datetime.timedelta(days=1)
    while True:
        try:
            return end_of_previous_month.replace(day=day)
        except:
            day -= 1
            if day < 0:
                raise ValueError("Unable to determine previous month")


def parse_range(fixed_range):
    fixed_range = fixed_range.upper()
    TODAY = datetime.date.today()

    if fixed_range in ("PREVIOUS_MONTH", "LAST_MONTH"):
        # end the day before the first of this month
        end = TODAY.replace(day=1) - datetime.timedelta(days=1)
        # start the first day of that month
        start = end.replace(day=1)
    elif fixed_range == "THIS_MONTH":
        # start the first day of this month
        start = TODAY.replace(day=1)
        # end today
        end = TODAY
    elif fixed_range in ("PREVIOUS_CYCLE", "LAST_CYCLE"):
        # if we're before the 21st
        if TODAY.day < 22:
            # end the 21st of last month
            end = _subtract_one_month(TODAY).replace(day=21)
            # start the 22nd of the month before
            start = _subtract_one_month(end).replace(day=22)
        else:
            # end the 21st of this month
            end = TODAY.replace(day=21)
            # start the 22nd of the month before
            start = _subtract_one_month(end).replace(day=22)
    elif fixed_range == "THIS_CYCLE":
        # if we're before the 21st
        if TODAY.day < 22:
            # end today
            end = TODAY
            # start the 22nd of last month
            start = _subtract_one_month(TODAY).replace(day=22)
        else:
            # end the today
            end = TODAY
            # start the 22nd of this month
            start = TODAY.replace(day=22)

    else:
        raise SqlError(f"Unknown temporal range `{fixed_range}`")

    return start, end


def parse_date(date):

    date = date.upper()
    TODAY = datetime.date.today()

    if date == "TODAY":
        return TODAY
    if date == "YESTERDAY":
        return TODAY - datetime.timedelta(days=1)

    parsed_date = dates.parse_iso(date[1:-1])
    if parsed_date:
        return parsed_date.date()


def extract_temporal_filters(sql):

    # prep the statement, by normalizing it
    clean_sql = remove_comments(sql)
    clean_sql = clean_statement(clean_sql)
    parts = sql_parts(clean_sql)

    TODAY = datetime.date.today()
    clearing_regex = None
    start_date = TODAY
    end_date = TODAY

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

        # swap the order if we need to
        if start_date > end_date:
            start_date, end_date = end_date, start_date
    except Exception as e:
        pass

    return start_date, end_date, sql


if __name__ == "__main__":

    def date_range(
        start_date,
        end_date,
    ):

        if end_date < start_date:  # type:ignore
            raise ValueError(
                "date_range: end_date must be the same or later than the start_date "
            )

        for n in range(int((end_date - start_date).days) + 1):  # type:ignore
            yield start_date + datetime.timedelta(n)  # type:ignore

    s = datetime.date.today().replace(day=1, month=1)
    e = s.replace(year=s.year + 1)
    for d in date_range(s, e):
        print(d, _subtract_one_month(d))
