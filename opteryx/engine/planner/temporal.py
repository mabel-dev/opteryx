"""
This compensates for missing temporal table support in the SQL parser.

For information on temporal tables see: 
https://blog.devgenius.io/a-query-in-time-introduction-to-sql-server-temporal-tables-145ddb1355d9

This supports the following syntaxes

- FOR TODAY
- FOR YESTERDAY
- FOR DATE <timestamp>
- FOR DATES BETWEEN <timestamp> AND <timestamp>

"""
import re
from opteryx.utils import dates
import datetime

SQL_PARTS = [
    r"SELECT",
    r"FROM",
    r"FOR",
    r"WHERE",
    r"GROUP BY",
    r"HAVING",
    r"ORDER BY",
    r"LIMIT",
    r"OFFSET",
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
    pattern = r"(\".*?\"|\'.*?\')|(/\*.*?\*/|--[^\r\n]*$)"
    # first group captures quoted strings (double or single)
    # second group captures comments (//single-line or /* multi-line */)
    regex = re.compile(pattern, re.MULTILINE | re.DOTALL)

    def _replacer(match):
        # if the 2nd group (capturing comments) is not None,
        # it means we have captured a non-quoted (real) comment string.
        if match.group(2) is not None:
            return ""  # so we will return empty to remove the comment
        else:  # otherwise, we will return the 1st group
            return match.group(1)  # captured quoted-string

    return regex.sub(_replacer, string)


def parse_range(range):
    range = range.upper()
    TODAY = datetime.date.today()

    if range == "PREVIOUS_MONTH":
        pass
    if range == "THIS_MONTH":
        pass
    if range == "MONTH(YEAR, MONTH)":
        pass
    if range == "PREVIOUS_CYCLE(ROLL_OVER)":
        pass
    if range == "CYCLE(YEAR, MONTH, ROLL_OVER)":
        pass


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
        pos = parts.index("FOR")
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
            raise NotImplementedError("FOR DATES IN not implemented")
            # PREVIOUS MONTH
            # PREVIOUS CYCLE
            # THIS MONTH
            # THIS CYCLE

        if clearing_regex:
            regex = re.compile(clearing_regex, re.MULTILINE | re.DOTALL | re.IGNORECASE)
            sql = regex.sub("\n-- FOR STATEMENT REMOVED\n", sql)

        # swap the order if we need to
        if start_date > end_date:
            start_date, end_date = end_date, start_date
    except:
        pass

    return start_date, end_date, sql
