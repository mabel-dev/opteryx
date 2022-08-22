"""
The best way to test a SQL engine is to throw queries at it.

This tests that features of the parser still aren't implemented, rather than having
to remember to check the parser for them regularly.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest
import opteryx


# fmt:off
STATEMENTS = [

        # SELECT EXCEPT isn't supported
        # https://towardsdatascience.com/4-bigquery-sql-shortcuts-that-can-simplify-your-queries-30f94666a046
        ("SELECT * EXCEPT id FROM $satellites"),

        # TEMPORAL QUERIES aren't part of the AST
        ("SELECT * FROM CUSTOMERS FOR SYSTEM_TIME ('2022-01-01', '2022-12-31')"),

        # DISTINCT ON detects as a function call for function ON
        ("SELECT DISTINCT ON (name) FROM $astronauts ORDER BY 1"),

        # YEAR isn't recognized as a non-identifier (or MONTH, DAY etc)
        ("SELECT DATEDIFF(YEAR, '2017/08/25', '2011/08/25') AS DateDiff;"),

        # MONTH has a bug
        ("SELECT DATEDIFF('months', birth_date, '2022-07-07') FROM $astronauts"),

        # JOIN hints aren't supported
        ("SELECT * FROM $satellites INNER HASH JOIN $planets USING (id)"),

        # Invalid temporal ranges
        ("SELECT * FROM $planets FOR 2022-01-01"),
        ("SELECT * FROM $planets FOR DATES IN 2022"),
        ("SELECT * FROM $planets FOR DATES BETWEEN 2022-01-01 AND TODAY"),
        ("SELECT * FROM $planets FOR DATES BETWEEN today AND yesterday"),

        # Can't IN an INDENTIFIER
        ("SELECT * FROM $astronauts WHERE 'Apollo 11' IN Missions"),
    ]
# fmt:on


@pytest.mark.parametrize("statement", STATEMENTS)
def test_sql_battery(statement):
    """
    Test an battery of statements
    """
    conn = opteryx.connect(partition_scheme=None)
    cursor = conn.cursor()
    with pytest.raises(Exception):
        cursor.execute(statement)
        cursor._results = list(cursor._results)


if __name__ == "__main__":  # pragma: no cover

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} TESTS WHICH SHOULD FAIL")
    for statement in STATEMENTS:
        print(statement)
        test_sql_battery(statement)

    print("✅ okay")
