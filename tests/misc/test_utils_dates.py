import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import datetime

import pytest

from opteryx.utils import dates

# fmt:off
DATE_TESTS = [
        ("2021001011", None),
        ("2021-02-21", datetime.datetime(2021,2,21)),
        ("2021-02-21T", None),
        ("2021-01-11 12:00", datetime.datetime(2021,1,11,12,0)),
        ("2021-01-11 12:00+0100", datetime.datetime(2021,1,11,12,0)),
        ("2021-01-11 12:00Z", datetime.datetime(2021,1,11,12,0)),
        ("2021-01-11T12:00", datetime.datetime(2021,1,11,12,0)),
        ("2021-01-11T12:00Z", datetime.datetime(2021,1,11,12,0)),
        ("2020-10-01 18:05:20", datetime.datetime(2020,10,1,18,5,20)),
        ("2020-10-01T18:05:20", datetime.datetime(2020,10,1,18,5,20)),
        ("2020-10-01T18:05:20+0100", datetime.datetime(2020,10,1,18,5,20)),
        ("1999-12-31 23:59:59.9", datetime.datetime(1999,12,31,23,59,59)),
        ("1999-12-31 23:59:59.9999", datetime.datetime(1999,12,31,23,59,59)),
        ("1999-12-31T23:59:59.9999", datetime.datetime(1999,12,31,23,59,59)),
        ("1999-12-31T23:59:59.9999Z", datetime.datetime(1999,12,31,23,59,59)),
        ("1999-12-31T23:59:59.999999", datetime.datetime(1999,12,31,23,59,59)),
        ("1999-12-31T23:59:59.999999+0800", datetime.datetime(1999,12,31,23,59,59)),
        ("1999-12-31T23:59:59.99999999", datetime.datetime(1999,12,31,23,59,59))
    ]
# fmt:on


@pytest.mark.parametrize("string, expect", DATE_TESTS)
def test_date_parser(string, expect):
    assert (
        dates.parse_iso(string) == expect
    ), f"{string}  {dates.parse_iso(string)}  {expect}"


if __name__ == "__main__":  # pragma: no cover
    print(f"RUNNING BATTERY OF {len(DATE_TESTS)} DATE TESTS")
    for date_string, date_date in DATE_TESTS:
        print(date_string)
        test_date_parser(date_string, date_date)
    print("okay")
