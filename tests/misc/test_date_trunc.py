import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from datetime import datetime, timezone

import pytest

from opteryx.utils.dates import date_trunc

DEFAULT_DT = datetime(2012, 7, 12, 12, 14, 14, 342, timezone.utc)


def test_truncate_to_second():
    dt = datetime(2012, 7, 12, 12, 14, 14, 342, timezone.utc)
    expected = dt.replace(microsecond=0)
    actual = date_trunc("second", dt)
    assert actual == expected


def test_truncate_to_minute():
    dt = datetime(2012, 7, 12, 12, 14, 14, 342, timezone.utc)
    expected = dt.replace(second=0, microsecond=0)
    actual = date_trunc("minute", dt)
    assert actual == expected


def test_truncate_to_hour():
    dt = datetime(2012, 7, 12, 12, 14, 14, 342, timezone.utc)
    expected = dt.replace(minute=0, second=0, microsecond=0)
    actual = date_trunc("hour", dt)
    assert actual == expected


def test_truncate_to_day():
    dt = datetime(2012, 7, 12, 12, 14, 14, 342, timezone.utc)
    expected = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    actual = date_trunc("day", dt)
    assert actual == expected, f"{actual}, {expected}"


def test_truncate_to_month():
    dt = datetime(2012, 7, 12, 12, 14, 14, 342, timezone.utc)
    expected = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    actual = date_trunc("month", dt)
    assert actual == expected


def test_truncate_to_year():
    dt = datetime(2012, 7, 12, 12, 14, 14, 342, timezone.utc)
    expected = dt.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    actual = date_trunc("year", dt)
    assert actual == expected


def test_truncate_to_week():
    dt = datetime(2012, 7, 12, 12, 14, 14, 342, timezone.utc)
    expected = dt.replace(day=9, hour=0, minute=0, second=0, microsecond=0)
    actual = date_trunc("week", dt)
    assert actual == expected, f"{actual}, {expected}"

    dt = datetime(2012, 7, 9, 12, 14, 14, 342, timezone.utc)
    expected = dt.replace(hour=0, minute=0, second=0, microsecond=0)


if __name__ == "__main__":  # pragma: no cover
    test_truncate_to_day()
    test_truncate_to_hour()
    test_truncate_to_minute()
    test_truncate_to_second()
    test_truncate_to_week()
    test_truncate_to_year()

    print("✅ okay")
