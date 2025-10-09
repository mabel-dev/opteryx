import os
import sys

import numpy

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.functions.date_functions import unixtime
from opteryx.functions.date_functions import from_unixtimestamp


def test_unixtime_with_datetime64():
    arr = numpy.array(['2020-01-01T00:00:00', '2021-01-01T12:00:00'], dtype='datetime64[ns]')
    result = unixtime(arr)
    assert result.dtype == numpy.int64
    assert result[0] == 1577836800  # 2020-01-01T00:00:00 UTC
    assert result[1] == 1609502400  # 2021-01-01T12:00:00 UTC

def test_unixtime_with_string_dates():
    arr = numpy.array(['2020-01-01T00:00:00', '2021-01-01T12:00:00'])
    result = unixtime(arr)
    assert result[0] == 1577836800
    assert result[1] == 1609502400

def test_unixtime_with_empty_array():
    arr = numpy.array([], dtype='datetime64[ns]')
    result = unixtime(arr)
    assert result.size == 0

def test_from_unixtimestamp_single_known_value():
    ts = numpy.array([1572912000], dtype=numpy.int64)  # 2019-11-05T00:00:00Z
    result = from_unixtimestamp(ts)
    assert result[0] == numpy.datetime64("2019-11-05T00:00:00")

def test_from_unixtimestamp_multiple_values():
    ts = numpy.array([
        0,                    # 1970-01-01T00:00:00
        946684800,            # 2000-01-01T00:00:00
        1609459200            # 2021-01-01T00:00:00
    ], dtype=numpy.int64)
    expected = numpy.array([
        numpy.datetime64("1970-01-01T00:00:00"),
        numpy.datetime64("2000-01-01T00:00:00"),
        numpy.datetime64("2021-01-01T00:00:00")
    ])
    result = from_unixtimestamp(ts)
    numpy.testing.assert_array_equal(result, expected)


def test_from_unixtimestamp_round_trip():
    # Round trip: datetime → unixtime → datetime
    datetimes = numpy.array(["2020-01-01", "2021-01-01"], dtype="datetime64[s]")
    ints = datetimes.astype(numpy.int64)
    roundtrip = from_unixtimestamp(ints)
    numpy.testing.assert_array_equal(roundtrip, datetimes)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
