import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import datetime
from pytest import raises
from opteryx.compiled.functions.timestamp import parse_iso_timestamp, parse_iso


def test_parse_timestamp():    # Test valid timestamp

    assert parse_iso_timestamp(b"2023-10-01 12:00:00") == 1696161600000000, parse_iso_timestamp(b"2023-10-01 12:00:00")
    assert parse_iso_timestamp(b"2023-10-01T12:00:00Z") == 1696161600000000

    # Test invalid timestamp
    with raises(ValueError, match="Invalid ISO timestamp"):
        parse_iso_timestamp(b"invalid-timestamp")
 
    # Test edge cases
    assert parse_iso_timestamp(b"1970-01-01 00:00:00") == 0, parse_iso_timestamp(b"1970-01-01 00:00:00")
    assert parse_iso_timestamp(b"1970-01-01 00:00:00.000001") == 1, parse_iso_timestamp(b"1970-01-01 00:00:00.000001")
    assert parse_iso_timestamp(b"1969-12-31 23:59:59") == -1000000, parse_iso_timestamp(b"1969-12-31 23:59:59")
    assert parse_iso_timestamp(b"1969-12-31 23:59:59.999999") == -1, parse_iso_timestamp(b"1969-12-31 23:59:59.999999")
    assert parse_iso_timestamp(b"1969-12-31 23:59:59.999998") == -2, parse_iso_timestamp(b"1969-12-31 23:59:59.999998")
    assert parse_iso_timestamp(b"1969-07-20 08:17:40") == -14226140000000, parse_iso_timestamp(b"1969-07-20 08:17:40")
    assert parse_iso_timestamp(b"9999-12-31 23:59:59") == 253402300799000000, parse_iso_timestamp(b"9999-12-31 23:59:59")

    assert parse_iso_timestamp(b"2023-10-01") == 1696118400000000, parse_iso_timestamp(b"2023-10-01")
    assert parse_iso(b"2023-10-01") == datetime.datetime(2023,10,1,0,0,0), f"{parse_iso(b'2023-10-01')} != {datetime.datetime(2023,10,1,0,0,0)}"

    # Test invalid formats
    with raises(ValueError, match="Month must be between 1 and 12"):
        parse_iso_timestamp(b"2023-13-01 12:00:00")
    
    with raises(ValueError, match="Invalid day for given month/year"):
        parse_iso_timestamp(b"2023-02-30 12:00:00")


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()