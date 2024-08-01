import os
import sys
import decimal
import datetime
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.functions import try_cast

# Sample test cases for each type
import decimal
import datetime

# Sample test cases for each type as a list of three-part tuples
CAST_TESTS = [
    ("BOOLEAN", "true", True),
    ("BOOLEAN", "false", False),
    ("BOOLEAN", "not a boolean", None),
    ("BOOLEAN", 1, True),
    ("BOOLEAN", 0, False),
    ("BOOLEAN", "1", True),
    ("BOOLEAN", "0", False),
    ("BOOLEAN", True, True),
    ("BOOLEAN", False, False),
    ("BOOLEAN", "yes", True),
    ("BOOLEAN", "no", False),
    ("BOOLEAN", None, None),
    ("BOOLEAN", "", None),

    ("DOUBLE", "3.14", 3.14),
    ("DOUBLE", "not a double", None),
    ("DOUBLE", 2, 2.0),
    ("DOUBLE", 2.0, 2.0),
    ("DOUBLE", "-123.456", -123.456),
    ("DOUBLE", "1e-3", 0.001),
    ("DOUBLE", "1e3", 1000.0),
    ("DOUBLE", float('inf'), float('inf')),
    ("DOUBLE", float('-inf'), float('-inf')),
#    ("DOUBLE", float('nan'), float('nan')),  # our nans don't compare
    ("DOUBLE", None, None),
    ("DOUBLE", "", None),

    ("INTEGER", "123", 123),
    ("INTEGER", "not an integer", None),
    ("INTEGER", 123.45, 123),
    ("INTEGER", 456, 456),
    ("INTEGER", -789, -789),
    ("INTEGER", "456", 456),
    ("INTEGER", "-789", -789),
    ("INTEGER", 0, 0),
    ("INTEGER", "0", 0),
    ("INTEGER", "123abc", None),
    ("INTEGER", None, None),
    ("INTEGER", "", None),

    ("DECIMAL", "3.14", decimal.Decimal("3.14")),
    ("DECIMAL", "not a decimal", None),
    ("DECIMAL", "123.456", decimal.Decimal("123.456")),
    ("DECIMAL", "-789.123", decimal.Decimal("-789.123")),
    ("DECIMAL", "1e-3", decimal.Decimal("0.001")),
    ("DECIMAL", "1e3", decimal.Decimal("1000")),
    ("DECIMAL", "0.001", decimal.Decimal("0.001")),
    ("DECIMAL", "0", decimal.Decimal("0")),
    ("DECIMAL", "Infinity", decimal.Decimal("Infinity")),
    ("DECIMAL", "-Infinity", decimal.Decimal("-Infinity")),
    ("DECIMAL", None, None),
    ("DECIMAL", "", None),

    ("VARCHAR", "string", "string"),
    ("VARCHAR", 123, "123"),
    ("VARCHAR", None, None),
    ("VARCHAR", "", ""),
    ("VARCHAR", "123", "123"),
    ("VARCHAR", 45.67, "45.67"),
    ("VARCHAR", True, "True"),
    ("VARCHAR", False, "False"),
    ("VARCHAR", "  leading and trailing spaces  ", "  leading and trailing spaces  "),
    ("VARCHAR", "special characters !@#$%^&*()", "special characters !@#$%^&*()"),
    ("VARCHAR", b'binary string', "b'binary string'"),  # Binary string to string
    ("VARCHAR", b'\x00\x01\x02', "b'\\x00\\x01\\x02'"),  # Binary data to string

    ("TIMESTAMP", "2021-02-21T12:00:00", datetime.datetime(2021, 2, 21, 12, 0, 0)),
    ("TIMESTAMP", "not a timestamp", None),
    ("TIMESTAMP", "2021-12-31T23:59:59", datetime.datetime(2021, 12, 31, 23, 59, 59)),
    ("TIMESTAMP", "2020-02-29T12:00:00", datetime.datetime(2020, 2, 29, 12, 0, 0)),
    ("TIMESTAMP", "2021-01-01", datetime.datetime(2021, 1, 1, 0, 0)),
    ("TIMESTAMP", "2021-01-01T00:00:00.000000", datetime.datetime(2021, 1, 1, 0, 0)),
    ("TIMESTAMP", "2021-02-30T12:00:00", None),  # Invalid date
    ("TIMESTAMP", "2021-02-21T24:00:00", None),  # Invalid hour
    ("TIMESTAMP", "2021-02-21T12:60:00", None),  # Invalid minute
    ("TIMESTAMP", "2021-02-21T12:00:60", None),  # Invalid second
    ("TIMESTAMP", None, None),
    ("TIMESTAMP", "", None),

    ("STRUCT", '{"key": "value"}', {"key": "value"}),
    ("STRUCT", "not a struct", None),
    ("STRUCT", '{"number": 123}', {"number": 123}),
    ("STRUCT", '{"boolean": true}', {"boolean": True}),
    ("STRUCT", '{"list": [1, 2, 3]}', {"list": [1, 2, 3]}),
    ("STRUCT", '{"nested": {"key": "value"}}', {"nested": {"key": "value"}}),
    ("STRUCT", '{"string": "string", "number": 123}', {"string": "string", "number": 123}),
    ("STRUCT", '{"null_value": null}', {"null_value": None}),
    ("STRUCT", '{}', {}),
    ("STRUCT", '[]', []),  # Invalid struct
    ("STRUCT", None, None),
    ("STRUCT", "", None),

    ("DATE", "2021-02-21", datetime.date(2021, 2, 21)),
    ("DATE", "not a date", None),
    ("DATE", "2021-12-31", datetime.date(2021, 12, 31)),
    ("DATE", "2020-02-29", datetime.date(2020, 2, 29)),
    ("DATE", "2021-01-01", datetime.date(2021, 1, 1)),
    ("DATE", "2021-02-30", None),  # Invalid date
    ("DATE", "2021-13-01", None),  # Invalid month
    ("DATE", "2021-00-01", None),  # Invalid month
    ("DATE", "2021/02/21", None),  # Invalid format
    ("DATE", "21-02-2021", None),  # Invalid format
    ("DATE", None, None),
    ("DATE", "", None),
]

@pytest.mark.parametrize("type_name, input, expected", CAST_TESTS)
def test_cast(type_name, input, expected):
    result = try_cast(type_name)([input])[0]
    assert result == expected, f"{type_name} cast of `{input}` failed: {result} != {expected}"



if __name__ == "__main__":
    passed = 0
    failed = 0
    print(f"RUNNING BATTERY OF {len(CAST_TESTS)} CAST TESTS\n")
    for type_name, input, expected in CAST_TESTS:
        try:
            print(f"TRY_CAST('{type_name}')({input}) == {expected}".ljust(75), end="")
            test_cast(type_name, input, expected)
            print("✅")
            passed += 1
        except AssertionError as err:
            print(err)
            failed += 1

    print()
    if failed == 0:
        print("✅ all tests passed")
    else:
        print(f"{passed} tests passed, {failed} tests failed")