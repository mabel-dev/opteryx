import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.utils.formatter import format_sql


def test_format_sql_no_indent():
    sql = "SELECT * FROM mytable"
    formatted_sql = format_sql(sql)
    assert (
        formatted_sql == "\n\x1b[38;5;117mSELECT\x1b[0m * \x1b[38;5;117mFROM\x1b[0m mytable"
    ), formatted_sql.encode()


def test_format_sql_single_level_indent():
    sql = "SELECT * FROM mytable WHERE id = 1"
    formatted_sql = format_sql(sql)
    assert (
        formatted_sql
        == "\n\x1b[38;5;117mSELECT\x1b[0m * \x1b[38;5;117mFROM\x1b[0m mytable \x1b[38;5;117mWHERE\x1b[0m id \x1b[38;5;183m=\x1b[0m \x1b[0;31m1\x1b[0m"
    ), formatted_sql.encode()


def test_format_sql_multiple_level_indent():
    sql = (
        "SELECT * FROM mytable WHERE id = 1 AND name = 'John' AND (age >= 18 OR city = 'New York')"
    )
    formatted_sql = format_sql(sql)
    assert (
        formatted_sql
        == "\n\x1b[38;5;117mSELECT\x1b[0m * \x1b[38;5;117mFROM\x1b[0m mytable \x1b[38;5;117mWHERE\x1b[0m id \x1b[38;5;183m=\x1b[0m \x1b[0;31m1\x1b[0m \x1b[38;5;117mAND\x1b[0m name \x1b[38;5;183m=\x1b[0m 'John' \x1b[38;5;117mAND\x1b[0m \x1b[38;5;102m(\x1b[0m age \x1b[38;5;183m>=\x1b[0m \x1b[0;31m18\x1b[0m \x1b[38;5;117mOR\x1b[0m city \x1b[38;5;183m=\x1b[0m 'New York' \x1b[38;5;102m)\x1b[0m"
    ), formatted_sql.encode()


def test_format_sql_with_order_by():
    sql = "SELECT * FROM mytable WHERE id > 10 ORDER BY name DESC"
    formatted_sql = format_sql(sql)
    assert (
        formatted_sql
        == "\n\x1b[38;5;117mSELECT\x1b[0m * \x1b[38;5;117mFROM\x1b[0m mytable \x1b[38;5;117mWHERE\x1b[0m id \x1b[38;5;183m>\x1b[0m \x1b[0;31m10\x1b[0m \x1b[38;5;117mORDER\x1b[0m \x1b[38;5;117mBY\x1b[0m name DESC"
    ), formatted_sql.encode()


def test_format_sql_with_limit():
    sql = "SELECT * FROM mytable LIMIT 10"
    formatted_sql = format_sql(sql)
    assert (
        formatted_sql
        == "\n\x1b[38;5;117mSELECT\x1b[0m * \x1b[38;5;117mFROM\x1b[0m mytable \x1b[38;5;117mLIMIT\x1b[0m \x1b[0;31m10\x1b[0m"
    ), formatted_sql.encode()


if __name__ == "__main__":
    test_format_sql_multiple_level_indent()
    test_format_sql_no_indent()
    test_format_sql_single_level_indent()
    test_format_sql_with_limit()
    test_format_sql_with_order_by()
