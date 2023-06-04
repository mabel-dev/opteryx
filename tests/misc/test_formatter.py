import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.utils.formatter import format_sql


def test_format_sql():
    sql = "SELECT * FROM mytable"
    formatted_sql = format_sql(sql)
    assert (
        formatted_sql
        == "\n\x1b[38;2;139;233;253mSELECT\x1b[0m * \x1b[38;2;139;233;253mFROM\x1b[0m mytable \x1b[0m"
    ), formatted_sql.encode()


if __name__ == "__main__":
    test_format_sql()
