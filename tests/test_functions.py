import datetime
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.data import Reader
from rich import traceback
from mabel.data.readers.internals import inline_functions

traceback.install()


FIXED_DATE = datetime.datetime(2000, 6, 12, 8, 43, 22)

print(FIXED_DATE)


def test_inline_date_functions():

    assert inline_functions.get_year("2021-01-02") == 2021
    assert inline_functions.get_year("2021-01-02T00:00") == 2021
    assert inline_functions.get_year(FIXED_DATE) == 2000

    assert inline_functions.get_month("2021-01-02") == 1
    assert inline_functions.get_month("2021-01-02T00:00") == 1
    assert inline_functions.get_month(FIXED_DATE) == 6

    assert inline_functions.get_day("2021-01-02") == 2
    assert inline_functions.get_day("2021-01-02T00:00") == 2
    assert inline_functions.get_day(FIXED_DATE) == 12

    assert inline_functions.get_date("2021-01-02") == datetime.date(2021, 1, 2)
    assert inline_functions.get_date("2021-01-02T00:00") == datetime.date(2021, 1, 2)
    assert inline_functions.get_date(FIXED_DATE) == datetime.date(2000, 6, 12)

    assert inline_functions.get_time("2021-01-02T10:12:03") == datetime.time(10, 12, 3)
    assert inline_functions.get_time("2021-01-02T00:00") == datetime.time(0, 0, 0)
    assert inline_functions.get_time(FIXED_DATE) == datetime.time(8, 43, 22)

    assert inline_functions.get_quarter("2021-01-02T10:12:03") == 1
    assert inline_functions.get_quarter("2021-01-02T00:00") == 1
    assert inline_functions.get_quarter(FIXED_DATE) == 2

    assert inline_functions.get_hour("2021-01-02T10:12:03") == 10
    assert inline_functions.get_hour("2021-01-02T00:00") == 0
    assert inline_functions.get_hour(FIXED_DATE) == 8

    assert inline_functions.get_minute("2021-01-02T10:12:03") == 12
    assert inline_functions.get_minute("2021-01-02T00:00") == 0
    assert inline_functions.get_minute(FIXED_DATE) == 43

    assert inline_functions.get_second("2021-01-02T10:12:03") == 3
    assert inline_functions.get_second("2021-01-02T00:00") == 0
    assert inline_functions.get_second(FIXED_DATE) == 22

    assert inline_functions.get_week("2021-01-10T10:12:03") == 1
    assert inline_functions.get_week("2021-01-02T00:00") == 53
    assert inline_functions.get_week(FIXED_DATE) == 24, inline_functions.get_week(
        FIXED_DATE
    )

    assert inline_functions.add_days("2021-01-10T10:12:03", 10) == datetime.datetime(
        2021, 1, 20, 10, 12, 3
    )
    assert inline_functions.add_days("2021-01-02T00:00", -10) == datetime.datetime(
        2020, 12, 23, 0, 0
    )
    assert inline_functions.add_days(FIXED_DATE, 10) == datetime.datetime(
        2000, 6, 22, 8, 43, 22
    )

    assert inline_functions.diff_days("2021-01-10T10:12:03", FIXED_DATE) == -7518


def test_inline_other():
    vals = []
    for i in range(10):
        vals.append(inline_functions.get_random())
    vals = set(vals)
    assert len(vals) == 10, len(vals)

    assert inline_functions.concat("a", "b", "c") == "abc"
    assert inline_functions.concat(["a", "b", "c"]) == "a, b, c"

    assert inline_functions.get_md5("a") == "0cc175b9c0f1b6a831c399e269772661"

    assert (
        inline_functions.to_string({"apples": 2, "pears": 4})
        == '\\{"apples":2,"pears":4}\\'
    )


if __name__ == "__main__":  # pragma: no cover
    test_inline_date_functions()
    test_inline_other()

    print("okay")
