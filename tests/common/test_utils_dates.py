import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import datetime
import pytest
from opteryx.utils import dates


@pytest.mark.parametrize(
    "string, expect",
    # fmt:off
    [
        ("2021001011", None),
        ("2021-02-21", datetime.datetime(2021,2,21)),
        ("2021-02-21T", None),
        ("2021-01-11 12:00", datetime.datetime(2021,1,11,12,0)),
        ("2021-01-11T12:00", datetime.datetime(2021,1,11,12,0)),
        ("2020-10-01 18:05:20", datetime.datetime(2020,10,1,18,5,20)),
        ("2020-10-01T18:05:20", datetime.datetime(2020,10,1,18,5,20)),
        ("1999-12-31 23:59:59.9999", datetime.datetime(1999,12,31,23,59,59)),
        ("1999-12-31T23:59:59.9999", datetime.datetime(1999,12,31,23,59,59)),
        ("1999-12-31T23:59:59.9999Z", datetime.datetime(1999,12,31,23,59,59)),
        ("1999-12-31T23:59:59.999999", datetime.datetime(1999,12,31,23,59,59))
    ],
    # fmt:on
)
def test_date_parser(string, expect):

    assert (
        dates.parse_iso(string) == expect
    ), f"{string}  {dates.parse_iso(string)}  {expect}"
