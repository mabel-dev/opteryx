# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime

import numpy
import pyarrow
from pyarrow import compute

from opteryx.exceptions import InvalidFunctionParameterError
from opteryx.exceptions import SqlError
from opteryx.utils.dates import parse_iso


def date_part(part, arr):
    """
    Also the EXTRACT function - we extract a given part from an array of dates
    """
    j2000_scalar = numpy.array([numpy.datetime64("2000-01-01T12:00:00", "us")])
    extractors = {
        "nanosecond": compute.nanosecond,
        "nanoseconds": compute.nanosecond,
        "microsecond": compute.microsecond,
        "microseconds": compute.microsecond,
        "millisecond": compute.millisecond,
        "milliseconds": compute.millisecond,
        "second": compute.second,
        "minute": compute.minute,
        "hour": compute.hour,
        "time": lambda x: compute.cast(x, "time64[us]"),
        "day": compute.day,
        "dayofweek": compute.day_of_week,
        "dow": compute.day_of_week,
        "date": lambda x: compute.cast(x, "date32"),
        "week": compute.week,
        "isoweek": compute.iso_week,
        "month": compute.month,
        "quarter": compute.quarter,
        "dayofyear": compute.day_of_year,
        "doy": compute.day_of_year,
        "year": compute.year,
        "isoyear": compute.iso_year,
        "decade": lambda x: compute.divide(compute.year(x), 10),
        "century": lambda x: compute.add(compute.divide(compute.year(x), 100), 1),
        "epoch": lambda x: compute.divide(compute.cast(x, "int64"), 1000000.00),
        "julian": lambda x: compute.add(
            compute.divide(compute.milliseconds_between(x, j2000_scalar), 86400000.0), 2451545.0
        ),
        # ** supported by parser but not by pyarrow
        # isodow
        # millenium
        # millennium
        # timezone
        # time
        # timezonehour
        # timezoneminute
    }

    # if we get a date literal
    if not hasattr(arr, "__iter__"):
        arr = numpy.array([arr])

    part = part[0].lower()  # [#325]
    if part in extractors:
        return extractors[part](arr)

    from opteryx.utils import suggest_alternative

    alt = suggest_alternative(part, list(extractors.keys()))
    if not alt:
        raise InvalidFunctionParameterError(
            f"Date part `{part}` unsupported for EXTRACT."
        )  # pragma: no cover
    raise InvalidFunctionParameterError(
        f"Date part `{part}` unsupported for EXTRACT. Did you mean '{alt}'?"
    )


def date_diff(part, start, end):
    """calculate the difference between timestamps"""

    extractors = {
        "days": compute.days_between,
        "hours": compute.hours_between,
        "microseconds": compute.microseconds_between,
        "minutes": compute.minutes_between,
        "months": compute.month_interval_between,  # this one doesn't work
        "quarters": compute.quarters_between,
        "seconds": compute.seconds_between,
        "weeks": compute.weeks_between,
        "years": compute.years_between,
    }

    # if we get date literals - this will never run due to [#325]
    if isinstance(start, str):  # pragma: no cover
        if not isinstance(end, (str, datetime.datetime)):
            start = pyarrow.array([parse_iso(start)] * len(end), type=pyarrow.timestamp("us"))
        else:
            start = pyarrow.array([parse_iso(start)], type=pyarrow.timestamp("us"))
    if isinstance(end, str):  # pragma: no cover
        if not isinstance(start, (str, datetime.datetime)):
            end = pyarrow.array([parse_iso(end)] * len(start), type=pyarrow.timestamp("us"))
        else:
            end = pyarrow.array([parse_iso(end)], type=pyarrow.timestamp("us"))

    TARGET_DATE_TYPE: str = "datetime64[us]"
    # cast to the desired type
    if hasattr(start, "dtype"):
        if start.dtype != TARGET_DATE_TYPE:
            start = start.astype(TARGET_DATE_TYPE)
    if hasattr(end, "dtype"):
        if end.dtype != TARGET_DATE_TYPE:
            end = end.astype(TARGET_DATE_TYPE)

    part = part[0].lower()  # [#325]
    if part[-1] != "s":
        part += "s"
    if part in extractors:
        diff = extractors[part](start, end)
        if not hasattr(diff, "__iter__"):
            diff = numpy.array([diff])
        return [i.as_py() for i in diff]

    raise SqlError(f"Date part '{part}' unsupported for DATEDIFF")


def time_diff(time1, time2):
    return date_diff(["hours"], time1, time2)


def date_format(dates, pattern):  # [#325]
    pattern = pattern[0]
    return [None if d is None else d.strftime(pattern) for d in dates.tolist()]


def date_floor(dates, magnitude, units):  # [#325]
    return compute.floor_temporal(dates, magnitude[0], units[0])


def from_unixtimestamp(values):
    return [datetime.datetime.fromtimestamp(i) for i in values]


def unixtime(*args):
    if isinstance(args[0], int):
        now = datetime.datetime.utcnow().timestamp()
        return numpy.full(args[0], now, numpy.int64)
    return [numpy.nan if d != d else d.astype(numpy.int64) for d in args[0]]
