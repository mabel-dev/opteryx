# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import datetime

import numpy
import pyarrow
from pyarrow import compute

from opteryx.exceptions import InvalidFunctionParameterError
from opteryx.exceptions import SqlError
from opteryx.utils.dates import parse_iso


def convert_int64_array_to_pyarrow_datetime(values: numpy.ndarray) -> pyarrow.Array:
    """
    Convert an array of int64 timestamps to a PyArrow TimestampArray based on their range.

    Parameters:
        values: numpy.ndarray
            An array of integer values representing timestamps.

    Returns:
        PyArrow Array
            The converted timestamps in PyArrow compatible format.
    """
    if not isinstance(values, numpy.ndarray):
        raise ValueError("Input must be a numpy ndarray of int64 values.")

    if values.dtype != numpy.int64:
        raise ValueError("Input array must have dtype of int64.")

    # Determine the range of the values to infer timestamp precision
    min_value = numpy.min(values)
    max_value = numpy.max(values)

    # Convert based on inferred precision
    if 1e9 <= min_value < 1e10 and 1e9 <= max_value < 1e10:  # Likely seconds
        timestamps = values.astype("datetime64[s]")
        return pyarrow.array(timestamps)
    elif 1e12 <= min_value < 1e13 and 1e12 <= max_value < 1e13:  # Likely milliseconds
        timestamps = values.astype("datetime64[ms]")
        return pyarrow.array(timestamps)
    elif 1e15 <= min_value < 1e16 and 1e15 <= max_value < 1e16:  # Likely microseconds
        timestamps = values.astype("datetime64[us]")
        return pyarrow.array(timestamps)
    elif min_value >= 1e18 and max_value >= 1e18:  # Likely nanoseconds
        timestamps = values.astype("datetime64[ns]")
        return pyarrow.array(timestamps)
    else:
        raise ValueError("Unable to determine the timestamp precision for the provided values.")


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
            compute.divide(compute.milliseconds_between(x, j2000_scalar), 86400000.0),
            2451545.0,
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

    if arr.dtype == numpy.int64():
        arr = convert_int64_array_to_pyarrow_datetime(arr)

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
    if hasattr(start, "dtype") and start.dtype != TARGET_DATE_TYPE:
        start = start.astype(TARGET_DATE_TYPE)
    if hasattr(end, "dtype") and end.dtype != TARGET_DATE_TYPE:
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
