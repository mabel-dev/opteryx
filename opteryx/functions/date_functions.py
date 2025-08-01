# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import datetime

import numpy
import pyarrow
from pyarrow import compute

from opteryx.exceptions import InvalidFunctionParameterError
from opteryx.exceptions import InvalidInternalStateError
from opteryx.exceptions import SqlError
from opteryx.utils.dates import parse_iso


def convert_int64_array_to_pyarrow_datetime(values: numpy.ndarray) -> pyarrow.Array:
    """
    Convert a NumPy int64 array to PyArrow TimestampArray, inferring time unit.
    """
    if isinstance(values, pyarrow.ChunkedArray):
        values = values.combine_chunks()

    if isinstance(values, pyarrow.Array):
        values = values.to_numpy(zero_copy_only=False)

    if not isinstance(values, numpy.ndarray):
        raise InvalidInternalStateError("Expected a NumPy int64 array.")

    if not numpy.issubdtype(values.dtype, numpy.integer):
        raise ValueError("Cannot convert non-integer array to a timestamp.")

    min_value = values.min()
    max_value = values.max()

    # Range dispatch table: (low, high, datetime unit)
    RANGES = [
        (1e0, 1e6, "D"),
        (1e9, 1e10, "s"),
        (1e12, 1e13, "ms"),
        (1e15, 1e16, "us"),
        (1e18, 1e19, "ns"),
    ]

    for low, high, unit in RANGES:
        if low <= min_value < high and low <= max_value < high:
            try:
                return pyarrow.array(values.astype(f"datetime64[{unit}]"))
            except Exception as e:
                raise ValueError(f"Failed to cast to datetime64[{unit}]: {e}")

    raise ValueError(
        f"Unable to determine timestamp precision for values in range [{min_value}, {max_value}]"
    )


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

    arr = numpy.array(arr, dtype="datetime64[us]")

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
    return numpy.array(
        [datetime.datetime.fromtimestamp(i, tz=datetime.timezone.utc) for i in values],
        dtype="datetime64[s]",
    )


def unixtime(array):
    """
    Convert a NumPy or Arrow array of timestamps or ISO8601 strings to Unix time (seconds since epoch).
    NaNs or nulls are converted to numpy.nan.
    """
    if isinstance(array, pyarrow.ChunkedArray):
        array = array.combine_chunks().to_numpy(zero_copy_only=False)

    if numpy.issubdtype(array.dtype, numpy.datetime64):
        # Convert datetime64[ns] to seconds since epoch
        return array.astype("datetime64[s]").astype(numpy.int64)

    elif array.dtype.kind in {"U", "S", "O"}:
        # Assume strings: parse to datetime and convert
        def to_epoch(s):
            if s is None or s != s:
                return numpy.datetime64("NaT")
            try:
                dt = numpy.datetime64(s, "s")
                return dt.astype(numpy.int64)
            except Exception:
                return numpy.datetime64("NaT")

        return numpy.vectorize(to_epoch)(array)

    else:
        raise TypeError(f"Unsupported array type: {array.dtype}")
