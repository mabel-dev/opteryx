# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
"""
Date Utilities
"""

import datetime
import re
from typing import Union

import numpy

TIMEDELTA_REGEX = (
    r"((?P<years>\d+)\s?(?:ys?|yrs?|years?))?\s*"
    r"((?P<months>\d+)\s?(?:mo|mons?|mths?|months?))?\s*"
    r"((?P<weeks>\d+)\s?(?:w|wks?|weeks?))?\s*"
    r"((?P<days>\d+)\s?(?:d|days?))?\s*"
    r"((?P<hours>\d+)\s?(?:h|hrs?|hours?))?\s*"
    r"((?P<minutes>\d+)\s?(?:m|mins?|minutes?))?\s*"
    r"((?P<seconds>\d+)\s?(?:s|secs?|seconds?))?\s*"
)

TIMEDELTA_PATTERN = re.compile(TIMEDELTA_REGEX, re.IGNORECASE)
UNIX_EPOCH: datetime.date = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


def add_months(start_date: datetime.datetime, number_of_months: int):
    """
    Add months to a date, makes assumptions about how to handle the end of the month.
    """
    new_year, new_month = divmod(start_date.month - 1 + number_of_months, 12)
    new_year += start_date.year
    new_month += 1
    # Ensure the month is valid
    new_month = min(max(1, new_month), 12)
    last_day_of_month = (
        datetime.datetime(new_year, new_month % 12 + 1, 1) - datetime.timedelta(days=1)
    ).day
    new_day = min(start_date.day, last_day_of_month)
    return datetime.datetime(
        new_year,
        new_month,
        new_day,
        start_date.hour,
        start_date.minute,
        start_date.second,
        start_date.microsecond,
    )


def add_interval(
    current_date: datetime.datetime, interval: str
) -> Union[datetime.date, datetime.datetime]:
    """
    Parses a human readable timedelta (3d5h19m) into a datetime.timedelta.
    """
    match = TIMEDELTA_PATTERN.match(interval)
    if match:
        parts = {k: int(v) for k, v in match.groupdict().items() if v}
        # time delta doesn't include weeks, months or years
        if "weeks" in parts:
            weeks = parts.pop("weeks")
            current_date = current_date + datetime.timedelta(days=weeks * 7)
        if "months" in parts:
            months = parts.pop("months")
            current_date = add_months(current_date, months)
        if "years" in parts:
            # need to avoid 29th Feb problems, so can't just say year - year
            years = parts.pop("years")
            current_date = add_months(current_date, 12 * years)
        if parts:
            return current_date + datetime.timedelta(**parts)
        return current_date
    raise ValueError(f"Unable to interpret interval - {interval}")  # pragma: no cover


def date_range(start_date, end_date, interval: str):
    """Create a series of dates between two dates with a given interval"""
    start_date = parse_iso(start_date)
    end_date = parse_iso(end_date)

    if start_date > end_date:  # pragma: no cover
        raise ValueError("Cannot create an series with the provided start and end dates")

    # if the dates are the same, return that date
    if start_date == end_date:  # pragma: no cover
        yield start_date
        return

    cursor = start_date
    while cursor <= end_date:
        yield cursor
        cursor = add_interval(cursor, interval)


def parse_iso(value):
    # Date validation at speed is hard, dateutil is great but really slow, this is fast
    # but error-prone. It assumes it is a date or it really nothing like a date.
    # Making that assumption - and accepting the consequences - we can convert up to
    # three times faster than dateutil.
    #
    # valid formats (not exhaustive):
    #
    #   YYYY-MM-DD                 <- date
    #   YYYY-MM-DD HH:MM           <- date and time, no seconds
    #   YYYY-MM-DDTHH:MM           <- date and time, T separator
    #   YYYY-MM-DD HH:MM:SS        <- date and time with seconds
    #   YYYY-MM-DD HH:MM:SS.mmmm   <- date and time with milliseconds
    #
    # If the last character is a Z, we ignore it.
    # If we can't parse as a date we return None rather than error

    from opteryx.compiled.functions.timestamp import parse_iso as c_parse_iso

    try:
        input_type = type(value)

        if input_type is str and value.isdigit():
            value = int(value)
            input_type = int

        if input_type is numpy.datetime64:
            # this can create dates rather than datetimes, so don't return yet
            value = value.astype(datetime.datetime)
            input_type = type(value)
            if input_type is int:
                value /= 1000000000

        if input_type in (int, numpy.int64, float, numpy.float64):
            return datetime.datetime.fromtimestamp(int(value), tz=datetime.timezone.utc).replace(
                tzinfo=None
            )

        if input_type is datetime.datetime:
            return value.replace(microsecond=0)
        if input_type is datetime.date:
            return datetime.datetime.combine(value, datetime.time.min)

        if isinstance(value, str):
            value = value.encode("utf-8")

        return c_parse_iso(value)

    except (ValueError, TypeError):
        return None


def date_trunc(truncate_to, date_values) -> numpy.ndarray:
    """
    Truncate an array of datetimes to a specified unit
    """

    #    numpy.datetime64(int(date_values), 's').astype(datetime.datetime)

    date_values = numpy.array(date_values, dtype="datetime64")

    if not isinstance(truncate_to, str):
        truncate_to = truncate_to[0]  # [#325]

    truncate_to = str(truncate_to).lower()

    if truncate_to == "year":
        return date_values.astype("datetime64[Y]").astype("datetime64[s]")
    elif truncate_to == "quarter":
        months = date_values.astype("datetime64[M]").astype(int) // 3 * 3
        return numpy.array(
            months,
            dtype="datetime64[M]",
        ).astype("datetime64[s]")
    elif truncate_to == "month":
        return date_values.astype("datetime64[M]").astype("datetime64[s]")
    elif truncate_to == "week":
        return (
            (
                date_values
                - ((date_values.astype("datetime64[D]").astype(int) - 4) % 7).astype(
                    "timedelta64[D]"
                )
            )
            .astype("datetime64[D]")
            .astype("datetime64[s]")
        )
    elif truncate_to == "day":
        return date_values.astype("datetime64[D]").astype("datetime64[s]")
    elif truncate_to == "hour":
        timestamps = date_values.astype("datetime64[s]").astype("int64")
        truncated = (timestamps // 3600) * 3600
        return truncated.astype("datetime64[s]")
    elif truncate_to == "minute":
        timestamps = date_values.astype("datetime64[s]").astype("int64")
        truncated = (timestamps // 60) * 60
        return truncated.astype("datetime64[s]")
    elif truncate_to == "second":
        return date_values.astype("datetime64[s]")

    else:
        raise ValueError("Invalid unit: {}".format(truncate_to))
