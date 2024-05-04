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
    try:
        input_type = type(value)

        if input_type == numpy.datetime64:
            # this can create dates rather than datetimes, so don't return yet
            value = value.astype(datetime.datetime)
            input_type = type(value)
            if input_type is int:
                value /= 1000000000

        if input_type in (int, numpy.int64, float, numpy.float64):
            return datetime.datetime.fromtimestamp(int(value), tz=datetime.timezone.utc).replace(
                tzinfo=None
            )

        if input_type == datetime.datetime:
            return value.replace(microsecond=0)
        if input_type == datetime.date:
            return datetime.datetime.combine(value, datetime.time.min)

        # if we're here, we're doing string parsing
        if input_type == str and 10 <= len(value) <= 33:
            if value[-1] == "Z":
                value = value[:-1]
            if "+" in value:
                value = value.split("+")[0]
                if not 10 <= len(value) <= 28:
                    return None
            val_len = len(value)
            if value[4] != "-" or value[7] != "-":
                return None
            if val_len == 10:
                # YYYY-MM-DD
                return datetime.datetime(
                    *map(int, [value[:4], value[5:7], value[8:10]])  # type:ignore
                )
            if val_len >= 16:
                if value[10] not in ("T", " ") and value[13] != ":":
                    return None
                if val_len >= 19 and value[16] == ":":
                    # YYYY-MM-DD HH:MM:SS
                    return datetime.datetime(
                        *map(  # type:ignore
                            int,
                            [
                                value[:4],  # YYYY
                                value[5:7],  # MM
                                value[8:10],  # DD
                                value[11:13],  # HH
                                value[14:16],  # MM
                                value[17:19],  # SS
                            ],
                        )
                    )
                if val_len == 16:
                    # YYYY-MM-DD HH:MM
                    return datetime.datetime(
                        *map(  # type:ignore
                            int,
                            [
                                value[:4],
                                value[5:7],
                                value[8:10],
                                value[11:13],
                                value[14:16],
                            ],
                        )
                    )
        return None
    except (ValueError, TypeError) as r:
        return None


def date_trunc(truncate_to, date_value):
    """
    Truncate a datetime to a specified unit
    """
    date_value = parse_iso(date_value)

    if not isinstance(truncate_to, str):
        truncate_to = truncate_to[0]  # [#325]

    # [#711]
    truncate_to = str(truncate_to).lower()

    # fmt:off
    if truncate_to == "year":
        return datetime.datetime(date_value.year, 1, 1, tzinfo=date_value.tzinfo)
    elif truncate_to == "quarter":
        quarter = (date_value.month - 1) // 3 + 1
        return datetime.datetime(date_value.year, 3 * quarter - 2, 1, tzinfo=date_value.tzinfo)
    elif truncate_to == "month":
        return datetime.datetime(date_value.year, date_value.month, 1, tzinfo=date_value.tzinfo)
    elif truncate_to == "week":
        days_since_monday = date_value.weekday()
        monday = date_value - datetime.timedelta(days=days_since_monday)
        return date_trunc("day", monday)
    elif truncate_to == "day":
        return datetime.datetime(date_value.year, date_value.month, date_value.day, tzinfo=date_value.tzinfo)
    elif truncate_to == "hour":
        return datetime.datetime(date_value.year, date_value.month, date_value.day, date_value.hour, tzinfo=date_value.tzinfo)
    elif truncate_to == "minute":
        return datetime.datetime(date_value.year, date_value.month, date_value.day, date_value.hour, date_value.minute, tzinfo=date_value.tzinfo)
    elif truncate_to == "second":
        return datetime.datetime(date_value.year, date_value.month, date_value.day, date_value.hour, date_value.minute, date_value.second, tzinfo=date_value.tzinfo)
    else:  # pragma: no cover
        raise ValueError("Invalid unit: {}".format(truncate_to))
    # fmt:on
