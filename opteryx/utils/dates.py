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
import re
from datetime import date
from datetime import datetime
from datetime import time as dtime
from datetime import timedelta
from datetime import timezone
from functools import lru_cache
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


def add_months(dte, num_months):
    new_year, new_month = divmod(dte.month - 1 + num_months, 12)
    new_year += dte.year
    new_month += 1
    last_day_of_month = (datetime(new_year, new_month % 12 + 1, 1) - timedelta(days=1)).day
    new_day = min(dte.day, last_day_of_month)
    return datetime(new_year, new_month, new_day)


def add_interval(current_date: Union[date, datetime], interval: str) -> Union[date, datetime]:
    """
    Parses a human readable timedelta (3d5h19m) into a datetime.timedelta.
    """
    match = TIMEDELTA_PATTERN.match(interval)
    if match:
        parts = {k: int(v) for k, v in match.groupdict().items() if v}
        # time delta doesn't include weeks, months or years
        if "weeks" in parts:
            weeks = parts.pop("weeks")
            current_date = current_date + timedelta(days=weeks * 7)
        if "months" in parts:
            months = parts.pop("months")
            current_date = add_months(current_date, months)
        if "years" in parts:
            # need to avoid 29th Feb problems, so can't just say year - year
            years = parts.pop("years")
            current_date = add_months(current_date, 12 * years)
        if parts:
            return current_date + timedelta(**parts)
        return current_date
    raise ValueError(f"Unable to interpret interval - {interval}")


def date_range(start, end, interval: str):
    """ """
    start = parse_iso(start)
    end = parse_iso(end)

    if start is end or start == end or start > end:
        raise ValueError("Cannot create an series with the provided start and end dates")

    cursor = start
    while cursor <= end:
        yield cursor
        cursor = add_interval(cursor, interval)


@lru_cache(128)
def parse_iso(value):
    date_separators = ("-", ":")
    # date validation at speed is hard, dateutil is great but really slow, this is fast
    # but error-prone. It assumes it is a date or it really nothing like a date.
    # Making that assumption - and accepting the consequences - we can convert upto
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
    try:
        input_type = type(value)

        if input_type in (int, numpy.int64):
            value = numpy.datetime64(int(value), "s").astype(datetime)
            input_type = type(value)

        if input_type == numpy.datetime64:
            # this can create dates rather than datetimes, so don't return yet
            value = value.astype(datetime)
            input_type = type(value)

        if input_type == datetime:
            return value
        if input_type == date:
            return datetime.combine(value, dtime.min)
        if input_type in (int, float):
            return datetime.fromtimestamp(value)
        if input_type == str and 10 <= len(value) <= 33:
            if value[-1] == "Z":
                value = value[:-1]
            if "+" in value:
                value = value.split("+")[0]
                if not 10 <= len(value) <= 28:
                    return None
            val_len = len(value)
            if not value[4] in date_separators or not value[7] in date_separators:
                return None
            if val_len == 10:
                # YYYY-MM-DD
                return datetime(
                    *map(int, [value[:4], value[5:7], value[8:10]])  # type:ignore
                )
            if val_len >= 16:
                if not (value[10] in ("T", " ") and value[13] in date_separators):
                    return False
                if val_len >= 19 and value[16] in date_separators:
                    # YYYY-MM-DD HH:MM:SS
                    return datetime(
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
                    return datetime(
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
    except (ValueError, TypeError):
        return None


EPOCH: date = datetime(1970, 1, 1, tzinfo=timezone.utc)


def date_trunc(truncate_to, dt):
    # convert acceptable non datetime values to datetime
    dt = parse_iso(dt)

    if not isinstance(truncate_to, str):
        truncate_to = truncate_to[0]  # [#325]

    # [#711]
    truncate_to = str(truncate_to).lower()

    if truncate_to == "year":
        return datetime(dt.year, 1, 1, tzinfo=dt.tzinfo)
    elif truncate_to == "quarter":
        quarter = (dt.month - 1) // 3 + 1
        return datetime(dt.year, 3 * quarter - 2, 1, tzinfo=dt.tzinfo)
    elif truncate_to == "month":
        return datetime(dt.year, dt.month, 1, tzinfo=dt.tzinfo)
    elif truncate_to == "week":
        days_since_monday = dt.weekday()
        monday = dt - timedelta(days=days_since_monday)
        return date_trunc("day", monday)
    elif truncate_to == "day":
        return datetime(dt.year, dt.month, dt.day, tzinfo=dt.tzinfo)
    elif truncate_to == "hour":
        return datetime(dt.year, dt.month, dt.day, dt.hour, tzinfo=dt.tzinfo)
    elif truncate_to == "minute":
        return datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, tzinfo=dt.tzinfo)
    elif truncate_to == "second":
        return datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, tzinfo=dt.tzinfo)
    else:
        raise ValueError("Invalid unit: {}".format(truncate_to))
