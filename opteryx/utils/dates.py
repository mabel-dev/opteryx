"""
Date Utilities
"""

import re
from datetime import date, datetime, time, timedelta
from functools import lru_cache
from typing import Optional, Union

TIMEDELTA_REGEX = (
    r"((?P<days>-?\d+)d)?"
    r"((?P<hours>-?\d+)h)?"
    r"((?P<minutes>-?\d+)m)?"
    r"((?P<seconds>-?\d+)s)?"
)
TIMEDELTA_PATTERN = re.compile(TIMEDELTA_REGEX, re.IGNORECASE)


def extract_date(value):
    if isinstance(value, str):
        value = parse_iso(value)
    if isinstance(value, (date, datetime)):
        return date(value.year, value.month, value.day)
    return date.today()


# based on:
# https://gist.github.com/santiagobasulto/698f0ff660968200f873a2f9d1c4113c#file-parse_timedeltas-py
def parse_delta(delta: str) -> timedelta:
    """
    Parses a human readable timedelta (3d5h19m) into a datetime.timedelta.

    Delta includes:
    * Xd days
    * Xh hours
    * Xm minutes
    * Xs seconds

    Values can be negative following timedelta's rules. Eg: -5h-30m
    """
    match = TIMEDELTA_PATTERN.match(delta)
    if match:
        parts = {k: int(v) for k, v in match.groupdict().items() if v}
        return timedelta(**parts)
    return timedelta(seconds=0)


@lru_cache(128)
def parse_iso(value):
    DATE_SEPARATORS = {"-", ":"}
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

        if input_type in (datetime, date, time):
            return value
        if input_type in (int, float):
            return datetime.fromtimestamp(value)
        if input_type == str and 10 <= len(value) <= 28:
            if value[-1] == "Z":
                value = value[:-1]
            val_len = len(value)
            if not value[4] in DATE_SEPARATORS or not value[7] in DATE_SEPARATORS:
                return None
            if val_len == 10:
                # YYYY-MM-DD
                return datetime(*map(int, [value[:4], value[5:7], value[8:10]]))
            if val_len >= 16:
                if not (value[10] in ("T", " ") and value[13] in DATE_SEPARATORS):
                    return False
                if val_len >= 19 and value[16] in DATE_SEPARATORS:
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


def date_range(
    start_date: Optional[Union[str, date]],
    end_date: Optional[Union[str, date]],
):
    """
    An interator over a range of dates
    """
    # if dates aren't provided, use today
    end_date = extract_date(end_date)
    start_date = extract_date(start_date)

    if end_date < start_date:  # type:ignore
        raise ValueError(
            "date_range: end_date must be the same or later than the start_date "
        )

    for delta in range(int((end_date - start_date).days) + 1):  # type:ignore
        yield start_date + timedelta(delta)  # type:ignore
