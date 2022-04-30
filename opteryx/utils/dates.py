"""
Date Utilities
"""
from datetime import date, datetime, time, timedelta
from functools import lru_cache
from typing import Optional, Union


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
