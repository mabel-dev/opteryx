# datetime_truncate
# ===================
# This module truncates a datetime object to the level of precision that you specify,
# making everything higher than that zero (or one for day and month).
#
# It is based on PostgreSQL's DATE_TRUNC.
#
# Originally from https://github.com/mediapop/datetime_truncate
#
# ------------------------------------------------------------------------------------
#
# This has been updated for Opteryx:
# - Unused functions removed and others renamed
# - the parameter order to match the order in POSTGRES
# - mod is used to truncate sub week values (01-01-1970 is a Thursday so would require
#   additional steps to use this approach)


from datetime import datetime, timezone
from opteryx.utils import dates

__all__ = [
    "date_trunc",
]

NUMERIC_PERIODS = {
    "second": 1,
    "minute": 60,
    "hour": 3600,
    "day": 86400,
}

# fmt:off
PERIODS = {
    "month": dict(microsecond=0, second=0, minute=0, hour=0, day=1),
    "year": dict(microsecond=0, second=0, minute=0, hour=0, day=1, month=1),
}
ODD_PERIODS = { "week", "quarter" }
# fmt:on


def _truncate_week(dt):
    """
    Truncates a date to the first day of an ISO 8601 week, i.e. monday.

    :params dt: an initialized datetime object
    :return: `dt` with the original day set to monday
    :rtype: :py:mod:`datetime` datetime object

    This has been rewritten for Opteryx, it has been moved to the `mod`
    approach, but requires an additional step because 01-JAN-1970 is a
    Thursday.
    """
    from datetime import timedelta

    #   I can't work out why this implementation doesn't work
    #    weeks = dt.timestamp() // 604800
    #    return datetime(1970,1,5, tzinfo=timezone.utc) + timedelta(weeks=weeks)

    dt = date_trunc("day", dt)
    return dt - timedelta(days=dt.isoweekday() - 1)


def _truncate_quarter(dt):
    """
    Truncates the datetime to the first day of the quarter for this date.

    :params dt: an initialized datetime object
    :return: `dt` with the month set to the first month of this quarter
    :rtype: :py:mod:`datetime` datetime object
    """
    dt = date_trunc("month", dt)

    month: int = dt.month
    if 1 <= month <= 3:
        return dt.replace(month=1)
    if 4 <= month <= 6:
        return dt.replace(month=4)
    if 7 <= month <= 9:
        return dt.replace(month=7)
    if 10 <= month <= 12:
        return dt.replace(month=10)
    return None  # pragma: no cover


def date_trunc(truncate_to: str, dt):
    """
    Truncates a dt to have the values with higher precision than
    the one set as `truncate_to` as zero (or one for day and month).

    Possible values for `truncate_to`:

    * second
    * minute
    * hour
    * day
    * week (iso week i.e. to monday)
    * month
    * quarter
    * year

    Examples::

       >>> truncate(datetime(2012, 12, 12, 12), 'day')
       datetime(2012, 12, 12)
       >>> truncate(datetime(2012, 12, 14, 12, 15), 'quarter')
       datetime(2012, 10, 1)
       >>> truncate(datetime(2012, 3, 1), 'week')
       datetime(2012, 2, 27)

    :params dt: an initialized datetime object
    :params truncate_to: The highest precision to keep its original data.
    :return: datetime with `truncated_to` as the highest level of precision
    :rtype: :py:mod:`dt` datetime object
    """
    # convert acceptable non datetime values to datetime
    dt = dates.parse_iso(dt)

    if not isinstance(truncate_to, str):
        truncate_to = truncate_to[0]  # [#325]

    # Added for Opteryx - this improves performance approximately
    # 33% for these items
    if truncate_to in NUMERIC_PERIODS:
        seconds = dt.timestamp()
        seconds -= seconds % NUMERIC_PERIODS[truncate_to]
        return datetime.fromtimestamp(seconds, tz=timezone.utc)

    if truncate_to in PERIODS:
        return dt.replace(**PERIODS[truncate_to])

    if truncate_to == "week":
        return _truncate_week(dt)
    if truncate_to == "quarter":
        return _truncate_quarter(dt)
    raise ValueError(
        f"DATE_TRUNC not valid. Valid periods: {', '.join(list(PERIODS.keys()) + list(ODD_PERIODS))}"
    )  # pragma: no cover
