# datetime_truncate
# ===================
# This module truncates a datetime object to the level of precision that you specify,
# making everything higher than that zero (or one for day and month).
#
# It is based on PostgreSQL's DATE_TRUNC.
#
# Originally from https://github.com/mediapop/datetime_truncate
#
# This has been modified to remove 'sugar' functions, to change function names
# to be inline with the rest of the code base and for the parameters to match
# the order in POSTGRES.

from datetime import timedelta
from opteryx.utils import dates

__all__ = [
    "date_trunc",
]

# fmt:off
PERIODS = {
    "second": dict(microsecond=0),
    "minute": dict(microsecond=0, second=0),
    "hour": dict(microsecond=0, second=0, minute=0),
    "day": dict(microsecond=0, second=0, minute=0, hour=0),
    "month": dict(microsecond=0, second=0, minute=0, hour=0, day=1),
    "year": dict(microsecond=0, second=0, minute=0, hour=0, day=1, month=1),
}
ODD_PERIODS = { "week", "quarter" }
# fmt:on


def _truncate_week(datetime):
    """
    Truncates a date to the first day of an ISO 8601 week, i.e. monday.

    :params datetime: an initialized datetime object
    :return: `datetime` with the original day set to monday
    :rtype: :py:mod:`datetime` datetime object
    """
    datetime = date_trunc("day", datetime)
    return datetime - timedelta(days=datetime.isoweekday() - 1)


def _truncate_quarter(datetime):
    """
    Truncates the datetime to the first day of the quarter for this date.

    :params datetime: an initialized datetime object
    :return: `datetime` with the month set to the first month of this quarter
    :rtype: :py:mod:`datetime` datetime object
    """
    datetime = date_trunc("month", datetime)

    month = datetime.month
    if 1 <= month <= 3:
        return datetime.replace(month=1)
    elif 4 <= month <= 6:
        return datetime.replace(month=4)
    elif 7 <= month <= 9:
        return datetime.replace(month=7)
    elif 10 <= month <= 12:
        return datetime.replace(month=10)


def date_trunc(truncate_to, datetime):
    """
    Truncates a datetime to have the values with higher precision than
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

    :params datetime: an initialized datetime object
    :params truncate_to: The highest precision to keep its original data.
    :return: datetime with `truncated_to` as the highest level of precision
    :rtype: :py:mod:`datetime` datetime object
    """
    # convert acceptable non datetime values to datetime
    datetime = dates.parse_iso(datetime)

    if not isinstance(truncate_to, str):
        truncate_to = truncate_to[0]  # [#325]

    if truncate_to in PERIODS:
        return datetime.replace(**PERIODS[truncate_to])

    if truncate_to == "week":
        return _truncate_week(datetime)
    if truncate_to == "quarter":
        return _truncate_quarter(datetime)
    raise ValueError(
        f"DATE_TRUNC not valid. Valid periods: {', '.join(PERIODS.keys() + ODD_PERIODS)}"
    )
