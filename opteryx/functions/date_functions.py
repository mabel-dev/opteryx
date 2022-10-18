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

from opteryx.exceptions import SqlError
from opteryx.utils.dates import parse_iso


def get_time():
    """
    Get the current time
    """
    return datetime.datetime.utcnow().time()


def get_today():
    """get today"""
    today = datetime.datetime.utcnow().date()
    today = datetime.datetime.combine(today, datetime.time.min)
    return numpy.datetime64(today)


def get_now():
    """get now"""
    now = datetime.datetime.utcnow()
    return numpy.datetime64(now)


def get_yesterday():
    """
    calculate yesterday
    """
    yesterday = datetime.datetime.utcnow().date() - datetime.timedelta(days=1)
    yesterday = datetime.datetime.combine(yesterday, datetime.time.min)
    return numpy.datetime64(yesterday)


def get_date(timestamp):
    """
    Convert input to a datetime object and extract the Date part
    """
    # if it's a string, parse it (to a datetime)
    if isinstance(timestamp, str):
        timestamp = parse_iso(timestamp)
    # if it's a numpy datetime, convert it to a date
    if isinstance(timestamp, (numpy.datetime64)):
        timestamp = timestamp.astype("M8[D]").astype("O")
    # if it's a datetime, convert it to a date
    if isinstance(timestamp, datetime.datetime):
        timestamp = timestamp.date()
    # set it to midnight that day to make it a datetime
    # even though we're getting the date, the supported column type is datetime
    if isinstance(timestamp, datetime.date):
        date = datetime.datetime.combine(timestamp, datetime.time.min)
        return numpy.datetime64(date)
    return None


def date_part(part, arr):
    """
    Also the EXTRACT function - we extract a given part from an array of dates
    """

    extractors = {
        "microsecond": compute.microsecond,
        "second": compute.second,
        "minute": compute.minute,
        "hour": compute.hour,
        "day": compute.day,
        "dow": compute.day_of_week,
        "week": compute.iso_week,
        "month": compute.month,
        "quarter": compute.quarter,
        "doy": compute.day_of_year,
        "year": compute.year,
    }

    # if we get a date literal
    if not hasattr(arr, "__iter__"):
        arr = numpy.array([arr])

    part = part[0].lower()  # [#325]
    if part in extractors:
        return extractors[part](arr)

    raise SqlError(f"Date part `{part}` unsupported for EXTRACT")  # pragma: no cover


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
            start = pyarrow.array(
                [parse_iso(start)] * len(end), type=pyarrow.timestamp("us")
            )
        else:
            start = pyarrow.array([parse_iso(start)], type=pyarrow.timestamp("us"))
    if isinstance(end, str):  # pragma: no cover
        if not isinstance(start, (str, datetime.datetime)):
            end = pyarrow.array(
                [parse_iso(end)] * len(start), type=pyarrow.timestamp("us")
            )
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


def date_format(dates, pattern):  # [#325]
    return compute.strftime(dates, pattern[0])


def date_floor(dates, magnitude, units):  # [#325]
    return compute.floor_temporal(dates, magnitude[0], units[0])
