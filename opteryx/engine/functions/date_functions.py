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

from pyarrow import compute

from opteryx.exceptions import SqlError
from opteryx.utils.dates import parse_iso


def get_time():
    """
    Get the current time
    """
    return datetime.datetime.utcnow().time()


def get_yesterday():
    """
    calculate yesterday
    """
    return datetime.datetime.utcnow().date() - datetime.timedelta(days=1)


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
        return datetime.datetime.combine(timestamp, datetime.datetime.min.time())
    return None


def date_part(interval, arr):
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

    interval = interval.lower()
    if interval in extractors:
        return extractors[interval](arr)

    raise SqlError(f"Unsupported date part `{interval}`")  # pragma: no cover
