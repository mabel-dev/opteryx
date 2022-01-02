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
from opteryx.utils.dates import parse_iso


def get_year(input):
    """
    Convert input to a datetime object and extract the Year part
    """
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return input.year
    return None  #


def get_month(input):
    """
    Convert input to a datetime object and extract the Month part
    """
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return input.month
    return None


def get_day(input):
    """
    Convert input to a datetime object and extract the Day part
    """
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return input.day
    return None


def get_date(input):
    """
    Convert input to a datetime object and extract the Date part
    """
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return input.date()
    return None


def get_time(input):
    """
    Convert input to a datetime object and extract the Time part
    """
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return input.time()
    return None


def get_quarter(input):
    """
    Convert input to a datetime object and calculate the Quarter of the Year
    """
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return ((input.month - 1) // 3) + 1
    return None


def get_hour(input):
    """
    Convert input to a datetime object and extract the Hour part
    """
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return input.hour
    return None


def get_minute(input):
    """
    Convert input to a datetime object and extract the Minute part
    """
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return input.minute
    return None


def get_second(input):
    """
    Convert input to a datetime object and extract the Seconds part
    """
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return input.second
    return None


def get_week(input):
    """
    Convert input to a datetime object and extract the ISO8601 Week
    """
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return int(input.strftime("%V"))
    return None


def add_days(start_date, day_count):
    if isinstance(start_date, str):
        start_date = parse_iso(start_date)
    if isinstance(start_date, (datetime.date, datetime.datetime)):
        return start_date + datetime.timedelta(days=day_count)
    return None


def diff_days(start_date, end_date):
    if isinstance(start_date, str):
        start_date = parse_iso(start_date)
    if isinstance(end_date, str):
        end_date = parse_iso(end_date)
    if isinstance(start_date, (datetime.date, datetime.datetime)) and isinstance(
        end_date, (datetime.date, datetime.datetime)
    ):
        return (end_date - start_date).days
    return None
