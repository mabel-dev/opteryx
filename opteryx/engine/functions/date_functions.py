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


def get_time():
    # doing this inline means the utcnow() function is called once and time is then
    # called for each row - meaning it's not 'now', it's 'when did you start'
    return datetime.datetime.utcnow().time()


def get_yesterday():
    return datetime.date.today() - datetime.timedelta(days=1)


def get_previous_month():
    from opteryx.utils.dates import date_range

    end_of_previous_month = datetime.date.today().replace(day=1) - datetime.timedelta(
        days=1
    )
    start_of_previous_month = end_of_previous_month.replace(day=1)
    return list(
        date_range(start_date=start_of_previous_month, end_date=end_of_previous_month)
    )


### not used yet


def get_date(input):
    """
    Convert input to a datetime object and extract the Date part
    """
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return input.date()
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
