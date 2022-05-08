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

from opteryx.utils.dates import parse_iso


def get_time():
    # doing this inline means the utcnow() function is called once and time is then
    # called for each row - meaning it's not 'now', it's 'when did you start'
    return datetime.datetime.utcnow().time()


def get_yesterday():
    return datetime.date.today() - datetime.timedelta(days=1)


def get_date(input):
    """
    Convert input to a datetime object and extract the Date part
    """
    # if it's a string, parse it (to a datetime)
    if isinstance(input, str):
        input = parse_iso(input)
    # if it's a numpy datetime, convert it to a date
    if isinstance(input, (numpy.datetime64)):
        input = input.astype("M8[D]").astype("O")
    # if it's a datetime, convert it to a date
    if isinstance(input, datetime.datetime):
        input = input.date()
    # set it to midnight that day to make it a datetime
    if isinstance(input, datetime.date):
        return datetime.datetime.combine(input, datetime.datetime.min.time())
    return None
