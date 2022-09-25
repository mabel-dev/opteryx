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


def satellites(*args):
    """load the satellite sample data"""
    from .satellite_data import load

    return load()


def planets(end_date=datetime.datetime.utcnow().date()):
    """load the planets sample data"""
    from .planet_data import load

    full_set = load()

    # make planet data act like it support temporality
    mask = [True, True, True, True, True, True, True, True, True]
    if end_date < datetime.date(1930, 3, 13):
        # March 13, 1930 - Pluto discovered by Clyde William Tombaugh
        mask = [True, True, True, True, True, True, True, True, False]
    if end_date < datetime.date(1846, 11, 13):
        # November 13, 1846 - Neptune
        mask = [True, True, True, True, True, True, True, False, False]
    if end_date < datetime.date(1781, 4, 26):
        # April 26, 1781 - Uranus discovered by Sir William Herschel
        mask = [True, True, True, True, True, True, False, False, False]

    return full_set.filter(mask)


def astronauts(*args):
    """load the astronaut sample data"""
    from .astronaut_data import load

    return load()


def no_table(*args):
    """load the null data table"""
    from .no_table_data import load

    return load()
