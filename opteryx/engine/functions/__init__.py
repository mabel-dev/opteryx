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
"""
These are a set of functions that can be applied to data.
"""
from os import truncate
import orjson
import datetime
from cityhash import CityHash32

from opteryx.engine.functions.date_functions import *


from opteryx.utils.text import levenshtein_distance


def get_random():
    from opteryx.utils.entropy import random_range

    return random_range(0, 999) / 1000


def concat(*items):
    """
    Turn each item to a string and concatenate the strings together
    """
    sep = ""
    if len(items) == 1 and (
        isinstance(items[0], (list, tuple, set)) or hasattr(items[0], "as_list")
    ):
        items = items[0]
        sep = ", "
    return sep.join(map(str, items))


def to_string(val):
    if isinstance(val, (list, tuple, set)):
        return concat(val)
    if hasattr(val, "mini"):
        return "\\" + val.mini.decode("UTF8") + "\\"
    if isinstance(val, dict):
        return "\\" + orjson.dumps(val).decode("UTF8") + "\\"
    else:
        return str(val)


def parse_number(parser, coerce):
    def inner(val):
        if val is None:
            return None
        return coerce(parser(val))

    return inner


def get_md5(item):
    import hashlib

    return hashlib.md5(str(item).encode()).hexdigest()  # nosec - meant to be MD5


def attempt(func):
    try:
        return func()
    except:
        return None


def not_implemented(*args):
    raise NotImplementedError()


FUNCTIONS = {
    # DATES & TIMES
    "YEAR": get_year,
    "MONTH": get_month,
    "MONTH_NAME": not_implemented,  # the name of the month
    "DAY": get_day,
    "DAY_NAME": not_implemented,  # the name of the day
    "DATE": get_date,
    "QUARTER_OF_YEAR": get_quarter,
    "WEEK_OF_YEAR": get_week,
    "DAY_OF_YEAR": not_implemented,  # get the day of the year
    "DAY_OF_WEEK": not_implemented,  # get the day of the week (Monday = 1)
    "HOUR": get_hour,
    "MINUTE": get_minute,
    "SECOND": get_second,
    "TIME": get_time,
    "CURRENT_DATE": datetime.date.today,
    "TODAY": not_implemented,
    "YESTERDAY": not_implemented,
    "NOW": datetime.datetime.now,
    "DATE_ADD": not_implemented,  # date, number, part
    "DATE_DIFF": not_implemented,  # start, end, part
    "AGE": not_implemented,  # 8 years, 3 months, 3 days
    "FROM_EPOCH": not_implemented,  # timestamp from linux epoch formatted time
    "TO_EPOCH": not_implemented,  # timestamp in linux epoch format
    "DATE_PART": not_implemented,  # DATE_PART("YEAR", timestamp)
    "TIMESTAMP": not_implemented,  # parse input as a TIMESTAMP
    # STRINGS
    "UCASE": lambda x: str(x).upper(),
    "UPPER": lambda x: str(x).upper(),
    "LCASE": lambda x: str(x).lower(),
    "LOWER": lambda x: str(x).lower(),
    "TRIM": lambda x: str(x).strip(),
    "LEN": len,
    "STRING": to_string,
    "VARCHAR": to_string,
    "LEFT": lambda x, y: str(x)[: int(y)],
    "RIGHT": lambda x, y: str(x)[-int(y) :],
    "MID": lambda x, y, z: str(x)[int(y) :][: int(z)],
    "CONCAT": concat,
    "LEVENSHTEIN": levenshtein_distance,
    "REGEXP_MATCH": not_implemented,  # string, pattern -> boolean
    "REPLACE": not_implemented,  # string, pattern to find, pattern to replace -> string
    # NUMBERS
    "ABS": abs,
    "ROUND": round,
    "TRUNC": parse_number(float, truncate),
    "INTEGER": parse_number(float, int),
    "DOUBLE": parse_number(float, float),
    # BOOLEAN
    "BOOLEAN": lambda x: str(x).upper() != "FALSE",
    "ISNONE": lambda x: x is None,
    # HASHING & ENCODING
    "HASH": lambda x: format(CityHash32(str(x)), "X"),
    "MD5": get_md5,
    "RANDOM": get_random,  # return a random number 0-99
    # OTHER
    "BETWEEN": lambda val, low, high: low < val < high,
    "SORT": sorted,
    "TRY": attempt,
    "LEAST": min,
    "GREATEST": max,
    "UUID": not_implemented,  # cast value as UUID
    "GET": not_implemented,  # GET(LIST, index) => LIST[index] or GET(STRUCT, accessor) => STRUCT[accessor]
}
