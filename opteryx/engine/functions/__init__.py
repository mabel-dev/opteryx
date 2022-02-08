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
import string
import orjson
import datetime
import numpy
import pyarrow
from pyarrow import compute
from cityhash import CityHash64

from opteryx.engine.functions.date_functions import *
from opteryx.exceptions import SqlError

from opteryx.utils.text import levenshtein_distance
from opteryx.utils.dates import parse_iso


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



def get_md5(item):
    # this is slow but expected to not have a lot of use
    import hashlib
    return hashlib.md5(str(item).encode()).hexdigest()  # nosec - meant to be MD5

def attempt(func):
    try:
        return func()
    except:
        return None

def not_implemented(*args):
    raise NotImplementedError()

def _get(value, item):
    try:
        if isinstance(value, dict):
            return value.get(item)
        return value[int(item)]
    except (KeyError, IndexError):
        return None


VECTORIZED_CASTERS = {
    "BOOLEAN": "bool",
    "NUMERIC": "float64",
    "VARCHAR": "string",
}

ITERATIVE_CASTERS = {
    "TIMESTAMP": parse_iso,
}

def cast(type):
    if type in VECTORIZED_CASTERS:
        return lambda a: compute.cast(a, VECTORIZED_CASTERS[type])
    if type in ITERATIVE_CASTERS:
        def _inner(arr):
            caster = ITERATIVE_CASTERS[type]
            for i in arr:
                yield [caster(i)]
        return _inner
    raise SqlError(f"Unable to cast to type {type}")


def _vectorize_single_parameter(func):
    def _inner(array):
        for a in array:
            yield [func(a)] 
    return _inner

def _vectorize_double_parameter(func):
    def _inner(array, p1):
        for a in array:
            yield [func(a, p1)]
    return _inner 


FUNCTIONS = {

    # TYPE CONVERSION
    #"CAST": cast_as,
    "TIMESTAMP": cast("TIMESTAMP"),
    "BOOLEAN": cast("BOOLEAN"),
    "NUMERIC": cast("NUMERIC"),
    "VARCHAR": cast("VARCHAR"),
    "STRING": cast("VARCHAR"),  # alias for VARCHAR

    # STRINGS - VECTORIZED
    "LENGTH": compute.utf8_length, # LENGTH(str) -> int
    "UPPER": compute.utf8_upper, # UPPER(str) -> str
    "LOWER": compute.utf8_lower, # LOWER(str) -> str
    "TRIM": compute.utf8_trim_whitespace, # TRIM(str) -> str

    # STRINGS - LOOPED FUNCTIONS
    "LEFT": _vectorize_double_parameter(lambda x, y: str(x)[: int(y)]),
    "RIGHT": _vectorize_double_parameter(lambda x, y: str(x)[-int(y) :]),

    # HASHING & ENCODING
    "HASH": _vectorize_single_parameter(lambda x: format(CityHash64(str(x)), "X")),
    "MD5": _vectorize_single_parameter(get_md5),

    # OTHER
    "GET": _vectorize_double_parameter(_get),  # GET(LIST, index) => LIST[index] or GET(STRUCT, accessor) => STRUCT[accessor]

    # NUMERIC - VECTORIZED
    "ROUND": compute.round,
    "FLOOR": compute.floor,
    "CEIL": compute.ceil,
    "ABS": compute.abs,
    "TRUNC": compute.trunc,

    # NOT CONVERTED YET






    "RANDOM": get_random,  # return a random number 0-99

    # DATES & TIMES
    "YEAR": get_year,
    "MONTH": get_month,
    "MONTH_NAME": not_implemented,  # the name of the month
    "DAY": get_day,
    "DAY_NAME": not_implemented,  # the name of the day
    "DATE": numpy.datetime64, # this should be vectorized
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

    

    "MID": lambda x, y, z: str(x)[int(y) :][: int(z)],
    "CONCAT": concat,
    "LEVENSHTEIN": levenshtein_distance,
    "REGEXP_MATCH": not_implemented,  # string, pattern -> boolean
    "REPLACE": not_implemented,  # string, pattern to find, pattern to replace -> string
    # NUMBERS
    "INTEGER": not_implemented,
    "DOUBLE": not_implemented,
    # BOOLEAN
    "ISNONE": lambda x: x is None,

    # OTHER

    "SORT": sorted,
    "TRY": attempt,
    "LEAST": min,
    "GREATEST": max,
    "UUID": not_implemented,  # cast value as UUID
}


def is_function(name):
    return name.upper() in FUNCTIONS
