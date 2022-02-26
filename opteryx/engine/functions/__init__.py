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
import datetime
import time
import numpy
from pyarrow import compute
from cityhash import CityHash64
import opteryx

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


def get_version():
    return opteryx.__version__


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


def _vectorize_no_parameters(func):
    def _inner(items):
        for i in range(items):
            yield [func()]

    return _inner


def _vectorize_single_parameter(func):
    def _inner(array):
        if isinstance(array, str):
            array = [array]
        for a in array:
            yield [func(a)]

    return _inner


def _vectorize_double_parameter(func):
    def _inner(array, p1):
        if isinstance(array, str):
            array = [array]
        for a in array:
            yield [func(a, p1)]

    return _inner


# fmt:off
FUNCTIONS = {
    "VERSION": _vectorize_no_parameters(get_version),
    # TYPE CONVERSION
    "TIMESTAMP": cast("TIMESTAMP"),
    "BOOLEAN": cast("BOOLEAN"),
    "NUMERIC": cast("NUMERIC"),
    "VARCHAR": cast("VARCHAR"),
    "STRING": cast("VARCHAR"),  # alias for VARCHAR
    # STRINGS
    "LENGTH": compute.utf8_length,  # LENGTH(str) -> int
    "UPPER": compute.utf8_upper,  # UPPER(str) -> str
    "LOWER": compute.utf8_lower,  # LOWER(str) -> str
    "TRIM": compute.utf8_trim_whitespace,  # TRIM(str) -> str
    "LEFT": _vectorize_double_parameter(lambda x, y: str(x)[: int(y)]),
    "RIGHT": _vectorize_double_parameter(lambda x, y: str(x)[-int(y) :]),
    # HASHING & ENCODING
    "HASH": _vectorize_single_parameter(lambda x: format(CityHash64(str(x)), "X")),
    "MD5": _vectorize_single_parameter(get_md5),
    "RANDOM": _vectorize_no_parameters(get_random),  # return a random number 0-0.999
    # OTHER
    "GET": _vectorize_double_parameter(_get),  # GET(LIST, index) => LIST[index] or GET(STRUCT, accessor) => STRUCT[accessor]
    # NUMERIC
    "ROUND": compute.round,
    "FLOOR": compute.floor,
    "CEIL": compute.ceil,
    "ABS": compute.abs,
    "TRUNC": compute.trunc,
    # DATES & TIMES
    "NOW": _vectorize_no_parameters(datetime.datetime.utcnow),
    "TODAY": _vectorize_no_parameters(datetime.date.today),
    "TIME": _vectorize_no_parameters(get_time),
    "YESTERDAY": _vectorize_no_parameters(get_yesterday),
    "YEAR": compute.year,
    "MONTH": compute.month,
    "DAY": compute.day,
    "WEEK": compute.iso_week,
    "HOUR": compute.hour,
    "MINUTE": compute.minute,
    "SECOND": compute.second,
    "QUARTER": compute.quarter,


    # NOT CONVERTED YET
    # DATES & TIMES
    "MONTH_NAME": not_implemented,  # the name of the month
    "DAY_NAME": not_implemented,  # the name of the day

    "DAY_OF_YEAR": not_implemented,  # get the day of the year
    "DAY_OF_WEEK": not_implemented,  # get the day of the week (Monday = 1)
    "DATE_ADD": not_implemented,  # date, number, part
    "DATE_DIFF": not_implemented,  # start, end, part
    "AGE": not_implemented,  # 8 years, 3 months, 3 days
    "MID": lambda x, y, z: str(x)[int(y) :][: int(z)],
    "CONCAT": concat,
    "LEVENSHTEIN": levenshtein_distance,
    "REPLACE": not_implemented,  # string, pattern to find, pattern to replace -> string
    # BOOLEAN
    "ISNONE": lambda x: x is None,
    # OTHER
    "SORT": sorted,
    "TRY": attempt,
    "LEAST": min,
    "GREATEST": max,
    "UUID": not_implemented,  # cast value as UUID
}
# fmt:on

PLACEHOLDERS = {
    "$$PREVIOUS_MONTH": get_previous_month,
}


def is_function(name):
    return name.upper() in FUNCTIONS
