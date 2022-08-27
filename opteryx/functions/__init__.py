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
import os
import pyarrow

from cityhash import CityHash64
from pyarrow import compute
from pyarrow import ArrowNotImplementedError

import opteryx.managers.connection as connection

from opteryx.exceptions import SqlError
from opteryx.functions import date_functions
from opteryx.functions import number_functions
from opteryx.functions import other_functions
from opteryx.functions import string_functions
from opteryx.third_party.date_trunc import date_trunc
from opteryx.utils import dates


def get_random():
    """get a random number between 0 and 1, four decimal places"""
    range_min, range_max = 0, 10000
    random_int = int.from_bytes(os.urandom(2), "big")
    try:
        return ((random_int % ((range_max + 1) - range_min)) + range_min) / 10000
    except:
        return 0


def get_md5(item):
    """calculate MD5 hash of a value"""
    # this is slow but expected to not have a lot of use
    import hashlib  # delay the import - it's rarely needed

    return hashlib.md5(str(item).encode()).hexdigest()  # nosec - meant to be MD5


def get_version():
    """return opteryx version"""
    return connection.__version__


def _get(value, item):
    try:
        if isinstance(value, dict):
            return value.get(item)
        return value[int(item)]
    except (KeyError, IndexError, TypeError):
        return None


VECTORIZED_CASTERS = {"BOOLEAN": "bool", "NUMERIC": "float64", "VARCHAR": "string"}

ITERATIVE_CASTERS = {
    #    "TIMESTAMP": lambda x: numpy.datetime64(int(x), "s")
    #    if isinstance(x, numpy.float64)
    #    else numpy.datetime64(x),
    "TIMESTAMP": dates.parse_iso
}


def cast(_type):
    """cast a column to a specified type"""
    if _type in VECTORIZED_CASTERS:
        return lambda a: compute.cast(a, VECTORIZED_CASTERS[_type])
    if _type in ITERATIVE_CASTERS:

        def _inner(arr):
            caster = ITERATIVE_CASTERS[_type]
            return [caster(i) for i in arr]

        return _inner
    raise SqlError(f"Unable to cast values in column to `{_type}`")


def safe(func, *parms):
    """execute a function, return None if fails"""
    try:
        return func(*parms)
    except (ValueError, IndexError, TypeError, ArrowNotImplementedError):
        return None


def try_cast(_type):
    """cast a column to a specified type"""
    casters = {
        "BOOLEAN": bool,
        "NUMERIC": float,
        "VARCHAR": str,
        "TIMESTAMP": dates.parse_iso,
    }
    if _type in casters:

        def _inner(arr):
            caster = casters[_type]
            return [safe(caster, i) for i in arr]

        return _inner
    raise SqlError(f"Unable to cast values in column to `{_type}`")


def _repeat_no_parameters(func):
    # call once and repeat
    def _inner(items):
        return [func()] * items

    return _inner


def _iterate_single_parameter(func):
    def _inner(array):
        if isinstance(array, str):
            array = [array]
        return [func(item) for item in array]

    return _inner


def _iterate_double_parameter(func):
    """
    for functions called FUNCTION(field, literal)
    """

    def _inner(array, literal):
        if isinstance(array, str):
            array = [array]
        return [func(item, literal[index]) for index, item in enumerate(array)]

    return _inner


def _iterate_double_parameter_field_second(func):
    """
    for functions called FUNCTION(LITERAL, FIELD)
    """

    def _inner(literal, array):
        if isinstance(array, str):
            array = [array]
        return [func(literal, item) for item in array]

    return _inner


def get_len(obj):
    """len, but nullsafe"""
    if hasattr(obj, "__len__"):
        return len(obj)
    return None


def _raise_exception(text):
    raise SqlError(text)


# fmt:off
# Function definitions optionally include the type and the function.
# The type is needed particularly when returning Python objects that
# the first entry is NONE.
FUNCTIONS = {
    "VERSION": _repeat_no_parameters(get_version),
    # TYPE CONVERSION
    "TIMESTAMP": cast("TIMESTAMP"),
    "BOOLEAN": cast("BOOLEAN"),
    "NUMERIC": cast("NUMERIC"),
    "VARCHAR": cast("VARCHAR"),
    "STRING": cast("VARCHAR"),  # alias for VARCHAR
    "TRY_TIMESTAMP": try_cast("TIMESTAMP"),
    "TRY_BOOLEAN": try_cast("BOOLEAN"),
    "TRY_NUMERIC": try_cast("NUMERIC"),
    "TRY_VARCHAR": try_cast("VARCHAR"),
    "TRY_STRING": try_cast("VARCHAR"),  # alias for VARCHAR
    # STRINGS
    "LEN": _iterate_single_parameter(get_len),  # LENGTH(str) -> int
    "LENGTH": _iterate_single_parameter(get_len),  # LENGTH(str) -> int
    "UPPER": compute.utf8_upper,  # UPPER(str) -> str
    "LOWER": compute.utf8_lower,  # LOWER(str) -> str
    "TRIM": compute.utf8_trim_whitespace,  # TRIM(str) -> str
    "LEFT": string_functions.string_slicer_left,
    "RIGHT": string_functions.string_slicer_right,
    "REVERSE": compute.utf8_reverse,
    "SOUNDEX": string_functions.soundex,
    "TITLE": compute.utf8_title,
    # HASHING & ENCODING
    "HASH": _iterate_single_parameter(lambda x: format(CityHash64(str(x)), "X")),
    "MD5": _iterate_single_parameter(get_md5),
    "RANDOM": number_functions.random,
    "RAND": number_functions.random,
    # OTHER
    "GET": _iterate_double_parameter(_get),  # GET(LIST, index) => LIST[index] or GET(STRUCT, accessor) => STRUCT[accessor]
    "LIST_CONTAINS": _iterate_double_parameter(other_functions.list_contains),
    "LIST_CONTAINS_ANY": _iterate_double_parameter(other_functions.list_contains_any),
    "LIST_CONTAINS_ALL": _iterate_double_parameter(other_functions.list_contains_all),
    "SEARCH": other_functions.search,
    "COALESCE": compute.coalesce,

    # NUMERIC
    "ROUND": number_functions.round,
    "FLOOR": compute.floor,
    "CEIL": compute.ceil,
    "CEILING": compute.ceil,
    "ABS": compute.abs,
    "ABSOLUTE": compute.abs,
    "SIGN": compute.sign,
    "SQRT": compute.sqrt,
    "TRUNC": compute.trunc,
    "TRUNCATE": compute.trunc,
    "PI": _repeat_no_parameters(number_functions.pi),
    # DATES & TIMES
    "DATE_TRUNC": _iterate_double_parameter_field_second(date_trunc),
    "TIME_BUCKET": date_functions.date_floor,
    "DATEDIFF": date_functions.date_diff,
    "DATEPART": date_functions.date_part,
    "DATE_FORMAT": date_functions.date_format,
    "CURRENT_TIME": _repeat_no_parameters(datetime.datetime.utcnow),
    "NOW": _repeat_no_parameters(datetime.datetime.utcnow),
    "CURRENT_DATE": _repeat_no_parameters(datetime.datetime.utcnow().date),
    "TODAY": _repeat_no_parameters(datetime.datetime.utcnow().date),
    "TIME": _repeat_no_parameters(date_functions.get_time),
    "YESTERDAY": _repeat_no_parameters(date_functions.get_yesterday),
    "DATE": _iterate_single_parameter(date_functions.get_date),
    "YEAR": compute.year,
    "MONTH": compute.month,
    "DAY": compute.day,
    "WEEK": compute.iso_week,
    "HOUR": compute.hour,
    "MINUTE": compute.minute,
    "SECOND": compute.second,
    "QUARTER": compute.quarter,

    "ON": lambda x: _raise_exception("`DISTINCT ON` is not supported"),

}
# fmt:on


def is_function(name):
    """
    sugar
    """
    return name.upper() in FUNCTIONS
