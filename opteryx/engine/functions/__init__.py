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
import numpy
import pyarrow

from cityhash import CityHash64
from pyarrow import compute
from pyarrow import ArrowNotImplementedError

import opteryx

from opteryx.engine.functions import date_functions
from opteryx.engine.functions import number_functions
from opteryx.engine.functions import other_functions
from opteryx.engine.functions import string_functions
from opteryx.exceptions import SqlError
from opteryx.third_party.date_trunc import date_trunc
from opteryx.utils import dates


def get_random():
    """get a random number between 0 and 1, three decimal places"""
    range_min, range_max = 0, 1000
    random_int = int.from_bytes(os.urandom(2), "big")
    try:
        return ((random_int % ((range_max + 1) - range_min)) + range_min) / 1000
    except:
        return 0


def get_md5(item):
    """calculate MD5 hash of a value"""
    # this is slow but expected to not have a lot of use
    import hashlib  # delay the import - it's rarely needed

    return hashlib.md5(str(item).encode()).hexdigest()  # nosec - meant to be MD5


def get_version():
    """return opteryx version"""
    return opteryx.__version__


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
            for i in arr:
                yield [caster(i)]

        return _inner
    raise SqlError(f"Unable to cast values in column to `{_type}`")


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
            for i in arr:
                try:
                    yield [caster(i)]
                except (ValueError, TypeError, ArrowNotImplementedError):
                    yield [None]

        return _inner
    raise SqlError(f"Unable to cast values in column to `{_type}`")


def _iterate_no_parameters(func):
    # call the function for each row, this is primarily to support "RANDOM"
    def _inner(items):
        for i in range(items):
            yield [func()]

    return _inner


def _repeat_no_parameters(func):
    # call once and repeat
    def _inner(items):
        return [[func()] * items]

    return _inner


def _iterate_single_parameter(func):
    def _inner(array):
        if isinstance(array, str):
            array = [array]
        for item in array:
            yield [func(item)]

    return _inner


def _iterate_double_parameter(func):
    """
    for functions called FUNCTION(field, literal)
    """

    def _inner(array, literal):
        if isinstance(array, str):
            array = [array]
        for item in array:
            yield [func(item, literal)]

    return _inner


def _iterate_double_parameter_field_second(func):
    """
    for functions called FUNCTION(LITERAL, FIELD)
    """

    def _inner(literal, array):
        if isinstance(array, str):
            array = [array]
        for item in array:
            yield [func(literal, item)]

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
    "VERSION": (None, _repeat_no_parameters(get_version),),
    # TYPE CONVERSION
    "TIMESTAMP": (None, cast("TIMESTAMP"),),
    "BOOLEAN": (None, cast("BOOLEAN"),),
    "NUMERIC": (None, cast("NUMERIC"),),
    "VARCHAR": (None, cast("VARCHAR"),),
    "STRING": (None, cast("VARCHAR"),),  # alias for VARCHAR
    "TRY_TIMESTAMP": (None, try_cast("TIMESTAMP"),),
    "TRY_BOOLEAN": (None, try_cast("BOOLEAN"),),
    "TRY_NUMERIC": (None, try_cast("NUMERIC"),),
    "TRY_VARCHAR": (None, try_cast("VARCHAR"),),
    "TRY_STRING": (None, try_cast("VARCHAR"),),  # alias for VARCHAR
    # STRINGS
    "LEN": (None, _iterate_single_parameter(get_len),),  # LENGTH(str) -> int
    "LENGTH": (None, _iterate_single_parameter(get_len),),  # LENGTH(str) -> int
    "UPPER": (None, compute.utf8_upper,),  # UPPER(str) -> str
    "LOWER": (None, compute.utf8_lower,),  # LOWER(str) -> str
    "TRIM": (None, compute.utf8_trim_whitespace,),  # TRIM(str) -> str
    "LEFT": (None, string_functions.string_slicer_left,),
    "RIGHT": (None, string_functions.string_slicer_right,),
    # HASHING & ENCODING
    "HASH": (None, _iterate_single_parameter(lambda x: format(CityHash64(str(x)), "X")),),
    "MD5": (None, _iterate_single_parameter(get_md5),),
    "RANDOM": (None, _iterate_no_parameters(get_random),),  # return a random number 0-0.999
    # OTHER
    "GET": (None, _iterate_double_parameter(_get),),  # GET(LIST, index) => LIST[index] or GET(STRUCT, accessor) => STRUCT[accessor]
    "LIST_CONTAINS": (None, _iterate_double_parameter(other_functions.list_contains),),
    "LIST_CONTAINS_ANY": (None, _iterate_double_parameter(other_functions.list_contains_any),),
    "LIST_CONTAINS_ALL": (None, _iterate_double_parameter(other_functions.list_contains_all),),
    "SEARCH": (None, other_functions.search,),
    "COALESCE": (None, compute.coalesce,),

    # NUMERIC
    "ROUND": (None, compute.round,),
    "FLOOR": (None, compute.floor,),
    "CEIL": (None, compute.ceil,),
    "CEILING": (None, compute.ceil,),
    "ABS": (None, compute.abs,),
    "ABSOLUTE": (None, compute.abs,),
    "TRUNC": (None, compute.trunc,),
    "TRUNCATE": (None, compute.trunc,),
    "PI": (None, _repeat_no_parameters(number_functions.pi)),
    # DATES & TIMES
    "DATE_TRUNC": (None, _iterate_double_parameter_field_second(date_trunc),),
    "TIME_BUCKET": (None, compute.floor_temporal),
    "DATEDIFF": (pyarrow.float64(), date_functions.date_diff,),
    "DATEPART": (None, date_functions.date_part,),
    "DATE_FORMAT": (None, compute.strftime),
    "CURRENT_TIME": (None, _repeat_no_parameters(datetime.datetime.utcnow),),
    "NOW": (None, _repeat_no_parameters(datetime.datetime.utcnow),),
    "CURRENT_DATE": (None, _repeat_no_parameters(datetime.datetime.utcnow().date),),
    "TODAY": (None, _repeat_no_parameters(datetime.datetime.utcnow().date),),
    "TIME": (None, _repeat_no_parameters(date_functions.get_time),),
    "YESTERDAY": (None, _repeat_no_parameters(date_functions.get_yesterday),),
    "DATE": (None, _iterate_single_parameter(date_functions.get_date),),
    "YEAR": (None, compute.year,),
    "MONTH": (None, compute.month,),
    "DAY": (None, compute.day,),
    "WEEK": (None, compute.iso_week,),
    "HOUR": (None, compute.hour,),
    "MINUTE": (None, compute.minute,),
    "SECOND": (None, compute.second,),
    "QUARTER": (None, compute.quarter,),

    "ON": (None, lambda x: _raise_exception("`DISTINCT ON` is not supported"),)

}
# fmt:on


def is_function(name):
    """
    sugar
    """
    return name.upper() in FUNCTIONS
