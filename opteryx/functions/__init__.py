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
from cityhash import CityHash64
from pyarrow import compute
from pyarrow import ArrowNotImplementedError

import numpy
import pyarrow

import opteryx

from opteryx.exceptions import SqlError, UnsupportedSyntaxError
from opteryx.functions import date_functions
from opteryx.functions import number_functions
from opteryx.functions import other_functions
from opteryx.functions import string_functions
from opteryx.third_party.date_trunc import date_trunc
from opteryx.utils import arrays


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


VECTORIZED_CASTERS = {
    "BOOLEAN": "bool",
    "NUMERIC": "float64",
    "VARCHAR": "string",
    "TIMESTAMP": pyarrow.timestamp("us"),
}


def cast(_type):
    """cast a column to a specified type"""
    if _type in VECTORIZED_CASTERS:
        return lambda a: compute.cast(a, VECTORIZED_CASTERS[_type])

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
        "TIMESTAMP": numpy.datetime64,
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
        return numpy.array([func()] * items)

    return _inner


def _iterate_single_parameter(func):
    def _inner(array):
        return numpy.array([func(item) for item in array])

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
    raise UnsupportedSyntaxError(text)


def _coalesce(*args):
    """wrap the pyarrow coalesce function because NaN != None"""
    coerced = []
    for arg in args:
        # there's no reasonable test to see if we need to do this before we start
        coerced.append(
            [None if value != value else value for value in arg]  # nosemgrep
        )
    return compute.coalesce(*coerced)


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
    "STR": cast("VARCHAR"),
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
    "LEFT": string_functions.string_slicer_left,
    "RIGHT": string_functions.string_slicer_right,
    "REVERSE": compute.utf8_reverse,
    "SOUNDEX": string_functions.soundex,
    "TITLE": compute.utf8_title,
    "CONCAT": string_functions.concat,
    "CONCAT_WS": string_functions.concat_ws,
    "STARTS_WITH": string_functions.starts_w,
    "ENDS_WITH": string_functions.ends_w,
    "SUBSTRING": string_functions.substring,
    "POSITION": _iterate_double_parameter(string_functions.position),
    "TRIM": string_functions.trim,
    "LTRIM": string_functions.ltrim,
    "RTRIM": string_functions.rtrim,

    # HASHING & ENCODING
    "HASH": _iterate_single_parameter(lambda x: format(CityHash64(str(x)), "X")),
    "MD5": _iterate_single_parameter(string_functions.get_md5),
    "SHA1": _iterate_single_parameter(string_functions.get_sha1),
    "SHA224": _iterate_single_parameter(string_functions.get_sha224),
    "SHA256": _iterate_single_parameter(string_functions.get_sha256),
    "SHA384": _iterate_single_parameter(string_functions.get_sha384),
    "SHA512": _iterate_single_parameter(string_functions.get_sha512),
    "RANDOM": number_functions.random_number,
    "RAND": number_functions.random_number,
    "NORMAL": number_functions.random_normal,
    "RANDOM_STRING": _iterate_single_parameter(number_functions.random_string),
    "BASE64_ENCODE": _iterate_single_parameter(string_functions.get_base64_encode),
    "BASE64_DECODE": _iterate_single_parameter(string_functions.get_base64_decode),
    "BASE85_ENCODE": _iterate_single_parameter(string_functions.get_base85_encode),
    "BASE85_DECODE": _iterate_single_parameter(string_functions.get_base85_decode),
    "HEX_ENCODE": _iterate_single_parameter(string_functions.get_hex_encode),
    "HEX_DECODE": _iterate_single_parameter(string_functions.get_hex_decode),

    # OTHER
    "GET": _iterate_double_parameter(_get),  # GET(LIST, index) => LIST[index] or GET(STRUCT, accessor) => STRUCT[accessor]
    "LIST_CONTAINS": _iterate_double_parameter(other_functions.list_contains),
    "LIST_CONTAINS_ANY": _iterate_double_parameter(other_functions.list_contains_any),
    "LIST_CONTAINS_ALL": _iterate_double_parameter(other_functions.list_contains_all),
    "SEARCH": other_functions.search,
    "COALESCE": _coalesce,
    "IFNULL": other_functions.if_null,
    "SORT": _iterate_single_parameter(numpy.sort),
    "GREATEST": _iterate_single_parameter(numpy.nanmax),
    "LEAST": _iterate_single_parameter(numpy.nanmin),
    "IIF": other_functions.iif,
    "GENERATE_SERIES": arrays.generate_series,
    "NULLIF": other_functions.null_if,
    "CASE": other_functions.case_when,

    # NUMERIC
    "ROUND": number_functions.round,
    "FLOOR": compute.floor,
    "CEIL": compute.ceil,
    "CEILING": compute.ceil,
    "ABS": compute.abs,
    "ABSOLUTE": compute.abs,
    "SIGN": compute.sign,
    "SIGNUM": compute.sign,
    "SQRT": compute.sqrt,
    "TRUNC": compute.trunc,
    "TRUNCATE": compute.trunc,
    "PI": _repeat_no_parameters(number_functions.pi),
    "PHI": _repeat_no_parameters(number_functions.phi),
    "E": _repeat_no_parameters(number_functions.e),
    "INT": _iterate_single_parameter(int),
    "INTEGER": _iterate_single_parameter(int),
    "FLOAT": _iterate_single_parameter(float),
    "POWER": compute.power,
    "LN": compute.ln,
    "LOG10": compute.log10,
    "LOG2": compute.log2,
    "LOG": compute.logb,

    # DATES & TIMES
    "DATE_TRUNC": _iterate_double_parameter_field_second(date_trunc),
    "TIME_BUCKET": date_functions.date_floor,
    "DATEDIFF": date_functions.date_diff,
    "DATEPART": date_functions.date_part,
    "DATE_FORMAT": date_functions.date_format,
    "CURRENT_TIME": _repeat_no_parameters(date_functions.get_now),
    "NOW": _repeat_no_parameters(date_functions.get_now),
    "CURRENT_DATE": _repeat_no_parameters(date_functions.get_today),
    "TODAY": _repeat_no_parameters(date_functions.get_today),
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


def functions():
    return list(FUNCTIONS.keys())
