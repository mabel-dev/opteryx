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

import numpy
import orjson
import pyarrow
from orso.cityhash import CityHash64
from pyarrow import ArrowNotImplementedError
from pyarrow import compute

import opteryx
from opteryx.exceptions import FunctionNotFoundError
from opteryx.exceptions import IncorrectTypeError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.functions import date_functions
from opteryx.functions import number_functions
from opteryx.functions import other_functions
from opteryx.functions import string_functions
from opteryx.utils import dates
from opteryx.utils import series


def _get(value, item):
    try:
        if isinstance(value, dict):
            return value.get(item)
        return value[int(item)]
    except ValueError as err:
        if isinstance(value, str):
            raise IncorrectTypeError(
                f"VARCHAR values can only be subscripted with NUMERIC values"
            ) from err
        if isinstance(value, dict):
            raise IncorrectTypeError(
                "STRUCT values must be subscripted with VARCHAR values"
            ) from err
        if isinstance(value, list):
            raise IncorrectTypeError("LIST values must be subscripted with NUMERIC values") from err
        else:
            raise IncorrectTypeError(f"Cannot subscript {type(value).__name__} values") from err
    except (KeyError, IndexError, TypeError):
        return None


VECTORIZED_CASTERS = {
    "BOOLEAN": "bool",
    "DOUBLE": "float64",
    "INTEGER": "int64",
    "DECIMAL": pyarrow.decimal128(14),
    "VARCHAR": "string",
    "TIMESTAMP": pyarrow.timestamp("us"),
    "DATE": pyarrow.date32(),
}


def fixed_value_function(function, context):
    from orso.types import OrsoTypes

    if function in ("VERSION",):
        return OrsoTypes.VARCHAR, opteryx.__version__
    if function in ("NOW", "CURRENT_TIME", "UTC_TIMESTAMP"):
        return OrsoTypes.TIMESTAMP, numpy.datetime64(context.connection.connected_at)
    if function in ("CURRENT_DATE", "TODAY"):
        return OrsoTypes.DATE, numpy.datetime64(context.connection.connected_at.date())
    if function in ("YESTERDAY",):
        return OrsoTypes.DATE, numpy.datetime64(
            context.connection.connected_at.date() - datetime.timedelta(days=1), "D"
        )
    if function == "CONNECTION_ID":
        return OrsoTypes.INTEGER, context.connection.connection_id
    if function == "DATABASE":
        return OrsoTypes.VARCHAR, context.connection.schema or "DEFAULT"
    if function == "USER":
        return OrsoTypes.VARCHAR, context.connection.user or "ANONYMOUS"
    if function == "PI":
        return OrsoTypes.DOUBLE, 3.14159265358979323846264338327950288419716939937510
    if function == "PHI":
        # the golden ratio
        return OrsoTypes.DOUBLE, 1.61803398874989484820458683436563811772030917980576
    if function == "E":
        # eulers number
        return OrsoTypes.DOUBLE, 2.71828182845904523536028747135266249775724709369995
    return None, None


def cast(_type):
    """cast a column to a specified type"""
    if _type in VECTORIZED_CASTERS:
        return lambda a: compute.cast(a, VECTORIZED_CASTERS[_type])

    raise FunctionNotFoundError(message=f"Internal function to cast values to `{_type}` not found.")


def safe(func, *parms):
    """execute a function, return None if fails"""
    try:
        return func(*parms)
    except (ValueError, IndexError, TypeError, ArrowNotImplementedError):
        return None


def try_cast(_type):
    """cast a column to a specified type"""
    import decimal

    casters = {
        "BOOLEAN": bool,
        "DOUBLE": float,
        "INTEGER": int,
        "DECIMAL": decimal.Decimal,
        "VARCHAR": str,
        "TIMESTAMP": numpy.datetime64,
        "STRUCT": orjson.loads,
        "DATE": lambda x: dates.parse_iso(x).date(),
    }
    if _type in casters:

        def _inner(arr):
            caster = casters[_type]
            return [safe(caster, i) for i in arr]

        return _inner
    raise FunctionNotFoundError(message=f"Internal function to cast values to `{_type}` not found.")


def _iterate_single_parameter(func):
    def _inner(array):
        return numpy.array(list(map(func, array)))

    return _inner


def _sort(func):
    def _inner(array):
        return pyarrow.array([func(item) for item in array])

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


def _coalesce(*arrays):
    """
    Element-wise coalesce function for multiple numpy arrays.
    Selects the first non-None item in each row across the input arrays.

    Parameters:
        arrays: tuple of numpy arrays

    Returns:
        numpy array with coalesced values
    """
    # Start with an array full of None values
    result = numpy.array(arrays[0], dtype=object)

    mask = result == None

    for arr in arrays[1:]:
        mask = numpy.array([None if value != value else value for value in result]) == None
        numpy.copyto(result, arr, where=mask)

    return result


# fmt:off
# Function definitions optionally include the type and the function.
# The type is needed particularly when returning Python objects that
# the first entry is NONE.
FUNCTIONS = {
    "VERSION": lambda x: None, # *
    "CONNECTION_ID": lambda x: None, # *
    "DATABASE": lambda x: None, # *
    "USER": lambda x: None, # *

    # TYPE CONVERSION
    "TIMESTAMP": cast("TIMESTAMP"),
    "BOOLEAN": cast("BOOLEAN"),
    "NUMERIC": cast("DOUBLE"),
    "INTEGER": cast("INTEGER"),
    "DOUBLE": cast("DOUBLE"),
    "FLOAT": cast("DOUBLE"),
    "DECIMAL": cast("DECIMAL"),
    "VARCHAR": cast("VARCHAR"),
    "STRING": cast("VARCHAR"),  # alias for VARCHAR
    "STR": cast("VARCHAR"),
    "STRUCT": _iterate_single_parameter(lambda x: orjson.loads(str(x))),
    "DATE":  cast("DATE"),
    "TRY_TIMESTAMP": try_cast("TIMESTAMP"),
    "TRY_BOOLEAN": try_cast("BOOLEAN"),
    "TRY_NUMERIC": try_cast("DOUBLE"),
    "TRY_VARCHAR": try_cast("VARCHAR"),
    "TRY_STRING": try_cast("VARCHAR"),  # alias for VARCHAR
    "TRY_STRUCT": try_cast("STRUCT"),
    "TRY_INTEGER": try_cast("INTEGER"),
    "TRY_DECIMAL": try_cast("DECIMAL"),
    "TRY_DOUBLE": try_cast("DOUBLE"),
    "TRY_DATE": try_cast("DATE"),

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
    "LEVENSHTEIN": string_functions.levenshtein,
    "SPLIT": string_functions.split,

    # HASHING & ENCODING
    "HASH": _iterate_single_parameter(lambda x: hex(CityHash64(str(x)))[2:]),
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
    "SORT": _sort(numpy.sort),
    "GREATEST": _iterate_single_parameter(numpy.nanmax),
    "LEAST": _iterate_single_parameter(numpy.nanmin),
    "IIF": other_functions.iif,
#    "GENERATE_SERIES": series.generate_series,
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
    "PI": lambda x: None, # *
    "PHI": lambda x: None, # *
    "E": lambda x: None, # *
    "INT": _iterate_single_parameter(int),
    "POWER": number_functions.safe_power,
    "LN": compute.ln,
    "LOG10": compute.log10,
    "LOG2": compute.log2,
    "LOG": compute.logb,

    # DATES & TIMES
    "DATE_TRUNC": _iterate_double_parameter_field_second(dates.date_trunc),
    "TIME_BUCKET": date_functions.date_floor,
    "DATEDIFF": date_functions.date_diff,
    "TIMEDIFF": date_functions.time_diff,
    "DATEPART": date_functions.date_part,
    "DATE_FORMAT": date_functions.date_format,
    "CURRENT_TIME": lambda x: None, # *
    "UTC_TIMESTAMP": lambda x: None, # *
    "NOW": lambda x: None, # *
    "CURRENT_DATE": lambda x: None, # *
    "TODAY": lambda x: None, # *
#    "TIME": _repeat_no_parameters(date_functions.get_time),
    "YESTERDAY": lambda x: None, # *
#    "DATE": lambda x: compute.cast(x, "date32"), #_iterate_single_parameter(date_functions.get_date),
    "YEAR": compute.year,
    "MONTH": compute.month,
    "DAY": compute.day,
    "WEEK": compute.iso_week,
    "HOUR": compute.hour,
    "MINUTE": compute.minute,
    "SECOND": compute.second,
    "QUARTER": compute.quarter,
    "FROM_UNIXTIME": date_functions.from_unixtimestamp,
    "UNIXTIME": date_functions.unixtime,

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
