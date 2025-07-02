# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
These are a set of functions that can be applied to data.
"""

import datetime
import decimal
import inspect
import time

import numpy
import orjson
import pyarrow
from orso.types import OrsoTypes
from pyarrow import ArrowNotImplementedError
from pyarrow import compute

import opteryx
from opteryx.compiled.list_ops.list_contains_any import list_contains_any
from opteryx.compiled.list_ops.list_encode_utf8 import list_encode_utf8 as to_blob
from opteryx.exceptions import FunctionExecutionError
from opteryx.exceptions import IncorrectTypeError
from opteryx.functions import date_functions
from opteryx.functions import number_functions
from opteryx.functions import other_functions
from opteryx.functions import string_functions
from opteryx.third_party.cyan4973.xxhash import hash_bytes
from opteryx.utils import dates


def array_encode_utf8(arr):
    try:
        # array_encode_utf8 is fast but brittle
        return to_blob(arr)
    except:
        return [None if s is None else str(s).encode() for s in arr]


def _get(array, key):
    # Determine the type of the first element (assuming homogeneous array)
    first_element = next((item for item in array if item is not None), None)
    if first_element is None:
        return numpy.full(len(array), None)

    key = key[0]
    if isinstance(first_element, dict):
        # Handle dict type
        from opteryx.compiled.list_ops import cython_arrow_op

        return cython_arrow_op(array, key)
    if isinstance(key, str):
        from opteryx.third_party.tktech import csimdjson as simdjson

        if hasattr(array, "to_numpy"):
            array = array.to_numpy(False)

        def extract(doc, elem):
            value = simdjson.Parser().parse(doc).get(elem)  # type:ignore
            if hasattr(value, "as_list"):
                return value.as_list()
            if hasattr(value, "as_dict"):
                return value.as_dict()
            return value

        try:
            return pyarrow.array([None if d is None else extract(d, key) for d in array])
        except ValueError:
            raise IncorrectTypeError(
                "VARCHAR subscripts can only be used on STRUCT or columns with valid JSON documents."
            )
    try:
        index = int(key)
    except Exception:
        raise IncorrectTypeError("VARCHAR and ARRAY values must be subscripted with NUMERIC values")
    if isinstance(first_element, (list, str, pyarrow.ListScalar, bytes, numpy.ndarray)):
        from opteryx.compiled.list_ops.list_get_element import list_get_element

        return list_get_element(array, key)

    raise IncorrectTypeError(f"Cannot subscript {type(first_element).__name__} values")


def _get_string(array, key):
    key = key[0]
    return pyarrow.array(
        [None if i != i else str(i) for i in (item.get(key) for item in array)],
        type=pyarrow.string(),
    )


def cast_varchar(arr):
    if len(arr) > 0 and all(i is None or isinstance(i, dict) for i in arr[:100]):
        return [orjson.dumps(n).decode() if n is not None else None for n in arr]
    return compute.cast(arr, "string")


def cast_blob(arr):
    """
    Checks if the first 100 elements of arr are either None or bytes.
    If true, returns the original array. Otherwise, converts all elements
    to strings and then encodes them to bytes.

    Parameters:
    arr (list): The input list to be checked and potentially converted.

    Returns:
    list: The original list if all elements in the first 100 are None or bytes,
          otherwise a new list with all elements converted to bytes.
    """
    if len(arr) > 0 and all(i is None or isinstance(i, bytes) for i in arr[:100]):
        return arr
    return [None if a is None else str(a).encode() for a in arr]


def fixed_value_function(function, context):
    from orso.types import OrsoTypes

    if function in ("VERSION",):
        return OrsoTypes.VARCHAR, opteryx.__version__
    if function in ("NOW", "UTC_TIMESTAMP"):
        return OrsoTypes.TIMESTAMP, numpy.datetime64(context.connection.connected_at)
    if function in ("CURRENT_TIME",):
        # CURRENT_TIME is an alias for NOW, so we return the same value
        return OrsoTypes.TIME, context.connection.connected_at.time()
    if function in ("CURRENT_TIMESTAMP",):
        # CURRENT_TIMESTAMP is an alias for NOW, so we return the same value
        return OrsoTypes.TIMESTAMP, numpy.datetime64(context.connection.connected_at, "us")
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
    if function == "UTC_TIMESTAMP":
        # UTC timestamp
        return OrsoTypes.TIMESTAMP, numpy.datetime64(datetime.datetime.utcnow(), "us")
    if function == "UNIXTIME":
        # We should only ever get here if the function is called without parameters
        return OrsoTypes.INTEGER, context.connection.connected_at.timestamp()
    if function == "YEAR":
        return OrsoTypes.INTEGER, context.connection.connected_at.year
    if function == "MONTH":
        return OrsoTypes.INTEGER, context.connection.connected_at.month
    if function == "DAY":
        return OrsoTypes.INTEGER, context.connection.connected_at.day
    if function == "HOUR":
        return OrsoTypes.INTEGER, context.connection.connected_at.hour
    if function == "MINUTE":
        return OrsoTypes.INTEGER, context.connection.connected_at.minute
    if function == "SECOND":
        return OrsoTypes.INTEGER, context.connection.connected_at.second
    return None, None


def safe(func, *parms, **kwargs):
    """execute a function, return None if fails"""
    try:
        return func(*parms, **kwargs)
    except (
        ValueError,
        IndexError,
        TypeError,
        ArrowNotImplementedError,
        AttributeError,
        decimal.InvalidOperation,
    ) as e:
        return None


def try_cast(_type):
    """cast a column to a specified type"""

    def _inner(arr, *args):
        args = [a[0] for a in args]
        kwargs = {}

        caster = OrsoTypes[_type].parse

        sig = inspect.signature(caster)
        params = list(sig.parameters.values())[1:]  # skip the first param (`value`)

        kwargs = {param.name: arg for param, arg in zip(params, args)}

        return [safe(caster, i, **kwargs) for i in arr]

    return _inner


def cast(_type):
    """cast a column to a specified type"""

    def _inner(arr, *args):
        args = [a[0] for a in args]
        kwargs = {}

        caster = OrsoTypes[_type].parse

        if _type == "DECIMAL":
            # DECIMAL requires special handling for precision and scale
            if len(args) == 2:
                kwargs["precision"] = args[0]
                kwargs["scale"] = args[1]
            elif len(args) == 1:
                kwargs["precision"] = args[0]
                kwargs["scale"] = 0
        elif _type in ("VARCHAR", "BLOB") and len(args) == 1:
            # VARCHAR and BLOB can take a single argument for length
            kwargs["length"] = args[0]
        elif _type == "ARRAY" and len(args) == 1:
            # ARRAY can take a single argument for the element type
            kwargs["element_type"] = args[0]

        return [caster(i, **kwargs) for i in arr]

    return _inner


def _iterate_single_parameter(func):
    def _inner(array):
        return pyarrow.array(list(map(func, array)))

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
        return pyarrow.array(func(item, literal[index]) for index, item in enumerate(array))

    return _inner


def get_len(obj):
    """len, but nullsafe"""
    if hasattr(obj, "__len__"):
        return len(obj)
    if hasattr(obj, "length"):  # Some Arrow scalar types have .length property
        return obj.length
    if hasattr(obj, "nbytes"):  # NumPy scalar types have .nbytes
        return obj.nbytes
    if hasattr(obj, "as_py") and isinstance(obj.as_py(), (bytes, str)):  # PyArrow string scalar
        return len(obj.as_py())
    return None


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


def select_values(boolean_arrays, value_arrays):
    """
    Build a result array based on boolean conditions and corresponding value arrays.

    Parameters:
    - boolean_arrays: List[np.ndarray], list of boolean arrays representing conditions.
    - value_arrays: List[np.ndarray], list of arrays with values corresponding to each condition.

    Returns:
    - np.ndarray: Result array with selected values or False where no condition is met.
    """
    # Ensure the input lists are not empty and have the same length
    if not boolean_arrays or not value_arrays or len(boolean_arrays) != len(value_arrays):
        raise ValueError("Input lists must be non-empty and of the same length.")

    # Initialize the result array with False, assuming no condition will be met
    result = numpy.full(len(boolean_arrays[0]), None)

    # Iterate over pairs of boolean and value arrays
    for condition, values in zip(reversed(boolean_arrays), reversed(value_arrays)):
        # Update the result array where the condition is True
        numpy.putmask(result, condition, values)

    return result


def sleep(x):
    time.sleep(x[0] / 1000)  # Sleep for x[0] milliseconds
    return x[0]


DEPRECATED_FUNCTIONS = {
    "LIST": "ARRAY_AGG",  # deprecated, removed 0.21.0
    "MAXIMUM": "MAX",  # deprecated, removed 0.21.0
    "MINIMUM": "MIN",  # deprecated, removed 0.21.0
    "AVERAGE": "AVG",  # deprecated, removed 0.21.0
    "CEILING": "CEIL",  # deprecated, removed 0.21.0
    "ABSOLUTE": "ABS",  # deprecated, removed 0.21.0
    "TRUNCATE": "TRUNC",  # deprecated, removed 0.21.0
    "LIST_CONTAINS_ANY": "ARRAY_CONTAINS_ANY",  # deprecated, removed 0.22.0
    "LIST_CONTAINS_ALL": "ARRAY_CONTAINS_ALL",  # deprecated, removed 0.22.0
    "STRUCT": None,  # deprecated, removed 0.22.0,
    "NUMERIC": "DOUBLE",  # deprecated, removed 0.22.0
    "LIST_CONTAINS": "ARRAY_CONTAINS",  # deprecated, removed 0.24.0
    "STR": "VARCHAR",  # deprecated, removed 0.24.0
    "STRING": "VARCHAR",  # deprecated, removed 0.24.0
    "FLOAT": "DOUBLE",  # deprecated, removed 0.24.0
    "TRY_NUMERIC": "TRY_DOUBLE",  # deprecated, removed 0.24.0
    "TRY_STRING": "TRY_VARCHAR",  # deprecated, removed 0.24.0
    "TRY_STRUCT": None,  # deprecated, removed 0.24.0
    "LEN": "LENGTH",  # deprecated, removed 0.24.0
}

# fmt:off
# Function definition look up table
# <function_name>: (function, return_type, cost_estimate)
# Note: * Return_type of VARIANT is used for functions that can return any type of data and the
#         actual type will be determined at runtime.
#       * cost_estimate is a float that represents the estimated time to execute a function
#         one million times - this is currently always 1.0 and is not used. 
FUNCTIONS = {

    # These functions are rewritten at plan time, they're here so the function resolver in the 
    # first phase of planning can find them
    "VERSION": (lambda x: None, "VARCHAR", 1.0),
    "CONNECTION_ID": (lambda x: None, "VARCHAR", 1.0),
    "DATABASE": (lambda x: None, "VARCHAR", 1.0),
    "USER": (lambda x: None, "VARCHAR", 1.0),
    "STARTS_WITH": (lambda x: None, "BOOLEAN", 1.0),  # always rewritten as a LIKE
    "ENDS_WITH": (lambda x: None, "BOOLEAN", 1.0),  # always rewritten as a LIKE
    # DEBUG: "SLEEP": (lambda x: [sleep(x)], OrsoTypes.NULL, 10.0), # SLEEP is only available in 'debug' mode

    # TYPE CONVERSION
    "ARRAY": (other_functions.array_cast, "VARIANT", 1.0),
    "TIMESTAMP": (lambda x: compute.cast(x, pyarrow.timestamp("us")), "TIMESTAMP", 1.0),
    "BOOLEAN": (lambda x: compute.cast(x, "bool"), "BOOLEAN", 1.0),
    "INTEGER": (lambda x: compute.cast(x, "int64", safe=False), "INTEGER", 1.0),
    "DOUBLE": (lambda x: compute.cast(x, "float64"), "DOUBLE", 1.0),
    "DECIMAL": (cast("DECIMAL"), "DECIMAL", 1.0),
    "VARCHAR": (cast("VARCHAR"), "VARCHAR", 1.0),
    "DATE": (lambda x: compute.cast(x, pyarrow.date32()), "DATE", 1.0),
    "PASSTHRU": (lambda x: x, "VARIANT", 1.0),
    "BLOB": (cast("BLOB"), "BLOB", 1.0),
    "TRY_ARRAY": (other_functions.array_cast_safe, "VARIANT", 1.0),
    "TRY_TIMESTAMP": (try_cast("TIMESTAMP"), "TIMESTAMP", 1.0),
    "TRY_BOOLEAN": (try_cast("BOOLEAN"), "BOOLEAN", 1.0),
    "TRY_VARCHAR": (try_cast("VARCHAR"), "VARCHAR", 1.0),
    "TRY_BLOB": (try_cast("BLOB"), "BLOB", 1.0),
    "TRY_INTEGER": (try_cast("INTEGER"), "INTEGER", 1.0),
    "TRY_DECIMAL": (try_cast("DECIMAL"), "DECIMAL", 1.0),
    "TRY_DOUBLE": (try_cast("DOUBLE"), "DOUBLE", 1.0),
    "TRY_DATE": (try_cast("DATE"), "DATE", 1.0),

    # CHARS
    "CHAR": (string_functions.to_char, "VARCHAR", 1.0),
    "ASCII": (string_functions.to_ascii, "INTEGER", 1.0),

    # STRINGS
    "LENGTH": (_iterate_single_parameter(get_len), "INTEGER", 1.0),  # LENGTH(str) -> int
    "UPPER": (compute.utf8_upper, "VARCHAR", 1.0),  # UPPER(str) -> str
    "LOWER": (compute.utf8_lower, "VARCHAR", 1.0),  # LOWER(str) -> str
    "LEFT": (string_functions.string_slicer_left, "VARCHAR", 1.0),
    "RIGHT": (string_functions.string_slicer_right, "VARCHAR", 1.0),
    "REVERSE": (compute.utf8_reverse, "VARCHAR", 1.0),
    "SOUNDEX": (string_functions.soundex, "VARCHAR", 1.0),
    "TITLE": (compute.utf8_title, "VARCHAR", 1.0),
    "CONCAT": (string_functions.concat, "VARCHAR", 1.0),
    "CONCAT_WS": (string_functions.concat_ws, "VARCHAR", 1.0),
    "SUBSTRING": (string_functions.substring, "VARCHAR", 1.0),
    "POSITION": (_iterate_double_parameter(string_functions.position), "INTEGER", 1.0),
    "TRIM": (string_functions.trim, "VARCHAR", 1.0),
    "LTRIM": (string_functions.ltrim, "VARCHAR", 1.0),
    "RTRIM": (string_functions.rtrim, "VARCHAR", 1.0),
    "LPAD": (string_functions.left_pad, "VARCHAR", 1.0),
    "RPAD": (string_functions.right_pad, "VARCHAR", 1.0),
    "LEVENSHTEIN": (string_functions.levenshtein, "INTEGER", 1.0),
    "SPLIT": (string_functions.split, "ARRAY<VARCHAR>", 1.0),
    "MATCH_AGAINST": (string_functions.match_against, "BOOLEAN", 1.0),
    "REGEXP_REPLACE": (string_functions.regex_replace, "BLOB", 1.0),

    # HASHING & ENCODING
    "HASH": (_iterate_single_parameter(lambda x: hex(hash_bytes(str(x).encode()))[2:]), "BLOB", 1.0),
    "MD5": (_iterate_single_parameter(string_functions.get_md5), "BLOB", 1.0),
    "SHA1": (_iterate_single_parameter(string_functions.get_sha1), "BLOB", 1.0),
    "SHA224": (_iterate_single_parameter(string_functions.get_sha224), "BLOB", 1.0),
    "SHA256": (_iterate_single_parameter(string_functions.get_sha256), "BLOB", 1.0),
    "SHA384": (_iterate_single_parameter(string_functions.get_sha384), "BLOB", 1.0),
    "SHA512": (_iterate_single_parameter(string_functions.get_sha512), "BLOB", 1.0),
    "RANDOM": (number_functions.random_number, "DOUBLE", 1.0),
    "RAND": (number_functions.random_number, "DOUBLE", 1.0),
    "NORMAL": (number_functions.random_normal, "DOUBLE", 1.0),
    "RANDOM_STRING": (number_functions.random_string, "BLOB", 1.0),
    "BASE64_ENCODE": (_iterate_single_parameter(string_functions.get_base64_encode), "BLOB", 1.0),
    "BASE64_DECODE": (_iterate_single_parameter(string_functions.get_base64_decode), "BLOB", 1.0),
    "BASE85_ENCODE": (_iterate_single_parameter(string_functions.get_base85_encode), "BLOB", 1.0),
    "BASE85_DECODE": (_iterate_single_parameter(string_functions.get_base85_decode), "BLOB", 1.0),
    "HEX_ENCODE": (_iterate_single_parameter(string_functions.get_hex_encode), "BLOB", 1.0),
    "HEX_DECODE": (_iterate_single_parameter(string_functions.get_hex_decode), "BLOB", 1.0),

    # OTHER
    "GET": (_get, "VARIANT", 1.0),
    "GET_STRING": (_get_string, "VARCHAR", 1.0),
    "ARRAY_CONTAINS": (_iterate_double_parameter(other_functions.list_contains), "BOOLEAN", 1.0),
    "ARRAY_CONTAINS_ANY": (lambda x, y: list_contains_any(x, set(y[0])), "BOOLEAN", 1.0),
    "ARRAY_CONTAINS_ALL": (other_functions.list_contains_all, "BOOLEAN", 1.0),
    "SEARCH": (other_functions.search, "BOOLEAN", 1.0),
    "COALESCE": (_coalesce, "VARIANT", 1.0),
    "IFNULL": (other_functions.if_null, "VARIANT", 1.0),
    "IFNOTNULL": (other_functions.if_not_null, "VARIANT", 1.0),
    "SORT": (_sort(numpy.sort), "ARRAY", 1.0),
    "GREATEST": (_iterate_single_parameter(numpy.nanmax), "VARIANT", 1.0),
    "LEAST": (_iterate_single_parameter(numpy.nanmin), "VARIANT", 1.0),
    "IIF": (numpy.where, "VARIANT", 1.0),
    "NULLIF": (other_functions.null_if, "VARIANT", 1.0),
    "CASE": (select_values, "VARIANT", 1.0),
    "JSONB_OBJECT_KEYS": (other_functions.jsonb_object_keys, "ARRAY<VARCHAR>", 1.0),
    "HUMANIZE": (other_functions.humanize, "VARCHAR", 1.0),

    # Vector
    "COSINE_SIMILARITY": (other_functions.cosine_similarity, "DOUBLE", 1.0),

    # NUMERIC
    "ROUND": (number_functions.round, "DOUBLE", 1.0),
    "FLOOR": (number_functions.floor, "DOUBLE", 1.0),
    "CEIL": (number_functions.ceiling, "DOUBLE", 1.0),
    "ABS": (compute.abs, "VARIANT", 1.0),
    "SIGN": (compute.sign, "INTEGER", 1.0),
    "SIGNUM": (compute.sign, "INTEGER", 1.0),
    "SQRT": (compute.sqrt, "DOUBLE", 1.0),
    "TRUNC": (compute.trunc, "INTEGER", 1.0),
    "PI": (lambda x: None, "DOUBLE", 1.0),
    "PHI": (lambda x: None, "DOUBLE", 1.0),
    "E": (lambda x: None, "DOUBLE", 1.0),
    "INT": (_iterate_single_parameter(int), "INTEGER", 1.0),
    "POWER": (number_functions.safe_power, "DOUBLE", 1.0),
    "LN": (compute.ln, "DOUBLE", 1.0),
    "LOG10": (compute.log10, "DOUBLE", 1.0),
    "LOG2": (compute.log2, "DOUBLE", 1.0),
    "LOG": (compute.logb, "DOUBLE", 1.0),

    # DATES & TIMES
    "DATE_TRUNC": (dates.date_trunc, "TIMESTAMP", 1.0),
    "TIME_BUCKET": (date_functions.date_floor, "TIMESTAMP", 1.0),
    "DATEDIFF": (date_functions.date_diff, "INTEGER", 1.0),
    "TIMEDIFF": (date_functions.time_diff, "INTEGER", 1.0),
    "DATEPART": (date_functions.date_part, "VARIANT", 1.0),
    "DATE_FORMAT": (date_functions.date_format, "VARCHAR", 1.0),
    "CURRENT_TIME": (lambda x: None, "TIME", 1.0),
    "CURRENT_TIMESTAMP": (lambda x: None, "TIMESTAMP", 1.0),
    "UTC_TIMESTAMP": (lambda x: None, "INTEGER", 1.0),
    "NOW": (lambda x: None, "TIMESTAMP", 1.0),
    "CURRENT_DATE": (lambda x: None, "DATE", 1.0),
    "TODAY": (lambda x: None, "TIMESTAMP", 1.0),
    "YESTERDAY": (lambda x: None, "TIMESTAMP", 1.0),
    "YEAR": (compute.year, "INTEGER", 1.0),
    "MONTH": (compute.month, "INTEGER", 1.0),
    "DAY": (compute.day, "INTEGER", 1.0),
    "WEEK": (compute.iso_week, "INTEGER", 1.0),
    "HOUR": (compute.hour, "INTEGER", 1.0),
    "MINUTE": (compute.minute, "INTEGER", 1.0),
    "SECOND": (compute.second, "INTEGER", 1.0),
    "QUARTER": (compute.quarter, "INTEGER", 1.0),
    "FROM_UNIXTIME": (date_functions.from_unixtimestamp, "TIMESTAMP", 1.0),
    "UNIXTIME": (date_functions.unixtime, "INTEGER", 1.0),
}

# fmt:on


def apply_function(function: str = None, *parameters):
    compressed = False

    if (
        not isinstance(parameters[0], int)
        and function
        not in (
            "IFNULL",
            "LIST_CONTAINS_ANY",
            "LIST_CONTAINS_ALL",
            "ARRAY_CONTAINS_ANY",
            "ARRAY_CONTAINS_ALL",
            "CONCAT",
            "CONCAT_WS",
            "IIF",
            "COALESCE",
            "SUBSTRING",
            "CASE",
        )
        and all(isinstance(arr, numpy.ndarray) for arr in parameters)
    ):
        morsel_size = len(parameters[0])
        null_positions = numpy.zeros(morsel_size, dtype=numpy.bool_)

        for parameter in parameters:
            # compute null positions
            null_positions = numpy.logical_or(
                null_positions,
                compute.is_null(parameter, nan_is_null=True),
            )

        # Early exit if all values are null
        if null_positions.all():
            return numpy.array([None] * morsel_size)

        if null_positions.any():
            # if we have nulls and the value array is a numpy arrays, we can speed things
            # up by removing the nulls from the calculations, we add the rows back in
            # later
            valid_positions = ~null_positions
            parameters = [arr.compress(valid_positions) for arr in parameters]
            compressed = True

    try:
        interim_results = FUNCTIONS[function][0](*parameters)
    except FunctionExecutionError as e:
        raise e
    except Exception as e:
        raise FunctionExecutionError(e) from e

    if compressed:
        # fill the result set
        results = numpy.array([None] * morsel_size, dtype=object)
        numpy.place(results, valid_positions, interim_results)
        return results

    return interim_results


def is_function(name: str) -> bool:
    """
    sugar
    """
    return name.upper() in FUNCTIONS


def functions() -> list[str]:
    return list(FUNCTIONS.keys())
