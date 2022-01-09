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
Helper routines for handling types between different dialects.
"""
from enum import Enum
from opteryx.exceptions import UnsupportedTypeError


def coerce_types(value):
    """
    Relations only support a subset of types, if we know how to translate a type
    into a supported type, do it.
    """
    import datetime
    import decimal

    t = type(value)
    if t in (int, float, tuple, bool, str, datetime.datetime, dict, decimal.Decimal):
        return value
    if t in (list, set):
        return tuple(value)
    if t in (datetime.date,):
        return datetime.datetime(t.year, t.month, t.day)
    if value is None:
        return value
    raise UnsupportedTypeError(
        f"Attributes of type `{t}` are not supported - the value was `{value}`"
    )


def coerce_values(value, value_type):
    #
    pass


class OPTERYX_TYPES(str, Enum):
    BOOLEAN = "BOOLEAN"
    INTEGER = "INTEGER"
    DOUBLE = "DOUBLE"
    LIST = "LIST"
    VARCHAR = "VARCHAR"
    STRUCT = "STRUCT"
    TIMESTAMP = "TIMESTAMP"
    OTHER = "OTHER"


OPTERYX_TYPE_NAMES = {
    OPTERYX_TYPES.BOOLEAN: "BOOLEAN",
    OPTERYX_TYPES.INTEGER: "INTEGER",
    OPTERYX_TYPES.DOUBLE: "DOUBLE",
    OPTERYX_TYPES.LIST: "LIST",
    OPTERYX_TYPES.VARCHAR: "VARCHAR",
    OPTERYX_TYPES.STRUCT: "STRUCT",
    OPTERYX_TYPES.TIMESTAMP: "TIMESTAMP",
    OPTERYX_TYPES.OTHER: "OTHER",
}


PYTHON_TYPES = {
    "bool": OPTERYX_TYPES.BOOLEAN,
    "datetime": OPTERYX_TYPES.TIMESTAMP,
    "dict": OPTERYX_TYPES.STRUCT,
    "int": OPTERYX_TYPES.INTEGER,
    "float": OPTERYX_TYPES.DOUBLE,
    "Decimal": OPTERYX_TYPES.DOUBLE,
    "str": OPTERYX_TYPES.VARCHAR,
    "tuple": OPTERYX_TYPES.LIST,
}

PARQUET_TYPES = {
    "bool": OPTERYX_TYPES.BOOLEAN,
    "timestamp[ms]": OPTERYX_TYPES.TIMESTAMP,
    "dict": OPTERYX_TYPES.STRUCT,
    "int64": OPTERYX_TYPES.INTEGER,
    "double": OPTERYX_TYPES.DOUBLE,
    "string": OPTERYX_TYPES.VARCHAR,
    "tuple": OPTERYX_TYPES.LIST,
}

COERCABLE_PYTHON_TYPES = {
    "bool": OPTERYX_TYPES.BOOLEAN,
    "datetime": OPTERYX_TYPES.TIMESTAMP,
    "date": OPTERYX_TYPES.TIMESTAMP,
    "dict": OPTERYX_TYPES.STRUCT,
    "int": OPTERYX_TYPES.INTEGER,
    "float": OPTERYX_TYPES.DOUBLE,
    "Decimal": OPTERYX_TYPES.DOUBLE,
    "str": OPTERYX_TYPES.VARCHAR,
    "tuple": OPTERYX_TYPES.LIST,
    "set": OPTERYX_TYPES.LIST,
    "list": OPTERYX_TYPES.LIST,
}


def get_coerced_type(python_type):
    if python_type in COERCABLE_PYTHON_TYPES:
        return COERCABLE_PYTHON_TYPES[python_type].name
    return f"unknown ({python_type})"
