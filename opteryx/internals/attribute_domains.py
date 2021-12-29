from enum import Enum
from opteryx.exceptions import UnsupportedTypeError
import datetime


def coerce(var):
    """
    Relations only support a subset of types, if we know how to translate a type
    into a supported type, do it.
    """
    t = type(var)
    if t in (int, float, tuple, bool, str, datetime.datetime, dict):
        return var
    if t in (list, set):
        return tuple(var)
    if t in (datetime.date,):
        return datetime.datetime(t.year, t.month, t.day)
    if var is None:
        return var
    raise UnsupportedTypeError(
        f"Attributes of type `{t}` are not supported - the value was `{var}`"
    )


class opteryx_TYPES(str, Enum):
    BOOLEAN = "BOOLEAN"
    INTEGER = "INTEGER"
    DOUBLE = "DOUBLE"
    LIST = "LIST"
    VARCHAR = "VARCHAR"
    STRUCT = "STRUCT"
    TIMESTAMP = "TIMESTAMP"
    OTHER = "OTHER"


PYTHON_TYPES = {
    "bool": opteryx_TYPES.BOOLEAN,
    "datetime": opteryx_TYPES.TIMESTAMP,
    "dict": opteryx_TYPES.STRUCT,
    "int": opteryx_TYPES.INTEGER,
    "float": opteryx_TYPES.DOUBLE,
    "str": opteryx_TYPES.VARCHAR,
    "tuple": opteryx_TYPES.LIST,
}

PARQUET_TYPES = {
    "bool": opteryx_TYPES.BOOLEAN,
    "timestamp[ms]": opteryx_TYPES.TIMESTAMP,
    "dict": opteryx_TYPES.STRUCT,
    "int64": opteryx_TYPES.INTEGER,
    "double": opteryx_TYPES.DOUBLE,
    "string": opteryx_TYPES.VARCHAR,
    "tuple": opteryx_TYPES.LIST,  
}

COERCABLE_PYTHON_TYPES = {
    "bool": opteryx_TYPES.BOOLEAN,
    "datetime": opteryx_TYPES.TIMESTAMP,
    "date": opteryx_TYPES.TIMESTAMP,
    "dict": opteryx_TYPES.STRUCT,
    "int": opteryx_TYPES.INTEGER,
    "float": opteryx_TYPES.DOUBLE,
    "str": opteryx_TYPES.VARCHAR,
    "tuple": opteryx_TYPES.LIST,
    "set": opteryx_TYPES.LIST,
    "list": opteryx_TYPES.LIST,
}


def get_coerced_type(python_type):
    if python_type in COERCABLE_PYTHON_TYPES:
        return COERCABLE_PYTHON_TYPES[python_type].name
    return f"unknown ({python_type})"
