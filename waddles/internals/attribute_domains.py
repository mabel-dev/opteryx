from enum import Enum
from waddles.exceptions import UnsupportedTypeError
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


class WADDLES_TYPES(str, Enum):
    BOOLEAN = "BOOLEAN"
    INTEGER = "INTEGER"
    DOUBLE = "DOUBLE"
    LIST = "LIST"
    VARCHAR = "VARCHAR"
    STRUCT = "STRUCT"
    TIMESTAMP = "TIMESTAMP"
    OTHER = "OTHER"


PYTHON_TYPES = {
    "bool": WADDLES_TYPES.BOOLEAN,
    "datetime": WADDLES_TYPES.TIMESTAMP,
    "dict": WADDLES_TYPES.STRUCT,
    "int": WADDLES_TYPES.INTEGER,
    "float": WADDLES_TYPES.DOUBLE,
    "str": WADDLES_TYPES.VARCHAR,
    "tuple": WADDLES_TYPES.LIST,
}

PARQUET_TYPES = {
    "bool": WADDLES_TYPES.BOOLEAN,
    "timestamp[ms]": WADDLES_TYPES.TIMESTAMP,
    "dict": WADDLES_TYPES.STRUCT,
    "int64": WADDLES_TYPES.INTEGER,
    "double": WADDLES_TYPES.DOUBLE,
    "string": WADDLES_TYPES.VARCHAR,
    "tuple": WADDLES_TYPES.LIST,  
}

COERCABLE_PYTHON_TYPES = {
    "bool": WADDLES_TYPES.BOOLEAN,
    "datetime": WADDLES_TYPES.TIMESTAMP,
    "date": WADDLES_TYPES.TIMESTAMP,
    "dict": WADDLES_TYPES.STRUCT,
    "int": WADDLES_TYPES.INTEGER,
    "float": WADDLES_TYPES.DOUBLE,
    "str": WADDLES_TYPES.VARCHAR,
    "tuple": WADDLES_TYPES.LIST,
    "set": WADDLES_TYPES.LIST,
    "list": WADDLES_TYPES.LIST,
}


def get_coerced_type(python_type):
    if python_type in COERCABLE_PYTHON_TYPES:
        return COERCABLE_PYTHON_TYPES[python_type].name
    return f"unknown ({python_type})"
