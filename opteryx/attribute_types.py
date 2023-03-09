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
import datetime
from decimal import Decimal
from enum import Enum

from pyarrow import lib


class OPTERYX_TYPES(str, Enum):
    BOOLEAN = "BOOLEAN"
    NUMERIC = "NUMERIC"
    LIST = "LIST"
    VARCHAR = "VARCHAR"
    STRUCT = "STRUCT"
    TIMESTAMP = "TIMESTAMP"
    INTERVAL = "INTERVAL"
    OTHER = "OTHER"


class TOKEN_TYPES(str, Enum):
    BOOLEAN = "BOOLEAN"
    NUMERIC = "NUMERIC"
    LIST = "LIST"
    VARCHAR = "VARCHAR"
    STRUCT = "STRUCT"
    TIMESTAMP = "TIMESTAMP"
    OTHER = "OTHER"
    IDENTIFIER = "IDENTIFIER"
    WILDCARD = "WILDCARD"
    QUERY_PLAN = "QUERY_PLAN"
    FUNCTION = "FUNCTION"
    INTERVAL = "INTERVAL"


PYTHON_TYPES = {
    "bool": OPTERYX_TYPES.BOOLEAN,
    "datetime": OPTERYX_TYPES.TIMESTAMP,
    "date": OPTERYX_TYPES.TIMESTAMP,
    "dict": OPTERYX_TYPES.STRUCT,
    "int": OPTERYX_TYPES.NUMERIC,  # INTEGER
    "float": OPTERYX_TYPES.NUMERIC,  # FLOAT
    "float64": OPTERYX_TYPES.NUMERIC,  # IS THIS USED?
    "Decimal": OPTERYX_TYPES.NUMERIC,  # DECIMAL
    "str": OPTERYX_TYPES.VARCHAR,
    "tuple": OPTERYX_TYPES.LIST,
    "list": OPTERYX_TYPES.LIST,
    "set": OPTERYX_TYPES.LIST,
    # INTERVAL?
}

PARQUET_TYPES = {
    "bool": OPTERYX_TYPES.BOOLEAN,
    "timestamp[ms]": OPTERYX_TYPES.TIMESTAMP,
    "timestamp[s]": OPTERYX_TYPES.TIMESTAMP,
    "timestamp[us]": OPTERYX_TYPES.TIMESTAMP,
    "date32[day]": OPTERYX_TYPES.TIMESTAMP,
    "dict": OPTERYX_TYPES.STRUCT,
    "decimal128(38,9)": OPTERYX_TYPES.NUMERIC,
    "int64": OPTERYX_TYPES.NUMERIC,
    "float64": OPTERYX_TYPES.NUMERIC,
    "double": OPTERYX_TYPES.NUMERIC,
    "string": OPTERYX_TYPES.VARCHAR,
    "tuple": OPTERYX_TYPES.LIST,
    "array": OPTERYX_TYPES.LIST,
    "object": OPTERYX_TYPES.VARCHAR,
    "month_day_nano_interval": OPTERYX_TYPES.INTERVAL,
}


def determine_type(_type):
    """
    Determine the type
    """
    if _type.startswith("struct"):
        return OPTERYX_TYPES.STRUCT
    if _type.startswith("list"):
        return OPTERYX_TYPES.LIST
    return PARQUET_TYPES.get(_type, f"OTHER ({_type})")


def parquet_type_map(parquet_type):
    if parquet_type == lib.Type_NA:
        return None
    if parquet_type == lib.Type_BOOL:
        return bool
    if parquet_type in {lib.Type_STRING, lib.Type_LARGE_STRING}:
        return str
    if parquet_type in {
        lib.Type_INT8,
        lib.Type_INT16,
        lib.Type_INT32,
        lib.Type_INT64,
        lib.Type_UINT8,
        lib.Type_UINT16,
        lib.Type_UINT32,
        lib.Type_UINT64,
    }:
        return int
    if parquet_type in {lib.Type_HALF_FLOAT, lib.Type_FLOAT, lib.Type_DOUBLE}:
        return float
    if parquet_type in {lib.Type_DECIMAL128, lib.Type_DECIMAL256}:
        return Decimal
    if parquet_type in {lib.Type_DATE32, lib.Type_DATE64}:
        return datetime.datetime
    if parquet_type in {lib.Type_TIME32, lib.Type_TIME64}:
        return datetime.time
    if parquet_type == lib.Type_INTERVAL_MONTH_DAY_NANO:  # lib.Type_DURATION?
        return datetime.timedelta
    if parquet_type in {lib.Type_LIST, lib.Type_LARGE_LIST, lib.Type_FIXED_SIZE_LIST}:
        return list
    if parquet_type in {lib.Type_STRUCT, lib.Type_MAP}:
        return dict
    if parquet_type in {lib.Type_BINARY, lib.Type_LARGE_BINARY}:
        return bytes
    # _UNION_TYPES = {lib.Type_SPARSE_UNION, lib.Type_DENSE_UNION}
    raise ValueError(f"Unable to map parquet type {parquet_type}")
