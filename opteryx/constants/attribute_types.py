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


class OPTERYX_TYPES(str, Enum):
    BOOLEAN = "BOOLEAN"
    NUMERIC = "NUMERIC"
    LIST = "LIST"
    VARCHAR = "VARCHAR"
    STRUCT = "STRUCT"
    TIMESTAMP = "TIMESTAMP"
    INTERVAL = "INTERVAL"
    OTHER = "OTHER"

    # FLOAT = DOUBLE
    # INTEGER = INTEGER
    # BYTES = BLOB


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
