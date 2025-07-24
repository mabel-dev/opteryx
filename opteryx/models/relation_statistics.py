# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import base64
from collections.abc import Mapping
from collections.abc import Sequence
from decimal import Decimal
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import orjson


def encode_object(obj):
    if isinstance(obj, bytes):
        return {"__bytes__": base64.b85encode(obj).decode("utf-8")}
    if isinstance(obj, Decimal):
        return {"__decimal__": str(obj)}
    if isinstance(obj, Mapping):
        return {k: encode_object(v) for k, v in obj.items()}
    if isinstance(obj, Sequence) and not isinstance(obj, str):
        return [encode_object(v) for v in obj]
    return obj


def decode_object(obj):
    if isinstance(obj, Mapping):
        if "__bytes__" in obj and len(obj) == 1:
            return base64.b85decode(obj["__bytes__"])
        if "__decimal__" in obj and len(obj) == 1:
            return Decimal(obj["__decimal__"])
        return {k: decode_object(v) for k, v in obj.items()}
    if isinstance(obj, Sequence) and not isinstance(obj, str):
        return [decode_object(v) for v in obj]
    return obj


class RelationStatistics:
    """
    Represents an entry in a manifest file, providing metadata about a file.

    Attributes:
        record_count (Optional[int]): The number of records in the file. Defaults to -1.
        record_count_estimate (Optional[int]): The estimated number of records in the file. Defaults to -1.
        null_count (Dict[str, int]): A dictionary containing the number of null values for each column.
        lower_bounds (Dict[str, int]): A dictionary containing the lower bounds for data values.
        upper_bounds (Dict[str, int]): A dictionary containing the upper bounds for data values.
    """

    record_count: int = 0
    """The number of records in the dataset"""
    record_count_estimate: int = 0
    """The estimated number of records in the dataset"""

    null_count: Optional[Dict[str, int]] = None
    lower_bounds: Dict[str, Any] = None
    upper_bounds: Dict[str, Any] = None
    cardinality_estimate: Optional[Dict[str, int]] = None

    raw_distribution_data: List[Tuple[str, str, Any, int]] = []

    def __init__(self):
        self.lower_bounds = {}
        self.upper_bounds = {}

    def update_lower(self, column: str, value: Any, index: Optional[int] = None):
        self.raw_distribution_data.append((column, "lower", value, index))
        if column not in self.lower_bounds or value < self.lower_bounds[column]:
            self.lower_bounds[column] = value

    def update_upper(self, column: str, value: Any, index: Optional[int] = None):
        self.raw_distribution_data.append((column, "upper", value, index))
        if column not in self.upper_bounds or value > self.upper_bounds[column]:
            self.upper_bounds[column] = value

    def add_null(self, column: str, nulls: int):
        if self.null_count is None:
            self.null_count = {}
        if column not in self.null_count:
            self.null_count[column] = 0
        self.null_count[column] += nulls

    def add_count(self, column: str, count: int, index: Optional[int] = None):
        self.raw_distribution_data.append((column, "count", count, index))

    def set_cardinality_estimate(self, column: str, cardinality: int):
        if self.cardinality_estimate is None:
            self.cardinality_estimate = {}
        self.cardinality_estimate[column] = cardinality

    def to_bytes(self) -> bytes:
        """Serialize the RelationStatistics object to bytes using JSON."""
        # Convert all attributes to a serializable dict
        data = {
            "record_count": self.record_count,
            "record_count_estimate": self.record_count_estimate,
            "null_count": self.null_count,
            "lower_bounds": self.lower_bounds,
            "upper_bounds": self.upper_bounds,
            "cardinality_estimate": self.cardinality_estimate,
            "raw_distribution_data": self.raw_distribution_data,
        }
        return orjson.dumps(encode_object(data))

    @classmethod
    def from_bytes(cls, data: bytes) -> "RelationStatistics":
        """Deserialize bytes to a RelationStatistics object using JSON."""
        obj = cls.__new__(cls)
        loaded = decode_object(orjson.loads(data))
        obj.record_count = loaded.get("record_count", 0)
        obj.record_count_estimate = loaded.get("record_count_estimate", 0)
        obj.null_count = loaded.get("null_count")
        obj.lower_bounds = loaded.get("lower_bounds")
        obj.upper_bounds = loaded.get("upper_bounds")
        obj.cardinality_estimate = loaded.get("cardinality_estimate", None)
        obj.raw_distribution_data = loaded.get("raw_distribution_data", [])
        return obj
