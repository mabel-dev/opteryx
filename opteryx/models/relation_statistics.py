# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import base64
import decimal
from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import orjson


def orjson_default(obj):
    if isinstance(obj, decimal.Decimal):
        return {"__decimal__": str(obj)}
    if isinstance(obj, bytes):
        return {"__bytes__": base64.b85encode(obj).decode("utf-8")}
    raise TypeError(f"Type not serializable: {type(obj)}")


def decode_object(obj):
    if isinstance(obj, dict):
        if set(obj) == {"__decimal__"}:
            return decimal.Decimal(obj["__decimal__"])
        if set(obj) == {"__bytes__"}:
            return base64.b85decode(obj["__bytes__"])
        return {k: decode_object(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [decode_object(v) for v in obj]
    return obj


@dataclass
class RelationStatistics:
    record_count: int = 0
    record_count_estimate: int = 0
    null_count: Optional[Dict[str, int]] = None
    lower_bounds: Dict[str, Any] = field(default_factory=dict)
    upper_bounds: Dict[str, Any] = field(default_factory=dict)
    cardinality_estimate: Optional[Dict[str, int]] = None
    raw_distribution_data: List[Tuple[str, str, Any, int]] = field(default_factory=list)

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
        self.null_count[column] = self.null_count.get(column, 0) + nulls

    def add_count(self, column: str, count: int, index: Optional[int] = None):
        self.raw_distribution_data.append((column, "count", count, index))

    def set_cardinality_estimate(self, column: str, cardinality: int):
        if self.cardinality_estimate is None:
            self.cardinality_estimate = {}
        self.cardinality_estimate[column] = cardinality

    def to_bytes(self) -> bytes:
        return orjson.dumps(asdict(self), default=orjson_default)

    @classmethod
    def from_bytes(cls, data: bytes) -> "RelationStatistics":
        loaded = decode_object(orjson.loads(data))
        return cls(**loaded)
