# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import base64
import decimal
from base64 import b85decode as _b85decode
from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import field
from decimal import Decimal as _Decimal
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import orjson


def orjson_default(obj):
    if type(obj) is decimal.Decimal:
        return {"__decimal__": str(obj)}
    if type(obj) is bytes:
        return {"__bytes__": base64.b85encode(obj).decode("utf-8")}
    raise TypeError(f"Type not serializable: {type(obj)}")


def decode_object(obj):
    """
    Decode an object that was encoded with orjson_default.

    This is part of an optimization path to avoid loading files, so is written to be fast
    over readability. It uses a stack to traverse the object structure and decode it in place.

    Before this stack-based approach, the recursive version was the third slowest function
    call in performance tests, so this is a significant improvement.
    """
    stack = [(None, None, obj)]  # (parent, key/index, child)
    root = None

    while stack:
        parent, key, item = stack.pop()

        t = type(item)

        if t is dict:
            if "__decimal__" in item:
                val = _Decimal(item["__decimal__"])
            elif "__bytes__" in item:
                val = _b85decode(item["__bytes__"])
            else:
                val = {}
                if parent is not None:
                    parent[key] = val
                else:
                    root = val
                for k in reversed(list(item.keys())):
                    stack.append((val, k, item[k]))
                continue

        elif t is list:
            val = [None] * len(item)
            if parent is not None:
                parent[key] = val
            else:
                root = val
            for i in reversed(range(len(item))):
                stack.append((val, i, item[i]))
            continue

        else:
            val = item

        if parent is not None:
            parent[key] = val
        else:
            root = val

    return root


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
