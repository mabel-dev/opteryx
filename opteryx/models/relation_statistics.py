from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple


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
