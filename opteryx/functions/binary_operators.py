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

import datetime
from typing import Any
from typing import Dict
from typing import Union

import numpy
import pyarrow
from pyarrow import compute

from opteryx.utils import dates

OPERATOR_FUNCTION_MAP: Dict[str, Any] = {
    "Divide": numpy.divide,
    "Minus": numpy.subtract,
    "Modulo": numpy.mod,
    "Multiply": numpy.multiply,
    "Plus": numpy.add,
    "StringConcat": compute.binary_join_element_wise,
    "MyIntegerDivide": lambda left, right: numpy.trunc(numpy.divide(left, right)).astype(
        numpy.int64
    ),
    "BitwiseOr": numpy.bitwise_or,
    "BitwiseAnd": numpy.bitwise_and,
    "BitwiseXor": numpy.bitwise_xor,
    "ShiftLeft": numpy.left_shift,
    "ShiftRight": numpy.right_shift,
}

BINARY_OPERATORS = set(OPERATOR_FUNCTION_MAP.keys())

INTERVALS = (pyarrow.lib.MonthDayNano, pyarrow.lib.MonthDayNanoIntervalArray)

# Also supported by the AST but not implemented

# PGBitwiseXor => ("#"), -- not supported in mysql
# PGBitwiseShiftLeft => ("<<"), -- not supported in mysql
# PGBitwiseShiftRight => (">>"), -- not supported in mysql


def _date_plus_interval(left, right):
    # left is the date, right is the interval
    if type(left) in INTERVALS or (isinstance(left, list) and type(left[0]) in INTERVALS):
        left, right = right, left

    result = []

    for index, date in enumerate(left):
        interval = right[index]
        if hasattr(interval, "value"):
            interval = interval.value
        months = interval.months
        days = interval.days
        nano = interval.nanoseconds

        date = dates.parse_iso(date)
        date = date + datetime.timedelta(days=days)
        date = date + datetime.timedelta(microseconds=(nano * 1000))
        date = dates.add_months(date, months)

        result.append(date)

    return result


def _date_minus_interval(left, right):
    # left is the date, right is the interval
    if type(left) in INTERVALS or (isinstance(left, list) and type(left[0]) in INTERVALS):
        left, right = right, left

    result = []

    for index, date in enumerate(left):
        interval = right[index]
        if hasattr(interval, "value"):
            interval = interval.value
        months = interval.months
        days = interval.days
        nano = interval.nanoseconds

        date = dates.parse_iso(date)
        date = date - datetime.timedelta(days=days)
        date = date - datetime.timedelta(microseconds=(nano * 1000))
        date = dates.add_months(date, (0 - months))

        result.append(date)

    return result


def _has_intervals(left, right):
    return (
        type(left) in INTERVALS
        or type(right) in INTERVALS
        or (isinstance(left, list) and type(left[0]) in INTERVALS)
        or (isinstance(right, list) and type(right[0]) in INTERVALS)
    )


def binary_operations(left, operator: str, right) -> Union[numpy.ndarray, pyarrow.Array]:
    """
    Execute inline operators (e.g. the add in 3 + 4).

    Parameters:
        left: Union[numpy.ndarray, pyarrow.Array]
            The left operand
        operator: str
            The operator to be applied
        right: Union[numpy.ndarray, pyarrow.Array]
            The right operand
    Returns:
        Union[numpy.ndarray, pyarrow.Array]
            The result of the binary operation
    """
    operation = OPERATOR_FUNCTION_MAP.get(operator)

    if operation is None:
        raise NotImplementedError(f"Operator `{operator}` is not implemented!")

    if operator == "Minus" or operator == "Plus":
        if _has_intervals(left, right):
            return (
                _date_minus_interval(left, right)
                if operator == "Minus"
                else _date_plus_interval(left, right)
            )

    if operator == "StringConcat":
        empty = numpy.full(len(left), "")
        joined = compute.binary_join_element_wise(left, right, empty)
        return joined
    return operation(left, right)
