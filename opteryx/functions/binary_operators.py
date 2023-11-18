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

import array
import datetime
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
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
DATES = (
    numpy.datetime64,
    pyarrow.lib.Date32Array,
)
LISTS = (pyarrow.Array, numpy.ndarray, list, array.ArrayType)
STRINGS = (str, numpy.str_)

# Also supported by the AST but not implemented

# PGBitwiseXor => ("#"), -- not supported in mysql
# PGBitwiseShiftLeft => ("<<"), -- not supported in mysql
# PGBitwiseShiftRight => (">>"), -- not supported in mysql


def _date_plus_interval(left, right):
    # left is the date, right is the interval
    if isinstance(left, INTERVALS) or (isinstance(left, LISTS) and type(left[0]) in INTERVALS):
        left, right = right, left

    result = []

    for index, date in enumerate(left):
        interval = right[index]
        if hasattr(interval, "value"):
            interval = interval.value
        months = interval.months
        days = interval.days
        nanoseconds = interval.nanoseconds

        date = dates.parse_iso(date)
        # Subtract days and nanoseconds (as microseconds)
        date += datetime.timedelta(days=days, microseconds=nanoseconds // 1000)
        date = dates.add_months(date, months)

        result.append(date)

    return result


def _date_minus_interval(left, right):
    # left is the date, right is the interval
    if isinstance(left, INTERVALS) or (isinstance(left, LISTS) and type(left[0]) in INTERVALS):
        left, right = right, left

    result = []

    for index, date in enumerate(left):
        interval = right[index]
        if hasattr(interval, "value"):
            interval = interval.value
        months = interval.months
        days = interval.days
        nanoseconds = interval.nanoseconds

        date = dates.parse_iso(date)
        # Subtract days and nanoseconds (as microseconds)
        date -= datetime.timedelta(days=days, microseconds=nanoseconds // 1000)
        date = dates.add_months(date, (0 - months))

        result.append(date)

    return result


def _ip_containment(left: List[Optional[str]], right: List[str]) -> List[Optional[bool]]:
    """
    Check if each IP address in 'left' is contained within the network specified in 'right'.

    Parameters:
        left: List[Optional[str]]
            List of IP addresses as strings.
        right: List[str]
            List containing the network as a string.

    Returns:
        List[Optional[bool]]:
            A list of boolean values indicating if each corresponding IP in 'left' is in 'right'.
    """
    from ipaddress import IPv4Address
    from ipaddress import IPv4Network

    network = IPv4Network(right[0], strict=False)
    return [(IPv4Address(ip) in network) if ip else None for ip in left]


def _either_side_is_type(left, right, types):
    return (
        _check_type(left, types)
        or _check_type(right, types)
        or (_check_type(left, LISTS) and _check_type(left[0], types))
        or (_check_type(right, LISTS) and _check_type(right[0], types))
    )


def _both_sides_are_type(left, right, types):
    return (
        _check_type(left, types) or _check_type(left, LISTS) and _check_type(left[0], types)
    ) and (_check_type(right, types) or _check_type(right, LISTS) and _check_type(right[0], types))


def _is_date_only(obj):
    obj_0 = obj[0]
    return isinstance(obj_0, pyarrow.lib.Date32Scalar) or (
        isinstance(obj_0, numpy.datetime64) and obj_0.dtype == "datetime64[D]"
    )


def _check_type(obj, types: Union[type, Tuple[type, ...]]) -> bool:
    return any(isinstance(obj, t) for t in types)


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

    if operator in ["Minus", "Plus"]:
        if _either_side_is_type(left, right, INTERVALS):
            return (
                _date_minus_interval(left, right)
                if operator == "Minus"
                else _date_plus_interval(left, right)
            )
        if _both_sides_are_type(left, right, DATES):
            if _is_date_only(left) and _is_date_only(right):
                return pyarrow.array(
                    [
                        pyarrow.MonthDayNano((0, v.view(numpy.int64), 0))
                        for v in operation(left, right)
                    ],
                    type=pyarrow.month_day_nano_interval(),
                )

    elif operator == "BitwiseOr":
        if _either_side_is_type(left, right, STRINGS):
            return _ip_containment(left, right)

    elif operator == "StringConcat":
        empty = numpy.full(len(left), "")
        joined = compute.binary_join_element_wise(left, right, empty)
        return joined
    return operation(left, right)
