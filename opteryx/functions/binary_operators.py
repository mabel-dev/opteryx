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

# fmt:off
OPERATOR_FUNCTION_MAP: Dict[str, Any] = {
    "Divide": numpy.divide,
    "Minus": numpy.subtract,
    "Modulo": numpy.mod,
    "Multiply": numpy.multiply,
    "Plus": numpy.add,
    "StringConcat": compute.binary_join_element_wise,
    "MyIntegerDivide": lambda left, right: numpy.trunc(numpy.divide(left, right)).astype(numpy.int64),
    "BitwiseOr": numpy.bitwise_or,
    "BitwiseAnd": numpy.bitwise_and,
    "BitwiseXor": numpy.bitwise_xor,
    "ShiftLeft": numpy.left_shift,
    "ShiftRight": numpy.right_shift,
}

BINARY_OPERATORS = set(OPERATOR_FUNCTION_MAP.keys())

INTERVALS = (pyarrow.lib.MonthDayNano, pyarrow.lib.MonthDayNanoIntervalArray)
DATES = (numpy.datetime64, pyarrow.lib.Date32Array)
LISTS = (pyarrow.Array, numpy.ndarray, list, array.ArrayType)
STRINGS = (str, numpy.str_)
# fmt:on

# Also supported by the AST but not implemented

# PGBitwiseXor => ("#"), -- not supported in mysql
# PGBitwiseShiftLeft => ("<<"), -- not supported in mysql
# PGBitwiseShiftRight => (">>"), -- not supported in mysql


def add_months_numpy(dates, months_to_add):
    """
    Adds a specified number of months to dates in a numpy array, adjusting for end-of-month overflow.

    Parameters:
    - dates: np.ndarray of dates (numpy.datetime64)
    - months_to_add: int, the number of months to add to each date

    Returns:
    - np.ndarray: Adjusted dates
    """
    # Convert dates to 'M' (month) granularity for addition
    months = dates.astype("datetime64[M]")

    # Add months (broadcasts the scalar value across the array)
    new_dates = months + numpy.timedelta64(months_to_add, "M")

    # Calculate the last day of the new month for each date
    last_day_of_new_month = new_dates + numpy.timedelta64(1, "M") - numpy.timedelta64(1, "D")

    # Calculate the day of the month for each original date
    day_of_month = dates - months

    # Adjust dates that would overflow their new month
    overflow_mask = day_of_month > (last_day_of_new_month - new_dates)
    adjusted_dates = numpy.where(overflow_mask, last_day_of_new_month, new_dates + day_of_month)

    return adjusted_dates.astype("datetime64[us]")


def _date_plus_interval(left: numpy.ndarray, right):
    """
    Adds intervals to dates, utilizing integer arithmetic for performance improvements.
    """
    if isinstance(left, INTERVALS) or (isinstance(left, LISTS) and type(left[0]) in INTERVALS):
        left, right = right, left

    interval = right[0].value
    delta = (interval.days * 24 * 3600 * 1_000_000_000) + interval.nanoseconds
    result = left.astype("datetime64[ns]") + delta

    # Handle months separately, requiring special logic
    if interval.months:
        for index in range(len(result)):
            result[index] = add_months_numpy(result[index], interval.months)

    return result


def _date_minus_interval(left, right):
    if isinstance(left, INTERVALS) or (isinstance(left, LISTS) and type(left[0]) in INTERVALS):
        left, right = right, left

    interval = right[0].value
    delta = (interval.days * 24 * 3600 * 1_000_000_000) + interval.nanoseconds
    result = left.astype("datetime64[ns]") - delta

    # Handle months separately, requiring special logic
    if interval.months:
        for index in range(len(result)):
            result[index] = add_months_numpy(result[index], 0 - interval.months)

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

    from opteryx.compiled.functions import ip_in_cidr

    try:
        return ip_in_cidr(left, str(right[0]))
    except (IndexError, AttributeError, ValueError) as err:
        from opteryx.exceptions import IncorrectTypeError

        raise IncorrectTypeError(
            "The `|` operator can be used as bitwise OR or IP address containment only."
        ) from err


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


def _check_type(obj, types: Tuple[type, ...]) -> bool:
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

    if operator in ("Minus", "Plus"):
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
