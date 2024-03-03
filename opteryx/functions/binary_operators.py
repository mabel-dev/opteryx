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

from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

import numpy
import pyarrow
from orso.types import OrsoTypes
from pyarrow import compute

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

# fmt:on


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


def binary_operations(
    left, left_type: OrsoTypes, operator: str, right, right_type: OrsoTypes
) -> Union[numpy.ndarray, pyarrow.Array]:
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

    if OrsoTypes.INTERVAL in (left_type, right_type):
        from opteryx.custom_types.intervals import INTERVAL_KERNELS

        function = INTERVAL_KERNELS.get((left_type, right_type, operator))
        if function is None:
            from opteryx.exceptions import UnsupportedTypeError

            raise UnsupportedTypeError(
                f"Cannot perform {operator.upper()} on {left_type} and {right_type}."
            )

        return function(left, left_type, right, right_type, operator)

    if (
        operator == "Minus"
        and left_type in (OrsoTypes.DATE, OrsoTypes.TIMESTAMP)
        and right_type in (OrsoTypes.DATE, OrsoTypes.TIMESTAMP)
    ):
        # substracting dates results in an INTERVAL (months, seconds)
        arr = operation(left, right)
        if arr.dtype.name == "timedelta64[D]":
            return pyarrow.array(
                [
                    None if v == -9223372036854775808 else (0, v * 86400)
                    for v in arr.astype(numpy.int64)
                ]
            )
        arr = arr.astype("timedelta64[s]").astype(numpy.int64)
        return pyarrow.array([(0, v) for v in arr.astype(numpy.int64)])

    elif operator == "BitwiseOr":
        if OrsoTypes.VARCHAR in (left_type, right_type):
            return _ip_containment(left, right)

    elif operator == "StringConcat":
        empty = numpy.full(len(left), "")
        joined = compute.binary_join_element_wise(left, right, empty)
        return joined

    return operation(left, right)
