# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

import numpy
import pyarrow
from orso.types import OrsoTypes
from pyarrow import compute

from opteryx.compiled import list_ops
from opteryx.third_party.tktech import csimdjson as simdjson

# Initialize simdjson parser once
parser = simdjson.Parser()


def ArrowOp(documents, elements) -> pyarrow.Array:
    """JSON Selector"""
    element = elements[0]

    # Fast path: if the documents are dicts, delegate to the cython optimized op
    if len(documents) > 0 and isinstance(documents[0], dict):
        return list_ops.cython_arrow_op(documents, element)

    if hasattr(documents, "to_numpy"):
        documents = documents.to_numpy(zero_copy_only=False)

    # Function to extract value from a document
    def extract(doc: bytes, elem: Union[bytes, str]) -> Any:
        value = parser.parse(doc).get(elem)  # type:ignore
        if hasattr(value, "as_list"):
            return value.as_list()
        if hasattr(value, "as_dict"):
            return value.mini
        return value

    try:
        extracted_values = [None if d is None else extract(d, element) for d in documents]
    except ValueError as err:
        from opteryx.exceptions import IncorrectTypeError

        raise IncorrectTypeError("The `->` operator can only be used on JSON documents.") from err

    # Return the result as a PyArrow array
    return pyarrow.array(extracted_values)


def LongArrowOp(documents, elements) -> pyarrow.Array:
    """JSON Selector (as byte string)"""
    element = elements[0]

    if len(documents) > 0 and isinstance(documents[0], dict):
        return list_ops.cython_long_arrow_op(documents, element)

    if hasattr(documents, "to_numpy"):
        documents = documents.to_numpy(zero_copy_only=False)

    def extract(doc: bytes, elem: Union[bytes, str]) -> bytes:
        value = simdjson.Parser().parse(doc).get(elem)  # type:ignore
        if hasattr(value, "mini"):
            return value.mini  # type:ignore
        return None if value is None else str(value).encode()

    try:
        extracted_values = [None if d is None else extract(d, element) for d in documents]
    except ValueError as err:
        from opteryx.exceptions import IncorrectTypeError

        raise IncorrectTypeError("The `->>` operator can only be used on JSON documents.") from err

    # Return the result as a PyArrow array
    return pyarrow.array(extracted_values, type=pyarrow.binary())


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

    from opteryx.compiled.functions.ip_address import ip_in_cidr

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
    "Arrow": ArrowOp,
    "LongArrow": LongArrowOp
}

BINARY_OPERATORS = set(OPERATOR_FUNCTION_MAP.keys())

# fmt:on
