# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Operation dispatch module for Draken.

This module provides a dispatch system to determine if a binary operation
is supported for given types and scalarity.
"""

from opteryx.draken.core.ops cimport BinaryOpFunc, DrakenOperation, get_op as c_get_op
from opteryx.draken.core.buffers cimport DrakenType
from opteryx.draken.core.buffers cimport (
    DRAKEN_INT8, DRAKEN_INT16, DRAKEN_INT32, DRAKEN_INT64,
    DRAKEN_FLOAT32, DRAKEN_FLOAT64,
    DRAKEN_DATE32, DRAKEN_TIMESTAMP64, DRAKEN_TIME32, DRAKEN_TIME64,
    DRAKEN_INTERVAL,
    DRAKEN_BOOL, DRAKEN_STRING, DRAKEN_ARRAY, DRAKEN_NON_NATIVE
)

# Export type constants
TYPE_INT8 = DRAKEN_INT8
TYPE_INT16 = DRAKEN_INT16
TYPE_INT32 = DRAKEN_INT32
TYPE_INT64 = DRAKEN_INT64
TYPE_FLOAT32 = DRAKEN_FLOAT32
TYPE_FLOAT64 = DRAKEN_FLOAT64
TYPE_DATE32 = DRAKEN_DATE32
TYPE_TIMESTAMP64 = DRAKEN_TIMESTAMP64
TYPE_TIME32 = DRAKEN_TIME32
TYPE_TIME64 = DRAKEN_TIME64
TYPE_INTERVAL = DRAKEN_INTERVAL
TYPE_BOOL = DRAKEN_BOOL
TYPE_STRING = DRAKEN_STRING
TYPE_ARRAY = DRAKEN_ARRAY
TYPE_NON_NATIVE = DRAKEN_NON_NATIVE

cpdef object dispatch_op(
    DrakenType left_type,
    bint left_is_scalar,
    DrakenType right_type,
    bint right_is_scalar,
    DrakenOperation operation
):
    """
    Dispatch a binary operation.

    Parameters
    ----------
    left_type : DrakenType
        Type of the left operand
    left_is_scalar : bool
        Whether the left operand is a scalar
    right_type : DrakenType
        Type of the right operand
    right_is_scalar : bool
        Whether the right operand is a scalar
    operation : DrakenOperation
        The operation to perform

    Returns
    -------
    object
        Function pointer if the operation is supported, None otherwise
    """
    cdef BinaryOpFunc func = c_get_op(
        left_type,
        1 if left_is_scalar else 0,
        right_type,
        1 if right_is_scalar else 0,
        operation
    )

    if func == NULL:
        return None

    # Return the function pointer as an integer that could be used by C code
    return <size_t>func


def get_operation_enum(op_name: str) -> int:
    """
    Get the operation enum value from a string name.

    Parameters
    ----------
    op_name : str
        Name of the operation (e.g., 'add', 'equals')

    Returns
    -------
    int
        The DrakenOperation enum value

    Raises
    ------
    ValueError
        If the operation name is not recognized
    """
    op_map = {
        'add': OP_ADD,
        'subtract': OP_SUBTRACT,
        'multiply': OP_MULTIPLY,
        'divide': OP_DIVIDE,
        'equals': OP_EQUALS,
        'not_equals': OP_NOT_EQUALS,
        'greater_than': OP_GREATER_THAN,
        'greater_than_or_equals': OP_GREATER_THAN_OR_EQUALS,
        'less_than': OP_LESS_THAN,
        'less_than_or_equals': OP_LESS_THAN_OR_EQUALS,
        'and': OP_AND,
        'or': OP_OR,
        'xor': OP_XOR,
    }

    if op_name.lower() not in op_map:
        raise ValueError(f"Unknown operation: {op_name}")

    return op_map[op_name.lower()]


def get_op(left_type, left_is_scalar, right_type, right_is_scalar, operation):
    """
    Get operation function for the given type and scalarity combination.

    This is a convenience wrapper around dispatch_op that matches the exact
    signature requested: get_op(left_type, left_is_scalar, right_type, right_is_scalar, operation).

    Parameters
    ----------
    left_type : DrakenType or int
        Type of the left operand (use TYPE_* constants)
    left_is_scalar : bool
        Whether the left operand is a scalar
    right_type : DrakenType or int
        Type of the right operand (use TYPE_* constants)
    right_is_scalar : bool
        Whether the right operand is a scalar
    operation : DrakenOperation or int or str
        The operation to perform (use get_operation_enum or OP_* constants or string name)

    Returns
    -------
    object or None
        Function pointer if the operation is supported, None otherwise

    Examples
    --------
    >>> from draken.core.ops import get_op, TYPE_INT64
    >>> # Using operation enum
    >>> func = get_op(TYPE_INT64, False, TYPE_INT64, True, 10)  # 10 is OP_EQUALS
    >>> print(func)  # None if not supported, function pointer otherwise

    >>> # Using operation name string
    >>> func = get_op(TYPE_INT64, False, TYPE_INT64, True, 'equals')
    >>> print(func)  # None if not supported, function pointer otherwise
    """
    # If operation is a string, convert it to enum
    if isinstance(operation, str):
        operation = get_operation_enum(operation)

    # Call the dispatch function
    return dispatch_op(
        left_type,
        left_is_scalar,
        right_type,
        right_is_scalar,
        operation
    )
