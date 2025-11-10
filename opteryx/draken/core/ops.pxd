# cython: language_level=3

from libc.stdint cimport int32_t
from opteryx.draken.core.buffers cimport DrakenType

cdef extern from "ops.h":
    # Operation types
    ctypedef enum DrakenOperation:
        OP_ADD
        OP_SUBTRACT
        OP_MULTIPLY
        OP_DIVIDE
        OP_EQUALS
        OP_NOT_EQUALS
        OP_GREATER_THAN
        OP_GREATER_THAN_OR_EQUALS
        OP_LESS_THAN
        OP_LESS_THAN_OR_EQUALS
        OP_AND
        OP_OR
        OP_XOR

    # Function pointer type for binary operations
    ctypedef void* (*BinaryOpFunc)(void* left, void* right, int left_is_scalar, int right_is_scalar)

    # Get operation function pointer
    BinaryOpFunc get_op(
        DrakenType left_type,
        int left_is_scalar,
        DrakenType right_type,
        int right_is_scalar,
        DrakenOperation operation
    )
