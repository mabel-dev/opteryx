#pragma once
#include <stdint.h>
#include <stddef.h>
#include "buffers.h"

// Operation types
typedef enum {
    OP_ADD = 1,
    OP_SUBTRACT = 2,
    OP_MULTIPLY = 3,
    OP_DIVIDE = 4,
    OP_EQUALS = 10,
    OP_NOT_EQUALS = 11,
    OP_GREATER_THAN = 12,
    OP_GREATER_THAN_OR_EQUALS = 13,
    OP_LESS_THAN = 14,
    OP_LESS_THAN_OR_EQUALS = 15,
    OP_AND = 20,
    OP_OR = 21,
    OP_XOR = 22,
} DrakenOperation;

// Function pointer type for binary operations
// Returns a new buffer or NULL if the operation is not supported
typedef void* (*BinaryOpFunc)(void* left, void* right, int left_is_scalar, int right_is_scalar);

// Get operation function pointer
// Returns NULL if the operation is not supported
BinaryOpFunc get_op(
    DrakenType left_type,
    int left_is_scalar,
    DrakenType right_type,
    int right_is_scalar,
    DrakenOperation operation
);
