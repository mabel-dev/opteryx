#include "ops.h"
#include <stddef.h>

// Helper function to check if types are compatible for an operation
static int types_compatible(DrakenType left, DrakenType right, DrakenOperation op) {
    // For now, we require exact type match for most operations
    // This can be expanded later to handle type coercion
    
    // Comparison operations can work with same types
    if (op >= OP_EQUALS && op <= OP_LESS_THAN_OR_EQUALS) {
        return left == right;
    }
    
    // Arithmetic operations on numeric types
    if (op >= OP_ADD && op <= OP_DIVIDE) {
        // Check if both are numeric
        int left_numeric = (left >= DRAKEN_INT8 && left <= DRAKEN_INT64) ||
                          (left >= DRAKEN_FLOAT32 && left <= DRAKEN_FLOAT64);
        int right_numeric = (right >= DRAKEN_INT8 && right <= DRAKEN_INT64) ||
                           (right >= DRAKEN_FLOAT32 && right <= DRAKEN_FLOAT64);
        
        if (left_numeric && right_numeric) {
            return left == right;  // For now, require same type
        }
    }
    
    // Boolean operations
    if (op >= OP_AND && op <= OP_XOR) {
        return left == DRAKEN_BOOL && right == DRAKEN_BOOL;
    }
    
    return 0;
}

// Get operation function pointer
BinaryOpFunc get_op(
    DrakenType left_type,
    int left_is_scalar,
    DrakenType right_type,
    int right_is_scalar,
    DrakenOperation operation
) {
    // Check scalarity: Only support vector-vector, vector-scalar, and scalar-scalar
    // Reject scalar-vector combinations
    if (left_is_scalar && !right_is_scalar) {
        return NULL;  // Scalar-vector not supported
    }
    
    // Check if types are compatible
    if (!types_compatible(left_type, right_type, operation)) {
        return NULL;  // Operation not supported for these types
    }
    
    // For now, we return NULL to indicate the operation dispatch is set up
    // but specific operation implementations would be added here
    // The actual operations are implemented in the vector classes
    // This function serves as a type checker and dispatcher
    
    // In a full implementation, this would return function pointers to
    // C++ implementations of the operations. For now, we just validate
    // that the operation is possible and return NULL to indicate
    // the caller should use the Cython-level implementations.
    
    return NULL;
}
