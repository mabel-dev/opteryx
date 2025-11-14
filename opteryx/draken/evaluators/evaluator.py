"""
Compiled expression evaluator for Draken.

This module provides the main evaluate() function that takes a morsel and
an expression tree, and evaluates the expression efficiently using compiled
evaluators for common patterns.
"""

import hashlib
import logging
from typing import Any
from typing import Callable
from typing import Dict

from opteryx.draken.evaluators.expression import BinaryExpression
from opteryx.draken.evaluators.expression import ColumnExpression
from opteryx.draken.evaluators.expression import Expression
from opteryx.draken.evaluators.expression import LiteralExpression
from opteryx.draken.evaluators.expression import UnaryExpression
from opteryx.draken.evaluators.generator import ensure_compiled_evaluator
from opteryx.draken.evaluators.generator import has_compiled_evaluator
from opteryx.draken.morsels.morsel import Morsel
from opteryx.draken.vectors import BoolMask
from opteryx.draken.vectors.vector import Vector

logger = logging.getLogger(__name__)

# Try to import compiled maskops helper (fast byte-wise ops). If unavailable,
# functions will be None and Python fallback will be used.
try:
    from opteryx.draken.compiled import and_mask
    from opteryx.draken.compiled import or_mask
    from opteryx.draken.compiled import xor_mask
except Exception:
    and_mask = or_mask = xor_mask = None


class CompiledEvaluator:
    """
    A compiled evaluator for a specific expression pattern.

    Compiled evaluators are cached and reused for common expression patterns
    to avoid repeated interpretation overhead.
    """

    def __init__(self, expression: Expression, evaluator_func: Callable, optimized: bool = False):
        """
        Create a compiled evaluator.

        Args:
            expression: The expression pattern this evaluator handles
            evaluator_func: The compiled evaluation function
        """
        self.expression = expression
        self.evaluator_func = evaluator_func
        # optimized==True means this evaluator is a purpose-built compiled
        # implementation for the pattern (single-pass comparison/boolean)
        self.optimized = optimized

    def evaluate(self, morsel: Morsel) -> Vector:
        """
        Evaluate the expression over a morsel.

        Args:
            morsel: The morsel to evaluate over

        Returns:
            Vector: Result vector
        """
        return self.evaluator_func(morsel)


# Cache for compiled evaluators
_evaluator_cache: Dict[int, CompiledEvaluator] = {}


def _get_expression_hash(expr: Expression) -> int:
    """Get a hash for an expression to use for caching."""
    return hash(expr)


def _compile_binary_comparison(operation: str, left: Expression, right: Expression) -> Callable:
    """
    Compile a binary comparison operation into an optimized evaluator.

    This generates an optimized single-pass evaluator for common patterns like:
    - column == literal
    - column > literal
    - column1 == column2
    """

    # Pattern: column OP literal (e.g., x == 1)
    if isinstance(left, ColumnExpression) and isinstance(right, LiteralExpression):
        col_name = left.column_name
        literal_value = right.value

        def evaluator(morsel: Morsel) -> Vector:
            # Get the column vector
            col_bytes = col_name.encode("utf-8")
            vec = morsel.column(col_bytes)

            # Call the appropriate vector comparison method
            if operation == "equals":
                res = vec.equals(literal_value)
            elif operation == "not_equals":
                res = vec.not_equals(literal_value)
            elif operation == "greater_than":
                res = vec.greater_than(literal_value)
            elif operation == "greater_than_or_equals":
                res = vec.greater_than_or_equals(literal_value)
            elif operation == "less_than":
                res = vec.less_than(literal_value)
            elif operation == "less_than_or_equals":
                res = vec.less_than_or_equals(literal_value)
            else:
                raise ValueError(f"Unknown comparison operation: {operation}")

            return BoolMask(res) if not isinstance(res, Vector) else res

        return evaluator

    # Pattern: column1 OP column2 (e.g., x == y)
    if isinstance(left, ColumnExpression) and isinstance(right, ColumnExpression):
        left_col = left.column_name
        right_col = right.column_name

        def evaluator(morsel: Morsel) -> Vector:
            # Get both column vectors
            left_vec = morsel.column(left_col.encode("utf-8"))
            right_vec = morsel.column(right_col.encode("utf-8"))

            # Call the appropriate vector-vector comparison method
            if operation == "equals":
                res = left_vec.equals_vector(right_vec)
            elif operation == "not_equals":
                res = left_vec.not_equals_vector(right_vec)
            elif operation == "greater_than":
                res = left_vec.greater_than_vector(right_vec)
            elif operation == "greater_than_or_equals":
                res = left_vec.greater_than_or_equals_vector(right_vec)
            elif operation == "less_than":
                res = left_vec.less_than_vector(right_vec)
            elif operation == "less_than_or_equals":
                res = left_vec.less_than_or_equals_vector(right_vec)
            else:
                raise ValueError(f"Unknown comparison operation: {operation}")

            return BoolMask(res) if not isinstance(res, Vector) else res

        return evaluator

    # Fall back to generic evaluation
    return None


def _compile_binary_boolean(operation: str, left: Expression, right: Expression) -> Callable:
    """
    Compile a binary boolean operation (AND, OR, XOR) into an optimized evaluator.

    For compound expressions like (x == 1 AND y == 'england'), this can evaluate
    both conditions in a single pass and combine them efficiently.
    """

    def _combine_boolean_results(left_result, right_result):
        """
        Combine two boolean-like results which may be:
        - A Draken BoolVector
        - A Cython memoryview (int8_t[::1]) returned by comparison ops
        - A Python list/sequence of bools

        Return a BoolVector when possible, otherwise return a memoryview of int8.
        """
        # If both are Vector-like and expose bitwise methods, try to use them
        try:
            # Prefer native BoolVector/BoolMask methods if available
            if hasattr(left_result, "and_vector"):
                if operation == "and":
                    return left_result.and_vector(right_result)
                elif operation == "or":
                    return left_result.or_vector(right_result)
                elif operation == "xor":
                    return left_result.xor_vector(right_result)
        except Exception as err:
            logger.debug(
                "Falling back to byte-wise combination for %s due to %s",
                operation,
                err,
            )

        # Normalize to raw byte buffers (int-like 0/1 per element)
        def to_bytes_like(res):
            # memoryview from cython int8_t[...] -> supports buffer protocol
            if isinstance(res, (bytes, bytearray)):
                return bytearray(res)
            if isinstance(res, memoryview):
                return bytearray(res.tobytes())
            # Cython memoryviews often appear as objects supporting buffer()
            try:
                mv = memoryview(res)
                return bytearray(mv.tobytes())
            except (TypeError, ValueError, BufferError) as err:
                logger.debug("memoryview conversion failed for %s: %s", type(res).__name__, err)
            # Try sequence of ints/bools
            try:
                return bytearray((1 if bool(x) else 0) for x in res)
            except Exception:
                raise TypeError("Unsupported boolean result type for combination")

        lb = to_bytes_like(left_result)
        rb = to_bytes_like(right_result)
        if len(lb) != len(rb):
            raise ValueError("Boolean operands must have same length")
        # If compiled maskops are available and inputs are per-element bytes,
        # use the compiled helper which is faster than Python looping.
        n = len(lb)
        if and_mask is not None and or_mask is not None and xor_mask is not None:
            if operation == "and":
                return BoolMask(and_mask(bytes(lb), bytes(rb), n))
            elif operation == "or":
                return BoolMask(or_mask(bytes(lb), bytes(rb), n))
            elif operation == "xor":
                return BoolMask(xor_mask(bytes(lb), bytes(rb), n))

        # Fallback to Python loop
        out = bytearray(n)
        if operation == "and":
            for i in range(n):
                out[i] = 1 if (lb[i] and rb[i]) else 0
        elif operation == "or":
            for i in range(n):
                out[i] = 1 if (lb[i] or rb[i]) else 0
        elif operation == "xor":
            for i in range(n):
                out[i] = 1 if ((lb[i] and not rb[i]) or (not lb[i] and rb[i])) else 0
        else:
            raise ValueError(f"Unknown boolean operation: {operation}")

        # Return a BoolMask wrapper for per-element results
        return BoolMask(bytes(out))

    def evaluator(morsel: Morsel) -> Vector:
        # Evaluate left and right sub-expressions
        left_result = evaluate(morsel, left)
        right_result = evaluate(morsel, right)

        return _combine_boolean_results(left_result, right_result)

    return evaluator


def _compile_expression(expr: Expression) -> CompiledEvaluator:
    """
    Compile an expression into an optimized evaluator.

    This function analyzes the expression pattern and generates the most
    efficient evaluation strategy.
    """

    # Handle literal expressions
    if isinstance(expr, LiteralExpression):

        def evaluator(morsel: Morsel) -> Any:
            return expr.value

        return CompiledEvaluator(expr, evaluator)

    # Handle column expressions
    if isinstance(expr, ColumnExpression):

        def evaluator(morsel: Morsel) -> Vector:
            col_bytes = expr.column_name.encode("utf-8")
            return morsel.column(col_bytes)

        return CompiledEvaluator(expr, evaluator)

    # Handle binary expressions
    if isinstance(expr, BinaryExpression):
        # Detect a simple DNF pattern: ORs of AND-clauses (or single atoms)
        def _is_atom(e):
            return (
                isinstance(e, BinaryExpression)
                and isinstance(e.left, (ColumnExpression, LiteralExpression, BinaryExpression))
                or isinstance(e.right, (ColumnExpression, LiteralExpression, BinaryExpression))
            )

        def _is_dnf(e):
            # Only check for top-level OR with AND children, or single atom
            if e.operation == "or":
                return all(isinstance(ch, BinaryExpression) for ch in [e.left, e.right])
            return True

        # If it looks like DNF, try to generate/load a compiled evaluator
        try:
            if _is_dnf(expr):
                key = hashlib.sha1(repr(expr).encode("utf-8")).hexdigest()[:12]
                if has_compiled_evaluator(key):
                    mod_name, mod = ensure_compiled_evaluator(key, expr)
                    return CompiledEvaluator(expr, mod.evaluate, optimized=True)
                else:
                    # Try to generate synchronously (blocking) — if generator returns module, use it
                    try:
                        mod_name, mod = ensure_compiled_evaluator(key, expr)
                        return CompiledEvaluator(expr, mod.evaluate, optimized=True)
                    except Exception as err:
                        logger.debug(
                            "Failed to synchronously compile evaluator %s: %s",
                            key,
                            err,
                        )
        except Exception as err:
            # Any error here shouldn't prevent fallback behavior
            logger.debug(
                "Falling back to generic evaluator for %r due to %s",
                expr,
                err,
            )
        # Try to compile as comparison
        comparison_ops = [
            "equals",
            "not_equals",
            "greater_than",
            "greater_than_or_equals",
            "less_than",
            "less_than_or_equals",
        ]
        if expr.operation in comparison_ops:
            compiled = _compile_binary_comparison(expr.operation, expr.left, expr.right)
            if compiled:
                return CompiledEvaluator(expr, compiled, optimized=True)

        # Try to compile as boolean operation
        boolean_ops = ["and", "or", "xor"]
        if expr.operation in boolean_ops:
            compiled = _compile_binary_boolean(expr.operation, expr.left, expr.right)
            if compiled:
                return CompiledEvaluator(expr, compiled, optimized=True)

        # Fall back to generic evaluation
        def evaluator(morsel: Morsel) -> Vector:
            left_result = evaluate(morsel, expr.left)
            right_result = evaluate(morsel, expr.right)

            # Determine if operands are scalars
            left_is_scalar = not isinstance(left_result, Vector)
            right_is_scalar = not isinstance(right_result, Vector)

            # For now, raise error on unsupported operations
            raise ValueError(f"Operation {expr.operation} not yet supported in generic evaluator")

        return CompiledEvaluator(expr, evaluator)

    # Handle unary expressions
    if isinstance(expr, UnaryExpression):

        def evaluator(morsel: Morsel) -> Vector:
            operand_result = evaluate(morsel, expr.operand)

            if expr.operation == "not":
                return operand_result.not_()
            elif expr.operation == "is_null":
                # Would need to implement is_null on vectors
                raise ValueError("is_null operation not yet implemented")
            else:
                raise ValueError(f"Unknown unary operation: {expr.operation}")

        return CompiledEvaluator(expr, evaluator)

    raise ValueError(f"Unknown expression type: {type(expr)}")


def evaluate(morsel: Morsel, expression: Expression) -> Vector:
    """
    Evaluate an expression tree over a morsel.

    This is the main entry point for expression evaluation. It uses compiled
    evaluators for common patterns to achieve high performance.

    Args:
        morsel: The morsel to evaluate the expression over
        expression: The expression tree to evaluate

    Returns:
        Vector: Result vector (typically a boolean vector for predicates)

    Examples:
        >>> import draken
        >>> import pyarrow as pa
        >>> from opteryx.draken.evaluators import (
        ...     evaluate,
        ...     BinaryExpression,
        ...     ColumnExpression,
        ...     LiteralExpression,
        ... )
        >>> # Create a morsel
        >>> table = pa.table({"x": [1, 2, 3, 4, 5], "y": [10, 20, 30, 40, 50]})
        >>> morsel = draken.Morsel.from_arrow(table)
        >>> # Create expression: x == 3
        >>> expr = BinaryExpression("equals", ColumnExpression("x"), LiteralExpression(3))
        >>> # Evaluate
        >>> result = evaluate(morsel, expr)
        >>> print(list(result))
        [False, False, True, False, False]

        >>> # Create compound expression: x == 3 AND y > 20
        >>> expr1 = BinaryExpression("equals", ColumnExpression("x"), LiteralExpression(3))
        >>> expr2 = BinaryExpression("greater_than", ColumnExpression("y"), LiteralExpression(20))
        >>> compound = BinaryExpression("and", expr1, expr2)
        >>> # Evaluate
        >>> result = evaluate(morsel, compound)
        >>> print(list(result))
        [False, False, True, False, False]
    """
    # Check cache first
    expr_hash = _get_expression_hash(expression)

    if expr_hash in _evaluator_cache:
        compiled = _evaluator_cache[expr_hash]
    else:
        # Compile the expression
        compiled = _compile_expression(expression)

        # Cache it for reuse
        _evaluator_cache[expr_hash] = compiled

    # Evaluate and return
    return compiled.evaluate(morsel)


def clear_cache():
    """Clear the compiled evaluator cache."""
    global _evaluator_cache
    _evaluator_cache.clear()
