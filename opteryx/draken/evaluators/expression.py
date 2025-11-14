"""
Expression tree data structures for compiled evaluators.

This module defines the expression tree nodes that represent operations
to be evaluated over morsels. The expression tree is used by the
compiled evaluator to generate optimized evaluation code.
"""

from typing import Any

from opteryx.third_party.cyan4973.xxhash import hash_bytes


class Expression:
    """
    Base class for expression nodes in an expression tree.

    An expression represents a computation that can be evaluated
    over a morsel to produce a result vector or scalar.
    """

    def __repr__(self):
        return f"{self.__class__.__name__}()"

    def __eq__(self, other):
        return isinstance(other, self.__class__)

    def __hash__(self) -> int:
        return hash_bytes(self.__class__.__name__.encode("utf-8"))


class LiteralExpression(Expression):
    """
    Represents a literal constant value in an expression tree.

    Examples:
        LiteralExpression(1)
        LiteralExpression('england')
        LiteralExpression(True)
    """

    def __init__(self, value: Any):
        """
        Create a literal expression.

        Args:
            value: The constant value
        """
        self.value = value

    def __repr__(self):
        return f"LiteralExpression({self.value!r})"

    def __eq__(self, other):
        return isinstance(other, LiteralExpression) and self.value == other.value

    def __hash__(self) -> int:
        return hash_bytes(f"{self.__class__.__name__}::{self.value}".encode("utf-8"))


class ColumnExpression(Expression):
    """
    Represents a reference to a column in a morsel.

    Examples:
        ColumnExpression('x')
        ColumnExpression('country')
    """

    def __init__(self, column_name: str):
        """
        Create a column reference expression.

        Args:
            column_name: Name of the column to reference
        """
        self.column_name = column_name

    def __repr__(self):
        return f"ColumnExpression({self.column_name!r})"

    def __eq__(self, other):
        return isinstance(other, ColumnExpression) and self.column_name == other.column_name

    def __hash__(self) -> int:
        return hash_bytes(f"{self.__class__.__name__}::{self.column_name}".encode("utf-8"))


class BinaryExpression(Expression):
    """
    Represents a binary operation between two expressions.

    Examples:
        BinaryExpression('equals', ColumnExpression('x'), LiteralExpression(1))
        BinaryExpression('and', expr1, expr2)
    """

    def __init__(self, operation: str, left: Expression, right: Expression):
        """
        Create a binary operation expression.

        Args:
            operation: Name of the operation (e.g., 'equals', 'and', 'greater_than')
            left: Left operand expression
            right: Right operand expression
        """
        self.operation = operation
        self.left = left
        self.right = right

    def __repr__(self):
        return f"BinaryExpression({self.operation!r}, {self.left!r}, {self.right!r})"

    def __eq__(self, other):
        return (
            isinstance(other, BinaryExpression)
            and self.operation == other.operation
            and self.left == other.left
            and self.right == other.right
        )

    def __hash__(self) -> int:
        return hash_bytes(
            f"{self.__class__.__name__}::{self.operation}::{self.left}::{self.right}".encode(
                "utf-8"
            )
        )


class UnaryExpression(Expression):
    """
    Represents a unary operation on an expression.

    Examples:
        UnaryExpression('not', BoolExpression(...))
    """

    def __init__(self, operation: str, operand: Expression):
        """
        Create a unary operation expression.

        Args:
            operation: Name of the operation (e.g., 'not', 'is_null')
            operand: The expression to operate on
        """
        self.operation = operation
        self.operand = operand

    def __repr__(self):
        return f"UnaryExpression({self.operation!r}, {self.operand!r})"

    def __eq__(self, other):
        return (
            isinstance(other, UnaryExpression)
            and self.operation == other.operation
            and self.operand == other.operand
        )

    def __hash__(self) -> int:
        return hash_bytes(
            f"{self.__class__.__name__}::{self.operation}::{self.operand}".encode("utf-8")
        )
