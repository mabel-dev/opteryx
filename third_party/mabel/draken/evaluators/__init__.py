"""
Compiled expression evaluators for Draken.

This module provides high-performance compiled evaluators that can evaluate
expression trees over morsels in a single optimized pass, rather than
evaluating each operation in isolation.

Main exports:
- Expression: Base class for expression nodes
- evaluate: Function to evaluate an expression tree over a morsel
"""

from draken.evaluators.evaluator import evaluate
from draken.evaluators.expression import BinaryExpression
from draken.evaluators.expression import ColumnExpression
from draken.evaluators.expression import Expression
from draken.evaluators.expression import LiteralExpression

__all__ = [
    "Expression",
    "BinaryExpression",
    "ColumnExpression",
    "LiteralExpression",
    "evaluate",
]
