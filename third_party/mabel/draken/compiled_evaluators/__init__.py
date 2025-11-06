"""Runtime for dynamically compiled evaluators.

This package holds generated Cython modules compiled at runtime into
`draken/compiled_evaluators` and imported dynamically.
"""

from .generator import ensure_compiled_evaluator
from .generator import has_compiled_evaluator

__all__ = ["ensure_compiled_evaluator", "has_compiled_evaluator"]
