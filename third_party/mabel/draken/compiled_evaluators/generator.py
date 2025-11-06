"""Generator for per-expression compiled evaluators.

This module exposes `ensure_compiled_evaluator(key, expr_ast)` which will
generate a small Cython `.pyx` wrapper for the expression AST, compile it
in-place into `draken/compiled_evaluators/` and import the resulting module.

For safety and simplicity the generator only handles DNF-like expressions
consisting of ORs of AND-clauses where each atom is a column vs literal or
column vs column comparison using equals/greater/less/etc.
"""

import hashlib
import importlib
import os
from pathlib import Path
from typing import Tuple

from draken.evaluators.expression import BinaryExpression
from draken.evaluators.expression import ColumnExpression
from draken.evaluators.expression import LiteralExpression

PACKAGE_DIR = Path(__file__).parent
GENERATED_DIR = PACKAGE_DIR


def _normalize_key(expr) -> str:
    """Create a short canonical key for an expression AST. Uses hashing to keep
    names short."""
    s = repr(expr)
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:12]


def has_compiled_evaluator(key: str) -> bool:
    """Check whether a compiled evaluator module exists for key."""
    mod_name = f"draken.compiled_evaluators.generated_{key}"
    try:
        importlib.import_module(mod_name)
        return True
    except ImportError:
        return False


def ensure_compiled_evaluator(key: str, expr_ast) -> Tuple[str, object]:
    """Ensure a compiled evaluator exists for `key` and `expr_ast`.

    Returns (module_name, module_obj).
    """
    mod_name = f"draken.compiled_evaluators.generated_{key}"
    if has_compiled_evaluator(key):
        return mod_name, importlib.import_module(mod_name)

    # Emit a simple .pyx file for the expression.
    pyx_name = GENERATED_DIR / f"generated_{key}.pyx"
    tmpl = _emit_pyx_for_expr(expr_ast)
    pyx_name.write_text(tmpl)

    # Build in-place using setup.py (synchronous but simple)
    cwd = Path.cwd()
    try:
        os.chdir(cwd)
        # Run build
        import subprocess

        subprocess.check_call(["python", "setup.py", "build_ext", "--inplace"])
    finally:
        os.chdir(cwd)

    # Import generated module
    mod = importlib.import_module(mod_name)
    return mod_name, mod


_PYX_TEMPLATE = r"""
# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

# This file is auto-generated. Do not edit directly.

from libc.stdint cimport uint8_t, intptr_t
from libc.stdlib cimport malloc, free

from draken.compiled import and_mask, or_mask, xor_mask
from draken.morsels.morsel import Morsel

def _ptr_to_bytes(void *data, size_t nbytes):
    # Create a Python bytes object from a raw pointer
    return bytes(<char *> data, nbytes)

# Generated code below this point
"""


def _emit_pyx_for_expr(expr_ast) -> str:
    # Very small codegen for DNF: ORs of ANDs where each atom is
    # (col OP literal) or (col OP col) and OP is equals/not_equals/gt/lt.
    # We'll generate a function that:
    #  - obtains columns via morsel.column(b'name')
    #  - calls vector comparison methods which return BoolVector
    #  - extracts ptr.data and length and uses compiled maskops to combine
    # This is intentionally conservative and assumes BoolVector.ptr.data is
    # the bit-packed buffer address.

    # Simple recursive flattening for OR of AND
    def flatten_or(e):
        if isinstance(e, BinaryExpression) and e.operation == "or":
            return flatten_or(e.left) + flatten_or(e.right)
        return [e]

    def flatten_and(e):
        if isinstance(e, BinaryExpression) and e.operation == "and":
            return flatten_and(e.left) + flatten_and(e.right)
        return [e]

    clauses = []
    # top-level could be OR of ANDs or a single clause
    if isinstance(expr_ast, BinaryExpression) and expr_ast.operation == "or":
        parts = flatten_or(expr_ast)
        for p in parts:
            if isinstance(p, BinaryExpression) and p.operation == "and":
                clauses.append(flatten_and(p))
            else:
                clauses.append([p])
    else:
        if isinstance(expr_ast, BinaryExpression) and expr_ast.operation == "and":
            clauses.append(flatten_and(expr_ast))
        else:
            clauses.append([expr_ast])

    # Build imports and header
    header = [_PYX_TEMPLATE]
    body_lines = ["def evaluate(morsel: Morsel):"]
    body_lines.append("    # Acquire comparison masks for each atom")
    body_lines.append("    masks = []")

    for cidx, clause in enumerate(clauses):
        for aidx, atom in enumerate(clause):
            if not isinstance(atom, BinaryExpression):
                raise ValueError("Unsupported atom in DNF codegen")
            op = atom.operation
            left = atom.left
            right = atom.right
            if isinstance(left, ColumnExpression) and isinstance(right, LiteralExpression):
                col = left.column_name
                lit = right.value
                lit_py = repr(lit)
                body_lines.append(f"    # atom {cidx}.{aidx}: {col} {op} {lit_py}")
                body_lines.append(f"    vec_{cidx}_{aidx} = morsel.column(b'{col}')")
                # map operation to method name
                method = {
                    "equals": "equals",
                    "not_equals": "not_equals",
                    "greater_than": "greater_than",
                    "greater_than_or_equals": "greater_than_or_equals",
                    "less_than": "less_than",
                    "less_than_or_equals": "less_than_or_equals",
                }.get(op)
                if method is None:
                    raise ValueError("Unsupported comparison op in codegen")
                body_lines.append(f"    mask_{cidx}_{aidx} = vec_{cidx}_{aidx}.{method}({lit_py})")
                body_lines.append(f"    masks.append(mask_{cidx}_{aidx})")
            elif isinstance(left, ColumnExpression) and isinstance(right, ColumnExpression):
                lcol = left.column_name
                rcol = right.column_name
                body_lines.append(f"    # atom {cidx}.{aidx}: {lcol} {op} {rcol}")
                body_lines.append(f"    vec_{cidx}_{aidx}_l = morsel.column(b'{lcol}')")
                body_lines.append(f"    vec_{cidx}_{aidx}_r = morsel.column(b'{rcol}')")
                method = {
                    "equals": "equals_vector",
                    "not_equals": "not_equals_vector",
                    "greater_than": "greater_than_vector",
                    "greater_than_or_equals": "greater_than_or_equals_vector",
                    "less_than": "less_than_vector",
                    "less_than_or_equals": "less_than_or_equals_vector",
                }.get(op)
                if method is None:
                    raise ValueError("Unsupported comparison op in codegen")
                body_lines.append(
                    f"    mask_{cidx}_{aidx} = vec_{cidx}_{aidx}_l.{method}(vec_{cidx}_{aidx}_r)"
                )
                body_lines.append(f"    masks.append(mask_{cidx}_{aidx})")
            else:
                raise ValueError("Unsupported atom types in codegen")

    # Now combine clauses: for each clause compute AND of its masks, then OR across clauses
    body_lines.append("")
    body_lines.append("    # Combine clause masks (AND within clause)")
    body_lines.append("    clause_masks = []")
    for cidx, clause in enumerate(clauses):
        if len(clause) == 1:
            body_lines.append(
                f"    clause_masks.append(masks[{0}])".replace(
                    "{0}", str(sum(len(cl) for cl in clauses[:cidx]))
                )
            )
        else:
            # combine sequentially using and_mask/or_mask
            start = sum(len(cl) for cl in clauses[:cidx])
            cur = f"masks[{start}]"
            for k in range(1, len(clause)):
                nxt = f"masks[{start + k}]"
                body_lines.append(f"    # combine {cur} AND {nxt}")
                body_lines.append(f"    {cur} = BoolMask({cur}).and_vector({nxt})")
            body_lines.append(f"    clause_masks.append({cur})")

    body_lines.append("")
    body_lines.append("    # OR across clause masks")
    body_lines.append("    if not clause_masks:")
    body_lines.append("        return BoolMask(bytes())")
    body_lines.append("    res = clause_masks[0]")
    body_lines.append("    for cm in clause_masks[1:]:")
    body_lines.append("        res = res.or_vector(cm)")
    body_lines.append("    return res")

    return "\n".join(header) + "\n\n" + "\n".join(body_lines)
