"""
This module provides an interface to the sqloxide library, which is responsible for parsing SQL,
restoring the Abstract Syntax Tree (AST), and performing various mutations on expressions and relations.

For more information about sqloxide: https://github.com/wseaton/sqloxide

This module is not from sqloxide, it is written for Opteryx.
"""

from .sqloxide import mutate_expressions
from .sqloxide import mutate_relations
from .sqloxide import parse_sql
from .sqloxide import restore_ast

# Explicitly define the API of this module for external consumers
__all__ = ["parse_sql", "restore_ast", "mutate_expressions", "mutate_relations"]
