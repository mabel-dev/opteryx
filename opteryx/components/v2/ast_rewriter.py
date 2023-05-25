"""
~~~
                      ┌───────────┐
                      │   USER    │
         ┌────────────┤           ◄────────────┐
         │SQL         └───────────┘            │
  ───────┼─────────────────────────────────────┼──────
         │                                     │
   ┌─────▼─────┐                               │
   │ SQL       │                               │
   │  Rewriter │                               │
   └─────┬─────┘                               │
         │SQL                                  │Plan
   ┌─────▼─────┐                         ┌─────┴─────┐
   │           │                         │           │
   │ Parser    │                         │ Executor  │
   └─────┬─────┘                         └─────▲─────┘
         │AST                                  │Plan
   ╔═════▼═════╗      ┌───────────┐      ┌─────┴─────┐
   ║ AST       ║      │           │Stats │Cost-Based │
   ║ Rewriter  ║      │ Catalogue ├──────► Optimizer │
   ╚═════╦═════╝      └─────┬─────┘      └─────▲─────┘
         │AST               │Schemas           │Plan
   ┌─────▼─────┐      ┌─────▼─────┐      ┌─────┴─────┐
   │ Logical   │ Plan │           │ Plan │ Heuristic │
   │   Planner ├──────► Binder    ├──────► Optimizer │
   └───────────┘      └───────────┘      └───────────┘
~~~
"""
import datetime
import decimal

import numpy

from opteryx.exceptions import ProgrammingError
from opteryx.exceptions import SqlError


def _build_literal_node(value):
    if value is None:
        return {"Value": "Null"}
    if isinstance(value, (bool)):
        # boolean must be before numeric
        return {"Value": {"Boolean": value}}
    if isinstance(value, (str)):
        return {"Value": {"SingleQuotedString": value}}
    if isinstance(value, (int, float, decimal.Decimal)):
        return {"Value": {"Number": [value, False]}}
    if isinstance(value, (numpy.datetime64)):
        return {"Value": {"SingleQuotedString": value.item().isoformat()}}
    if isinstance(value, (datetime.date, datetime.datetime, datetime.time)):
        return {"Value": {"SingleQuotedString": value.isoformat()}}


def variable_binder(node, parameter_set, connection, query_type):
    """Walk the AST replacing 'Placeholder' nodes, this is recursive"""
    # Replace placeholders with parameters.
    # We do this after the AST has been parsed to remove any chance of the parameter affecting the
    # meaning of any of the other tokens - i.e. to eliminate this feature being used for SQL
    # injection.
    if isinstance(node, list):
        return [variable_binder(i, parameter_set, connection, query_type) for i in node]
    if isinstance(node, dict):
        if "Value" in node:
            if "Placeholder" in node["Value"]:
                # fmt:off
                if len(parameter_set) == 0:
                    raise ProgrammingError("Incorrect number of bindings supplied."
                    " More placeholders are provided than parameters.")
                placeholder_value = parameter_set.pop(0)
                # prepared statements will have parsed this already
                if hasattr(placeholder_value, "value"):
                    placeholder_value = placeholder_value.value
                return _build_literal_node(placeholder_value)
                # fmt:on
        # replace @variables
        #        if query_type != "SetVariable" and "Identifier" in node:
        #            token_name = node["Identifier"]["value"]
        #            if token_name.startswith("@@"):
        #                return _build_literal_node(SystemVariables[token_name[2:]])
        #            elif token_name[0] == "@":
        #                if token_name not in properties.variables:  # pragma: no cover
        #                    raise SqlError(f"Undefined variable found in query `{token_name}`.")
        #                variable_value = properties.variables[token_name]
        #                return _build_literal_node(variable_value.value)
        return {
            k: variable_binder(v, parameter_set, connection, query_type) for k, v in node.items()
        }
    # we're a leaf
    return node


def temporal_range_binder(ast, filters):
    if isinstance(ast, (list)):
        return [temporal_range_binder(node, filters) for node in ast]
    if isinstance(ast, (dict)):
        node_name = next(iter(ast))
        if node_name == "Table":
            temporal_range = filters.pop(0)
            ast["Table"]["start_date"] = temporal_range[1]
            ast["Table"]["end_date"] = temporal_range[2]
            return ast
        if "table_name" in ast:
            temporal_range = filters.pop(0)
            ast["table_name"][0]["start_date"] = temporal_range[1]
            ast["table_name"][0]["end_date"] = temporal_range[2]
            return ast
        if "ShowCreate" in ast:
            temporal_range = filters.pop(0)
            ast["ShowCreate"]["start_date"] = temporal_range[1]
            ast["ShowCreate"]["end_date"] = temporal_range[2]
            return ast
        return {k: temporal_range_binder(v, filters) for k, v in ast.items()}
    return ast


def do_ast_rewriter(ast: list, temporal_filters: list, paramters: list, connection):
    query_type = next(iter(ast))

    with_temporal_ranges = temporal_range_binder(ast, temporal_filters)
    with_parameters_exchanged = variable_binder(
        with_temporal_ranges, parameter_set=paramters, connection=connection, query_type=query_type
    )

    return with_parameters_exchanged
