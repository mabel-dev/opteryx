# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
This is the AST rewriter, it sits between the Parser and the Logical Planner.

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
         │SQL                                  │Results
   ┌─────▼─────┐                         ┌─────┴─────┐
   │           │                         │           │
   │ Parser    │                         │ Executor  │
   └─────┬─────┘                         └─────▲─────┘
         │AST                                  │Plan
   ┌─────▼─────┐                         ╔═══════════╗
   │ AST       │                         ║Cost-Based ║
   │ Rewriter  │                         ║ Optimizer ║
   └─────┬─────┘                         ╚═════▲═════╝
         │AST                                  │Plan
   ┌─────▼─────┐      ┌───────────┐      ┌─────┴─────┐
   │ Logical   │ Plan │ Heuristic │ Plan │           │
   │   Planner ├──────► Optimizer ├──────► Binder    │
   └───────────┘      └───────────┘      └─────▲─────┘
                                               │Schemas
                                         ┌─────┴─────┐
                                         │           │
                                         │ Catalogue │
                                         └───────────┘
~~~

The primary role is to bind information to the AST which is provided in the order they appear
in the AST. For example, parameter substitutions and temporal ranges. Both of these require the
AST to be in the same order as the SQL provided by the user, which we can't guarantee after the
logical planner.

The parameter substitution is done after the AST is parsed to limit the possibility of the values
in the parameters affecting how the SQL is parsed (i.e. to prevent injection attacks). For similar
reasons, we do variable subsitutions here as well. Although these are not sensitive to ordering, 
we should remove any opportunity for them to be used for injection attacks so we bind them after
the building of the AST.

The temporal range binding is done here because it is a non-standard extention not supported by
the AST parser, so we strip the temporal ranges out in the SQL rewriter, and add them to the AST
here.
"""
import datetime
import decimal
from typing import Any
from typing import Dict
from typing import List
from typing import Union

import numpy

from opteryx.exceptions import ParameterError

LiteralNode = Dict[str, Any]


def _build_literal_node(value: Any) -> LiteralNode:
    """
    Construct the AST node for a given literal value.

    Parameters:
        value: The literal value to be converted into an AST node.

    Returns:
        A dictionary representing the AST node for the given literal.
    """
    if value is None:
        return {"Value": "Null"}
    elif isinstance(value, bool):
        return {"Value": {"Boolean": value}}
    elif isinstance(value, str):
        return {"Value": {"SingleQuotedString": value}}
    elif isinstance(value, (int, float, decimal.Decimal, numpy.int64)):
        return {"Value": {"Number": [value, False]}}
    elif isinstance(value, (datetime.date, datetime.datetime, datetime.time)):
        return {"Value": {"SingleQuotedString": value.isoformat()}}
    else:
        raise ValueError(f"Unsupported literal type: {type(value)}")


def parameter_list_binder(
    node: Union[Dict, List], parameter_set: List[Any], connection, query_type
) -> Union[Dict, List]:
    """
    Recursively walk the AST replacing 'Placeholder' nodes with parameters.

    Parameters:
        node: The AST node or list of nodes.
        parameter_set: The list of parameters to bind.
        connection: The database connection.
        query_type: The type of the query.

    Returns:
        The AST with parameters bound.

    Raises:
        ParameterError: If the number of placeholders and parameters do not match.
    """
    if isinstance(node, list):
        return [
            parameter_list_binder(child, parameter_set, connection, query_type) for child in node
        ]

    if isinstance(node, dict):
        if "Value" in node and "Placeholder" in node["Value"]:
            if node["Value"]["Placeholder"] != "?":
                raise ParameterError("Parameter lists are only used with qmark (?) parameters.")
            if not parameter_set:
                raise ParameterError(
                    "Incorrect number of bindings supplied. More placeholders than parameters."
                )
            placeholder_value = parameter_set.pop(0)
            if hasattr(placeholder_value, "value"):
                placeholder_value = placeholder_value.value
            return _build_literal_node(placeholder_value)
        return {
            k: parameter_list_binder(v, parameter_set, connection, query_type)
            for k, v in node.items()
        }

    return node  # Leaf node


def parameter_dict_binder(
    node: Union[Dict, List], parameter_set: Dict[str, Any], connection, query_type
) -> Union[Dict, List]:

    if isinstance(node, list):
        return [
            parameter_dict_binder(child, parameter_set, connection, query_type) for child in node
        ]

    if isinstance(node, dict):
        if "Placeholder" in node:
            placeholder_name = node["Placeholder"]
            if hasattr(placeholder_name, "value"):
                placeholder_name = placeholder_name.value
            placeholder_name = placeholder_name[1:]
            if placeholder_name not in parameter_set:
                raise ParameterError(f"Parameter not defined - {placeholder_name}")
            placeholder_value = parameter_set[placeholder_name]
            return _build_literal_node(placeholder_value)
        return {
            k: parameter_dict_binder(v, parameter_set, connection, query_type)
            for k, v in node.items()
        }
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


def do_ast_rewriter(ast: list, temporal_filters: list, parameters: Union[list, dict], connection):
    # get the query type
    query_type = next(iter(ast))
    # bind the temporal ranges, we do that here because the order in the AST matters
    with_temporal_ranges = temporal_range_binder(ast, temporal_filters)
    # bind the user provided parameters, we this that here because we want it after the
    # AST has been created (to avoid injection flaws) but also because the order
    # matters
    if isinstance(parameters, list) and len(parameters) > 0:
        with_parameters_exchanged = parameter_list_binder(
            with_temporal_ranges,
            parameter_set=parameters,
            connection=connection,
            query_type=query_type,
        )
        if len(parameters) != 0:
            raise ParameterError(
                "More parameters were provided than placeholders found in the query."
            )
    elif isinstance(parameters, dict):
        with_parameters_exchanged = parameter_dict_binder(
            with_temporal_ranges,
            parameter_set=parameters,
            connection=connection,
            query_type=query_type,
        )
    else:
        with_parameters_exchanged = with_temporal_ranges

    # Do some AST rewriting
    # first eliminate WHERE x IN (subquery) queries
    rewritten_query = with_parameters_exchanged

    return rewritten_query
