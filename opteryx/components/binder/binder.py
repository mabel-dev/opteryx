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


## bind variables
## bind temporal ranges
## (bind statistics)

import datetime
import decimal

from typing import Iterable

import numpy

from opteryx.exceptions import ProgrammingError, SqlError
from opteryx.models import QueryProperties


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
    if isinstance(value, (datetime.date, datetime.datetime)):
        return {"Value": {"SingleQuotedString": value.isoformat()}}


def variable_binder(node, parameter_set, properties, query_type):
    """Walk the AST replacing 'Placeholder' nodes, this is recursive"""
    # Replace placeholders with parameters.
    # We do this after the AST has been parsed to remove any chance of the
    # parameter affecting the meaning of any of the other tokens - i.e. to
    # eliminate this feature being used for SQL injection.
    if isinstance(node, list):
        return [variable_binder(i, parameter_set, properties, query_type) for i in node]
    if isinstance(node, dict):
        if "Value" in node:
            if "Placeholder" in node["Value"]:
                # fmt:off
                if len(parameter_set) == 0:
                    raise ProgrammingError("Incorrect number of bindings supplied."
                    " More placeholders are provided than parameters.")
                placeholder_value = parameter_set.pop(0)
                return _build_literal_node(placeholder_value)
                # fmt:on
        # replace @variables
        if query_type != "SetVariable" and "Identifier" in node:
            token_name = node["Identifier"]["value"]
            if token_name[0] == "@":
                if token_name not in properties.variables:  # pragma: no cover
                    raise SqlError(f"Undefined variable found in query `{token_name}`.")
                variable_value = properties.variables.get(token_name)
                return _build_literal_node(variable_value.value)
        return {
            k: variable_binder(v, parameter_set, properties, query_type)
            for k, v in node.items()
        }
    # we're a leaf
    return node


def temporal_range_binder(ast, filters, cache):
    if isinstance(ast, (list)):
        return [temporal_range_binder(node, filters, cache) for node in ast]
    if isinstance(ast, (dict)):
        node_name = next(iter(ast))
        if node_name == "Table":
            temporal_range = filters.pop(0)
            ast["Table"]["start_date"] = temporal_range[1]
            ast["Table"]["end_date"] = temporal_range[2]
            ast["Table"]["cache"] = cache
            return ast
        if "table_name" in ast:
            temporal_range = filters.pop(0)
            ast["table_name"][0]["start_date"] = temporal_range[1]
            ast["table_name"][0]["end_date"] = temporal_range[2]
            ast["table_name"][0]["cache"] = cache
            return ast
        if "ShowCreate" in ast:
            temporal_range = filters.pop(0)
            ast["ShowCreate"]["start_date"] = temporal_range[1]
            ast["ShowCreate"]["end_date"] = temporal_range[2]
            ast["ShowCreate"]["cache"] = cache
            return ast
        return {k: temporal_range_binder(v, filters, cache) for k, v in ast.items()}
    return ast


def statistics_binder(ast):
    return ast


def bind_ast(ast, parameters: Iterable = None, properties: QueryProperties = None):
    """
    Bind physical information to the AST

    This includes the following activities
    - Replacing placeholders with the parameters
    - Adding temporal range information to relations
    """
    query_type = next(iter(ast))

    bound_ast = ast.copy()
    bound_ast = variable_binder(bound_ast, parameters, properties, query_type)
    if query_type in ("Analyze", "Query", "Explain", "ShowColumns", "ShowCreate"):
        bound_ast = temporal_range_binder(
            bound_ast, list(properties.temporal_filters), properties.cache
        )
        bound_ast = statistics_binder(bound_ast)

    return bound_ast
