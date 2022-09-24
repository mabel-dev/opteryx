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

from opteryx import operators
from opteryx.connectors import connector_factory
from opteryx.exceptions import SqlError
from opteryx.managers.expression import ExpressionTreeNode
from opteryx.managers.expression import NodeType
from opteryx.managers.planner.logical import builders, custom_builders
from opteryx.models import ExecutionTree


def explain_query(ast, properties):
    pass


def select_query(ast, properties):
    pass


def set_variable_query(ast, properties):
    """put variables defined in SET statements into context"""
    key = ast["SetVariable"]["variable"][0]["value"]
    value = builders.build(ast["SetVariable"]["value"][0]["Value"])
    if key[0] == "@":  # pragma: no cover
        properties.variables[key] = value
    else:
        key = key.lower()
        if key in ("variables",):
            raise SqlError(f"Invalid parameter '{key}'")
        if hasattr(properties, key):
            setattr(properties, key, value.value)
        else:
            raise SqlError(
                f"Unknown parameter, variables must be prefixed with a '@' - '{key}'"
            )

    # return a plan, because it's expected
    plan = ExecutionTree()
    operator = operators.ShowValueNode(
        key="result", value="Complete", properties=properties
    )
    plan.add_operator("show", operator=operator)
    return plan


def show_columns_query(ast, properties):

    plan = ExecutionTree()
    dataset = ".".join([part["value"] for part in ast["ShowColumns"]["table_name"]])

    if dataset[0:1] == "$":
        mode = "Internal"
        reader = None
    else:
        reader = connector_factory(dataset)
        mode = reader.__mode__

    plan.add_operator(
        "reader",
        operators.reader_factory(mode)(
            properties=properties,
            dataset=dataset,
            alias=None,
            reader=reader,
            cache=None,  # never read from cache
            start_date=properties.start_date,
            end_date=properties.end_date,
        ),
    )
    last_node = "reader"

    filters = custom_builders.extract_show_filter(ast["ShowColumns"])
    if filters:
        plan.add_operator(
            "filter",
            operators.ColumnFilterNode(properties=properties, filter=filters),
        )
        plan.link_operators(last_node, "filter")
        last_node = "filter"

    plan.add_operator(
        "columns",
        operators.ShowColumnsNode(
            properties=properties,
            full=ast["ShowColumns"]["full"],
            extended=ast["ShowColumns"]["extended"],
        ),
    )
    plan.link_operators(last_node, "columns")
    last_node = "columns"

    return plan


def show_create_query(ast, properties):
    pass


def show_variable_query(ast, properties):
    """
    This is the generic SHOW <variable> handler - there are specific handlers
    for some keywords after SHOW, like SHOW COLUMNS.

    SHOW <variable> only really has a single node.

    All of the keywords should up as a 'values' list in the variable in the ast.
    """

    plan = ExecutionTree()

    keywords = [value["value"].upper() for value in ast["ShowVariable"]["variable"]]
    if keywords[0] == "FUNCTIONS":
        show_node = "show_functions"
        node = operators.ShowFunctionsNode(properties=properties)
        plan.add_operator(show_node, operator=node)
    elif keywords[0] == "PARAMETER":
        if len(keywords) != 2:
            raise SqlError("`SHOW PARAMETER` expects a single parameter name.")
        key = keywords[1].lower()
        if not hasattr(properties, key) or key == "variables":
            raise SqlError(f"Unknown parameter '{key}'.")
        value = getattr(properties, key)

        show_node = "show_parameter"
        node = operators.ShowValueNode(properties=properties, key=key, value=value)
        plan.add_operator(show_node, operator=node)
    else:  # pragma: no cover
        raise SqlError(f"SHOW statement type not supported for `{keywords[0]}`.")

    name_column = ExpressionTreeNode(NodeType.IDENTIFIER, value="name")

    order_by_node = operators.SortNode(
        properties=properties,
        order=[([name_column], "ascending")],
    )
    plan.add_operator("order", operator=order_by_node)
    plan.link_operators(show_node, "order")

    return plan


def show_variables_query(ast, properties):
    """show the known variables, optionally filter them"""
    plan = ExecutionTree()

    show = operators.ShowVariablesNode(properties=properties)
    plan.add_operator("show", show)
    last_node = "show"

    filters = custom_builders.extract_show_filter(ast["ShowVariables"])
    if filters:
        plan.add_operator(
            "filter",
            operators.SelectionNode(properties=properties, filter=filters),
        )
        plan.link_operators(last_node, "filter")

    return plan


# wrappers for the query builders
QUERY_BUILDER = {
    "Explain": explain_query,
    "Query": select_query,
    "SetVariable": set_variable_query,
    "ShowColumns": show_columns_query,
    "ShowCreate": show_create_query,
    "ShowVariable": show_variable_query,  # generic SHOW handler
    "ShowVariables": show_variables_query,
}
