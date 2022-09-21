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

from opteryx.exceptions import SqlError
from opteryx.managers.planner.logical import literals
from opteryx.models import ExecutionTree
from opteryx import operators


def explain_query():
    pass


def select_query():
    pass


def set_variable_query(ast, properties):
    """put variables defined in SET statements into context"""
    key = ast["SetVariable"]["variable"][0]["value"]
    value = literals.build(ast["SetVariable"]["value"][0]["Value"])
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


def show_columns_query():
    pass


def show_create_query():
    pass


def show_variable_query():
    pass


def show_variables_query():
    pass


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
