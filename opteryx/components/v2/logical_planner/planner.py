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
Represents a logical plan

A small set of functions are available in the logical plan (a set similar to, but
different from Cobb's relational algebra)

Steps are given random IDs to prevent collisions
"""

from enum import auto, Enum

from opteryx.components.logical_planner import builders
from opteryx.managers.expression import ExpressionTreeNode, NodeType
from opteryx.third_party.travers import Graph
from opteryx.utils import unique_id


class LogicalPlanStepType(int, Enum):
    PROJECT = auto()  # field selection
    SELECT = auto()  # tuple filtering
    UNION = auto()  #  appending relations
    DIFFERENCE = auto()  # relation interection
    RENAME = auto()  # field renaming, including evaluation
    JOIN = auto()  # all joina
    GROUP = auto()  # group by, without the aggregation
    READ = auto()  # read a dataset
    SET = auto()  # set a variable
    LIMIT = auto()  # limit and offset
    ORDER = auto()  # order by


class LogicalPlan(Graph):
    def get_relations(self):
        relations = []
        for nid, node in self._nodes.items():
            if node["step"] == LogicalPlanStepType.READ:
                relations.append(nid)
        return relations


"""
CLAUSE PLANNERS
"""


def extract_value(clause):
    if len(clause) == 1:
        return builders.build(clause[0])
    return [builders.build(token) for token in clause]


def extract_variable(clause):
    if len(clause) == 1:
        return clause[0]["value"]
    return [token["value"] for token in clause]


"""
STATEMENT PLANNERS
"""


def plan_set_variable(statement):
    root_node = "SetVariable"
    plan = LogicalPlan()
    set_step = {
        "step": LogicalPlanStepType.SET,
        "variable": extract_variable(statement[root_node]["variable"]),
        "value": extract_value(statement[root_node]["value"]),
    }
    plan.add_node(unique_id(), set_step)
    return plan


def plan_show_variables(statement):
    root_node = "ShowVariables"
    plan = LogicalPlan()

    read_step = {
        "step": LogicalPlanStepType.READ,
        "source": "$variables",
    }
    step_id = unique_id()
    plan.add_node(step_id, read_step)

    predicate = statement[root_node]["filter"]
    if predicate is not None:
        operator = next(iter(predicate))
        select_step = {
            "step": LogicalPlanStepType.SELECT,
            "predicate": ExpressionTreeNode(
                token_type=NodeType.COMPARISON_OPERATOR,
                value=operator,
                left=ExpressionTreeNode(token_type=NodeType.IDENTIFIER, value="name"),
                right=predicate[operator],
            ),
        }
        print(select_step)
        previous_step_id, step_id = step_id, unique_id()
        plan.add_node(step_id, select_step)
        plan.add_edge(previous_step_id, step_id)

    return plan


QUERY_BUILDERS = {
    #    "Analyze": analyze_query,
    #    "Explain": explain_query,
    #    "Query": select_query,
    "SetVariable": plan_set_variable,
    #    "ShowColumns": show_columns_query,
    #    "ShowCreate": show_create_query,
    #    "ShowFunctions": show_functions_query,
    #    "ShowVariable": show_variable_query,  # generic SHOW handler
    "ShowVariables": plan_show_variables,
}


def get_planners(parsed_statements):
    # The sqlparser ast is an array of asts
    for parsed_statement in parsed_statements:
        statement_type = next(iter(parsed_statement))
        yield QUERY_BUILDERS[statement_type], parsed_statement
