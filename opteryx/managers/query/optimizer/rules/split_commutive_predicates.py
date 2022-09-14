

"""
Optimization Rule - Split Commutive Predicates (ANDs)

Commutive Predicates are those that evaluat

The reason for splitting is two-fold:

1) Smaller expressions are easier to move around the query plan as they have fewer
   dependencies.
2) Executing predicates like this means susequent predicates will be operating on
   fewer records, which is generally faster
"""

from opteryx import operators
from opteryx.managers.query.optimizer import plan_has_operator

def run(plan):

    # find the in-scope nodes
    selection_nodes = get_matching_plan_operators(plan, operators.SelectionNode)

    # killer questions
    if selection_nodes is None:
        return plan

    # HAVING and WHERE are selection nodes
    for selection_node in selection_nodes:
        pass
    # get the expression out of the selection nodes
    # if the root is an AND split into two and repeat (i.e. if they have an AND root)
    # remove the node from the naive plan and insert the new nodes

    return plan