

"""
Optimization Rule - Selection Pushdown PreJoin

The naive order of a query execution puts the WHERE execution after the JOIN execution.
This means that most of the time when these two exist in the same query, the JOIN is
doing work that is thrown-away almost immediately. This optimization step attempts
to reduce this by trying to identify when it can move the filter to before the join.
"""

from opteryx import operators
from opteryx.managers.query.optimizer import plan_has_operator

def run(plan):

    # find the in-scope nodes
    selection_nodes = plan_has_operator(plan, operators.SelectionNode)
    join_nodes = plan_has_operator(plan, (operators.InnerJoinNode, operators.OuterJoinNode)

    # killer questions
    if selection_nodes is None:
        return plan
    if join_nodes is None:
        return plan

    # just because we're here - doesn't mean we can optimize

#    walk the DAG

    return plan