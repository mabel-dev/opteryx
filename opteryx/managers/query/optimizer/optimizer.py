

"""
This is a naive initial impementation of a query optimizer.

It is simply a set of rules which are executed in turn.

Query optimizers are the magic in a query engine, this is not magic, but all complex
things emerged from simple things, we we've set a low bar to get started on
implementing optimization.
"""


RULESET = []

# split commutive expressions into multiple where filters (ANDs) - gives more opportunity to push down
# move selection nodes
# choose join based on the fields in the column (I don't know if there's a performance choice)


def run_optimizer(plan):
    
    for rule in RULESET:
        plan = rule(plan)

    return plan