"""
Distinct Node

This is a SQL Query Execution Plan Node.

This Node eliminates duplicate records.
"""
from opteryx.engine.relation import Relation
from opteryx.engine.planner.operations.base_plan_node import BasePlanNode
import numpy as np

def groupify_array(arr):
    # Input: Pyarrow/Numpy array
    # Output:
    #   - 1. Unique values
    #   - 2. Sort index
    #   - 3. Count per unique
    #   - 4. Begin index per unique
    dic, counts = np.unique(arr, return_counts=True)
    sort_idx = np.argsort(arr)
    return dic, counts, sort_idx, [0] + np.cumsum(counts)[:-1].tolist()

def combine_column(table, name):
    return table.column(name).combine_chunks()

f = np.vectorize(hash)
def columns_to_array(table, columns):
    columns = ([columns] if isinstance(columns, str) else list(set(columns)))
    if len(columns) == 1:
        #return combine_column(table, columns[0]).to_numpy(zero_copy_only=False)
        return f(combine_column(table, columns[0]).to_numpy(zero_copy_only=False))
    else:
        values = [c.to_numpy() for c in table.select(columns).itercolumns()]
        return np.array(list(map(hash, zip(*values))))

# Drop duplicates
def drop_duplicates(table, on=[], keep='first'):
    # Gather columns to arr
    arr = columns_to_array(table, (on if on else table.column_names))

    # Groupify
    dic, counts, sort_idxs, bgn_idxs = groupify_array(arr)

    # Gather idxs
    if keep == 'last':
        idxs = (np.array(bgn_idxs) - 1)[1:].tolist() + [len(sort_idxs) - 1]
    elif keep == 'first':
        idxs = bgn_idxs
    elif keep == 'drop':
        idxs = [i for i, c in zip(bgn_idxs, counts) if c == 1]
    return table.take(sort_idxs[idxs])

class DistinctNode(BasePlanNode):
    def __init__(self, **config):
        self._distinct = config.get("distinct", True)

    def execute(self, relation: Relation) -> Relation:
        if self._distinct:
            return drop_duplicates(relation)
        return relation
