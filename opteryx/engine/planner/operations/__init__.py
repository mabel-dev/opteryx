from .base_plan_node import BasePlanNode

from .distinct_node import DistinctNode  # remove duplicate records
from .limit_node import LimitNode  # select the first N records
from .partition_reader_node import PartitionReaderNode  # read partitions
from .projection_node import ProjectionNode  # remove unwanted columns including renames
from .selection_node import SelectionNode  # filter unwanted rows

# - JoinNode (currently only INNER JOIN)
# - SortNode (order a relation by given keys)
# - AggregationNode (put a relation into groups - GROUP BY - also does aggregations)
# - CombineNode (combine sketches and aggregations)
# - Merge - append sets to each other
# - Evaluate ()
# - Index (create an index, can be temporary or persisted) <- beyond the BRIN, do we need one?
