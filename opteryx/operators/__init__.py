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

from .base_plan_node import BasePlanDataObject  # isort: skip
from .base_plan_node import BasePlanNode  # isort: skip
from .base_plan_node import OperatorType  # isort: skip

from .aggregate_and_group_node import AggregateAndGroupNode  # Group is always followed by aggregate
from .aggregate_node import AGGREGATORS
from .aggregate_node import AggregateNode  # aggregate data
from .async_read_node import AsyncReaderNode

# from .build_statistics_node import BuildStatisticsNode  # Analyze Tables
from .cross_join_node import CrossJoinNode  # CROSS JOIN
from .distinct_node import DistinctNode  # remove duplicate records
from .exit_node import ExitNode
from .explain_node import ExplainNode  # EXPLAIN queries
from .filter_node import FilterNode  # filter unwanted rows
from .function_dataset_node import FunctionDatasetNode  # Dataset Constructors
from .heap_sort_node import HeapSortNode  # Heap

# from .information_schema_node import InformationSchemaNode  # information_schema
from .inner_join_node import InnerJoinNode
from .join_node import JoinNode
from .left_join_node import LeftJoinNode
from .limit_node import LimitNode  # select the first N records

# from .metadata_writer_node import MetadataWriterNode
# from .morsel_defragment_node import MorselDefragmentNode  # consolidate small morsels
from .noop_node import NoOpNode  # No Operation
from .projection_node import ProjectionNode  # remove unwanted columns including renames
from .read_node import ReaderNode
from .set_variable_node import SetVariableNode
from .show_columns_node import ShowColumnsNode  # column details

# from .show_create_node import ShowCreateNode  # SHOW CREATE TABLE
# from .show_databases_node import ShowDatabasesNode  # SHOW DATABASES
# from .show_functions_node import ShowFunctionsNode  # supported functions
from .show_value_node import ShowValueNode  # display node for SHOW
from .sort_node import SortNode  # order by selected columns
from .union_node import UnionNode


def is_aggregator(name):
    return name in AGGREGATORS


def aggregators():
    return list(AGGREGATORS.keys())
