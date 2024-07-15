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


from typing import Dict
from typing import List
from urllib.parse import quote as url_encode

from opteryx.managers.expression import Node
from opteryx.managers.expression import NodeType
from opteryx.managers.schemes import BasePartitionScheme

_OPS_XLAT: Dict[str, str] = {
    "Eq": "%3D",  # Encoded value for "="
    "Gt": "%3E",  # Encoded value for ">"
    "GtEq": "%3E%3D",  # Encoded value for ">="
    "Lt": "%3C",  # Encoded value for "<"
    "LtEq": "%3C%3D",  # Encoded value for "<="
}


def _handle_operand(operand: Node) -> str:
    if operand.node_type == NodeType.IDENTIFIER:
        return operand.source_column

    literal = operand.value
    if hasattr(literal, "item"):
        literal = literal.item()

    return url_encode(str(literal))


def _construct_filter_predicates(predicates: list) -> str:
    """
    Construct a string of filter predicates from a list of predicate objects.

    Parameters:
        predicates: list
            List of predicate objects containing left, right operands and an operator.

    Returns:
        str: A string of filter predicates.
    """
    filter_predicates = []

    for predicate in predicates:
        operator = _OPS_XLAT.get(predicate.value)
        if operator:
            left_operand = predicate.left
            right_operand = predicate.right

            # Assuming _handle_operand is a function that processes operands appropriately
            left_value = _handle_operand(left_operand)
            right_value = _handle_operand(right_operand)

            filter_predicates.append(f"{left_value}{operator}{right_value}")

    return ",".join(filter_predicates)


class TarchiaScheme(BasePartitionScheme):
    """
    Handle reading data using the Tarchia scheme.
    """

    def get_blobs_in_partition(
        self,
        *,
        prefix: str,
        predicates: List,
        **kwargs,
    ) -> List[str]:
        from opteryx.managers.catalog.tarchia_provider import TarchiaCatalogProvider

        provider = TarchiaCatalogProvider()

        filter_predicates = _construct_filter_predicates(predicates) if predicates else ""

        blobs = provider.get_blobs_in_table(prefix, filters=filter_predicates)

        return sorted(b["path"].split("://")[1] for b in blobs)
