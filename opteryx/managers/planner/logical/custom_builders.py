"""
Builders which require special handling
"""

from opteryx.managers.expression import ExpressionTreeNode
from opteryx.managers.expression import NodeType


def extract_show_filter(ast):
    """filters are used in SHOW queries"""
    filters = ast["filter"]
    if filters is None:
        return None
    #    if "Where" in filters:
    #        return self._filter_extract(filters["Where"])
    if "Like" in filters:
        left = ExpressionTreeNode(NodeType.IDENTIFIER, value="name")
        right = ExpressionTreeNode(NodeType.LITERAL_VARCHAR, value=filters["Like"])
        root = ExpressionTreeNode(
            NodeType.COMPARISON_OPERATOR,
            value="Like",
            left_node=left,
            right_node=right,
        )
        return root
