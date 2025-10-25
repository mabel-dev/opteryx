"""
Remote Database Pushdown Strategy

This strategy identifies SQL-backed data sources and pushes operations to the remote
database server by:
1. Finding SQL reader nodes
2. Walking up the tree until hitting a non-pushable operation
3. Detaching that subtree and attaching it to the SQL reader node
4. Letting the SQL reader/connector figure out how to execute it

The strategy's job is ONLY to identify what can be pushed - not to generate SQL.
SQL generation is the connector's responsibility.
"""

import contextlib
from typing import Dict
from typing import List
from typing import Tuple

from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext

# Operations that can potentially be pushed to SQL databases
PUSHABLE_OPERATIONS = {
    LogicalPlanStepType.Filter,
    LogicalPlanStepType.Project,
    LogicalPlanStepType.Limit,
    LogicalPlanStepType.Order,
    LogicalPlanStepType.Aggregate,
    LogicalPlanStepType.AggregateAndGroup,
    LogicalPlanStepType.Join,
}


class RemoteDatabasePushdownStrategy(OptimizationStrategy):
    """
    Strategy to identify pushable subtrees and attach them to SQL reader nodes.
    """

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        """
        No-op during visit phase. All work happens in complete().
        """
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore
        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        """
        Find SQL reader nodes, walk up to find pushable subtrees, attach them to readers.
        """
        if not context.optimized_plan or len(getattr(context.optimized_plan, "_nodes", {})) == 0:
            return context.pre_optimized_tree

        working_plan = context.optimized_plan

        # Find all SQL reader nodes
        sql_readers = self._find_sql_readers(working_plan)

        if not sql_readers:
            return working_plan

        # Group readers by connection string (same connection can share operations)
        readers_by_connection = self._group_by_connection(sql_readers)

        # For each connection group, try to find pushable subtrees
        for connection_id, readers in readers_by_connection.items():
            import sys

            print(
                f">>> Processing connection {connection_id} with {len(readers)} readers",
                file=sys.stderr,
            )
            self._try_push_subtree(working_plan, readers, connection_id)

        return working_plan

    def should_i_run(self, plan: LogicalPlan) -> bool:
        """
        Only run if we have SQL reader nodes in the plan.
        """
        for _, node in plan.nodes(data=True):
            if node.node_type == LogicalPlanStepType.Scan:
                connector = getattr(node, "connector", None)
                if connector and getattr(connector, "__type__", None) == "SQL":
                    return True
        return False

    def _find_sql_readers(self, plan: LogicalPlan) -> List[Tuple[str, LogicalPlanNode]]:
        """
        Find all SQL-backed reader nodes in the plan.
        Returns list of (node_id, node) tuples.
        """
        sql_readers = []
        for nid, node in plan.nodes(data=True):
            if node.node_type == LogicalPlanStepType.Scan:
                connector = getattr(node, "connector", None)
                if connector and getattr(connector, "__type__", None) == "SQL":
                    sql_readers.append((nid, node))
        return sql_readers

    def _group_by_connection(
        self, readers: List[Tuple[str, LogicalPlanNode]]
    ) -> Dict[str, List[Tuple[str, LogicalPlanNode]]]:
        """
        Group SQL readers by their connection string.
        Readers with the same connection can potentially share operations (e.g., joins).
        """
        import sys

        groups: Dict[str, List[Tuple[str, LogicalPlanNode]]] = {}

        for nid, node in readers:
            connector = getattr(node, "connector", None)
            if not connector:
                continue

            # Use actual connection string, not object id
            connection_string = getattr(connector, "connection", None) or getattr(
                connector, "connection_string", None
            )
            # Fall back to object id if no connection string
            connection_id = str(id(connector)) if connection_string is None else connection_string

            print(f">>> Reader {nid} has connection_id: {connection_id}", file=sys.stderr)

            if connection_id not in groups:
                groups[connection_id] = []
            groups[connection_id].append((nid, node))

        return groups

    def _try_push_subtree(
        self, plan: LogicalPlan, readers: List[Tuple[str, LogicalPlanNode]], _connection_id: str
    ) -> None:
        """
        Try to find and push a subtree for this group of readers.

        Algorithm:
        1. Start from each reader
        2. Walk up the tree while operations are pushable
        3. Stop when we hit a non-pushable operation or multiple data sources
        4. Attach the subtree to the reader(s)
        """
        # For now, handle single reader case
        # Multi-reader (join) case is more complex and will be added later
        if len(readers) == 1:
            reader_nid, reader_node = readers[0]
            self._push_single_reader_subtree(plan, reader_nid, reader_node)
        else:
            # Multi-reader case - look for common ancestors (joins)
            self._push_multi_reader_subtree(plan, readers, _connection_id)

    def _push_single_reader_subtree(
        self, plan: LogicalPlan, reader_nid: str, reader_node: LogicalPlanNode
    ) -> None:
        """
        Walk up from a single reader and find the pushable subtree.
        """
        import sys

        print(f">>> Walking from reader {reader_nid}", file=sys.stderr)

        # Walk up from the reader
        current_nid = reader_nid
        pushable_nodes = [reader_nid]

        while True:
            # Get parent nodes
            parents = list(plan.outgoing_edges(current_nid))

            print(f">>> Node {current_nid} has {len(parents)} parents", file=sys.stderr)

            if len(parents) == 0:
                # Reached the top of the plan
                break

            if len(parents) > 1:
                # Multiple parents - can't continue (this shouldn't happen in a tree)
                break

            _, parent_nid, _ = parents[0]
            parent_node = plan[parent_nid]
            parent_node.nid = parent_nid  # type: ignore

            print(
                f">>> Checking if {parent_nid} ({parent_node.node_type}) is pushable",
                file=sys.stderr,
            )

            # Check if this operation is pushable
            if not self._is_pushable_operation(parent_node, plan, pushable_nodes):
                # Hit a non-pushable operation, stop here
                print(f">>> Node {parent_nid} is NOT pushable, stopping", file=sys.stderr)
                break

            print(f">>> Node {parent_nid} IS pushable", file=sys.stderr)

            # Add to pushable set
            pushable_nodes.append(parent_nid)
            current_nid = parent_nid

        print(f">>> Found {len(pushable_nodes)} pushable nodes: {pushable_nodes}", file=sys.stderr)

        # If we found more than just the reader, we have a pushable subtree
        if len(pushable_nodes) > 1:
            # Attach the subtree to the reader
            self._attach_subtree_to_reader(plan, reader_nid, reader_node, pushable_nodes)

    def _push_multi_reader_subtree(
        self, plan: LogicalPlan, readers: List[Tuple[str, LogicalPlanNode]], _connection_id: str
    ) -> None:
        """
        Handle case where multiple readers from same connection might be joined.

        Strategy: Find the JOIN node connecting them, push everything including the JOIN
        into the right-leg reader, and remove all pushed nodes from the plan.
        """
        import sys

        print(
            f">>> Multi-reader case: {len(readers)} readers from same connection", file=sys.stderr
        )

        # Find common parent nodes - look for JOINs
        reader_nids = {nid for nid, _ in readers}

        # Walk up from each reader to find JOIN nodes
        for reader_nid, reader_node in readers:
            parents = list(plan.outgoing_edges(reader_nid))
            if len(parents) == 0:
                continue

            _, parent_nid, _ = parents[0]
            parent_node = plan[parent_nid]

            # Is this a JOIN?
            if parent_node.node_type == LogicalPlanStepType.Join:
                print(f">>> Found JOIN node {parent_nid}", file=sys.stderr)

                # Get both children of the JOIN
                join_children = list(plan.ingoing_edges(parent_nid))
                if len(join_children) != 2:
                    continue

                left_nid, _, _ = join_children[0]
                right_nid, _, _ = join_children[1]

                print(f">>> JOIN children: left={left_nid}, right={right_nid}", file=sys.stderr)

                # Check if both children are in our reader set (or their descendants are)
                left_is_sql_reader = left_nid in reader_nids
                right_is_sql_reader = right_nid in reader_nids

                if left_is_sql_reader and right_is_sql_reader:
                    print(
                        ">>> Both legs are SQL readers - pushing JOIN to right leg", file=sys.stderr
                    )

                    # Push the entire JOIN subtree into the right reader
                    # This includes: right reader, left reader, and the JOIN itself
                    self._push_join_to_reader(plan, parent_nid, left_nid, right_nid)
                    return

        # If we didn't find a join, fall back to single-reader pushdown
        print(">>> No JOIN found, falling back to single-reader pushdown", file=sys.stderr)
        for reader_nid, reader_node in readers:
            self._push_single_reader_subtree(plan, reader_nid, reader_node)

    def _is_pushable_operation(
        self, node: LogicalPlanNode, plan: LogicalPlan, already_pushable: List[str]
    ) -> bool:
        """
        Check if an operation can be pushed to the SQL database.

        Rules:
        - Must be in PUSHABLE_OPERATIONS set
        - If it's a join, both children must be SQL readers from same connection
        - Must not reference external data sources
        """
        # Check if operation type is pushable
        if node.node_type not in PUSHABLE_OPERATIONS:
            return False

        # Special case: Join nodes need both children to be pushable
        if node.node_type == LogicalPlanStepType.Join:
            children = list(plan.ingoing_edges(node.nid))
            if len(children) != 2:
                return False

            # Both children must be either SQL readers or already in pushable set
            for child_nid, _, _ in children:
                child_node = plan[child_nid]

                # Is it a SQL reader?
                is_sql_reader = (
                    child_node.node_type == LogicalPlanStepType.Scan
                    and getattr(child_node, "connector", None)
                    and getattr(child_node.connector, "__type__", None) == "SQL"
                )

                # Or already in our pushable set?
                is_already_pushable = child_nid in already_pushable

                if not (is_sql_reader or is_already_pushable):
                    return False

        # Operation is pushable
        return True

    def _push_join_to_reader(
        self, plan: LogicalPlan, join_nid: str, left_nid: str, right_nid: str
    ) -> None:
        """
        Push a JOIN and both its reader children into the right-leg reader.

        This creates a subtree containing:
        - The JOIN node
        - The left reader node
        - The right reader node
        - Any operations above the JOIN that can also be pushed

        The subtree is attached to the right reader, and all nodes (except the right reader)
        are removed from the main plan.
        """
        import sys

        print(f">>> Pushing JOIN {join_nid} into right reader {right_nid}", file=sys.stderr)

        # Start with the nodes we know: left reader, right reader, join
        subtree_nodes = [left_nid, right_nid, join_nid]

        # Walk up from the JOIN to find more pushable operations
        current_nid = join_nid
        while True:
            parents = list(plan.outgoing_edges(current_nid))

            if len(parents) == 0:
                break

            if len(parents) > 1:
                break

            _, parent_nid, _ = parents[0]
            parent_node = plan[parent_nid]

            print(
                f">>> Checking if {parent_nid} ({parent_node.node_type}) above JOIN is pushable",
                file=sys.stderr,
            )

            # Check if pushable (and not a JOIN - we already have one)
            if parent_node.node_type != LogicalPlanStepType.Join and self._is_pushable_operation(
                parent_node, plan, subtree_nodes
            ):
                print(f">>> Adding {parent_nid} to subtree", file=sys.stderr)
                subtree_nodes.append(parent_nid)
                current_nid = parent_nid
            else:
                print(f">>> Stopping at {parent_nid}", file=sys.stderr)
                break

        print(f">>> Subtree has {len(subtree_nodes)} nodes: {subtree_nodes}", file=sys.stderr)

        # Attach the subtree to the RIGHT reader (not left)
        right_reader = plan[right_nid]
        self._attach_subtree_to_reader(plan, right_nid, right_reader, subtree_nodes)

    def _attach_subtree_to_reader(
        self,
        plan: LogicalPlan,
        reader_nid: str,
        reader_node: LogicalPlanNode,
        subtree_nodes: List[str],
    ) -> None:
        """
        Attach the identified subtree to the reader node.

        The reader/connector will walk this subtree and generate appropriate SQL.

        Steps:
        1. Extract the subtree as a separate graph
        2. Attach it to the reader node as 'subquery_plan'
        3. Rewire the main plan to bypass the pushed operations
        """
        # Store the top node of the pushable subtree
        top_node_nid = subtree_nodes[-1]  # Last node is the top of the subtree

        # Create a subgraph with just the pushable nodes
        subquery_plan = LogicalPlan()
        for nid in subtree_nodes:
            node = plan[nid]
            subquery_plan.add_node(nid, node)

        # Copy edges within the subtree
        for source_nid in subtree_nodes:
            for _, target_nid, relationship in plan.outgoing_edges(source_nid):
                if target_nid in subtree_nodes:
                    subquery_plan.add_edge(source_nid, target_nid, relationship)
        # Diagnostic: print nodes and edges in the subquery_plan
        import sys

        with contextlib.suppress(Exception):
            edges = list(subquery_plan.edges())
            nodes = list(subquery_plan.nodes(data=True))
            print(
                f">>> SUBQUERY PLAN NODES: {[(n, type(a.node_type)) for n, a in nodes]}",
                file=sys.stderr,
            )
            print(f">>> SUBQUERY PLAN EDGES: {edges}", file=sys.stderr)

        # Attach the subtree to the reader
        reader_node.pushdown_enabled = True
        reader_node.subquery_plan = subquery_plan
        reader_node.pushdown_top_node = top_node_nid

        # Debug: Log what we're pushing
        print(
            f">>> PUSHDOWN: Attaching subtree with {len(subtree_nodes)} nodes to reader {reader_nid}",
            file=sys.stderr,
        )
        print(
            f">>> PUSHDOWN: Top node is {top_node_nid} ({plan[top_node_nid].node_type})",
            file=sys.stderr,
        )
        print(f">>> PUSHDOWN: Subtree nodes: {subtree_nodes}", file=sys.stderr)

        # Rewire the main plan:
        # 1. Find what the top node connects to (its parents)
        # 2. Connect the reader directly to those parents
        # 3. Remove all the pushed nodes except the reader

        top_node_parents = list(plan.outgoing_edges(top_node_nid))

        for _, parent_nid, relationship in top_node_parents:
            # Remove the edge from top_node to parent
            plan.remove_edge(top_node_nid, parent_nid, relationship)
            # Add edge from reader to parent
            plan.add_edge(reader_nid, parent_nid, relationship)

        # Remove all pushed nodes from the plan except the reader
        # Remove edges first, then remove the node without healing the graph.
        # This prevents the graph "heal" logic from reconnecting nodes and
        # leaving a JOIN with only one leg.
        for nid in subtree_nodes:
            if nid == reader_nid:
                continue

            # Remove all outgoing edges
            for _, target, rel in list(plan.outgoing_edges(nid)):
                with contextlib.suppress(Exception):
                    plan.remove_edge(nid, target, rel)

            # Remove all ingoing edges
            for source, _, rel in list(plan.ingoing_edges(nid)):
                with contextlib.suppress(Exception):
                    plan.remove_edge(source, nid, rel)

            # Finally remove the node without healing (we've already rewired parents)
            with contextlib.suppress(Exception):
                plan.remove_node(nid, heal=False)
