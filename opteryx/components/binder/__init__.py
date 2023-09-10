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
This is Binder, it sits between the Logical Planner and the Optimizers.

~~~
                      ┌───────────┐
                      │   USER    │
         ┌────────────┤           ◄────────────┐
         │SQL         └───────────┘            │
  ───────┼─────────────────────────────────────┼──────
         │                                     │
   ┌─────▼─────┐                               │
   │ SQL       │                               │
   │  Rewriter │                               │
   └─────┬─────┘                               │
         │SQL                                  │Plan
   ┌─────▼─────┐                         ┌─────┴─────┐
   │           │                         │           │
   │ Parser    │                         │ Executor  │
   └─────┬─────┘                         └─────▲─────┘
         │AST                                  │Plan
   ┌─────▼─────┐      ┌───────────┐      ┌─────┴─────┐
   │ AST       │      │           │Stats │Cost-Based │
   │ Rewriter  │      │ Catalogue ├──────► Optimizer │
   └─────┬─────┘      └─────┬─────┘      └─────▲─────┘
         │AST               │Schemas           │Plan
   ┌─────▼─────┐      ╔═════▼═════╗      ┌─────┴─────┐
   │ Logical   │ Plan ║           ║ Plan │ Heuristic │
   │   Planner ├──────►   Binder  ║──────► Optimizer │
   └───────────┘      ╚═══════════╝      └───────────┘
~~~

The Binder is responsible for adding information about the database and engine into the
Logical Plan.

The binder takes the the logical plan, and adds information from various catalogues
into that planand then performs some validation checks.

These catalogues include:
- The Data Catalogue (e.g. data schemas)
- The Function Catalogue (e.g. function inputs and data types)
- The Variable Catalogue (i.e. the @ variables)

The Binder performs these activities:
- schema lookup and propagation (add columns and types, add aliases)

"""


from opteryx.components.binder.binder_visitor import BinderVisitor
from opteryx.components.binder.binding_context import BindingContext
from opteryx.exceptions import InvalidInternalStateError
from opteryx.third_party.travers import Graph
from opteryx.virtual_datasets import derived


def do_bind_phase(plan, connection=None, qid: str = None, common_table_expressions=None):
    binder_visitor = BinderVisitor()
    root_node = plan.get_exit_points()
    context = {
        "schemas": {"$derived": derived.schema()},
        "qid": qid,
        "connection": connection,
        "relations": set(),
    }
    if len(root_node) > 1:
        raise InvalidInternalStateError(
            f"{qid} - logical plan has {len(root_node)} heads - this is an error"
        )
    plan, _ = binder_visitor.traverse(plan, root_node[0], context=context)
    return plan


def do_bind_phase(plan: Graph, connection=None, qid: str = None) -> Graph:
    """
    Execute the bind phase of the query engine.

    Parameters:
        plan: Any
            The logical plan.
        context: BindingContext
            The context needed for the binding phase.

    Returns:
        Modified logical plan after the binding phase.

    Raises:
        InvalidInternalStateError: Raised when the logical plan has more than one root node.
    """
    binder_visitor = BinderVisitor()
    root_node = plan.get_exit_points()
    context = BindingContext.initialize(qid=qid, connection=connection)

    if len(root_node) > 1:
        raise InvalidInternalStateError(
            f"{context.qid} - logical plan has {len(root_node)} heads - this is an error"
        )

    plan, _ = binder_visitor.traverse(plan, root_node[0], context=context)
    return plan
