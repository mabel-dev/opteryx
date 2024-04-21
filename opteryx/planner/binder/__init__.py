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
         │SQL                                  │Results
   ┌─────▼─────┐                         ┌─────┴─────┐
   │           │                         │           │
   │ Parser    │                         │ Executor  │
   └─────┬─────┘                         └─────▲─────┘
         │AST                                  │Plan
   ┌─────▼─────┐                         ╔═══════════╗
   │ AST       │                         ║Cost-Based ║
   │ Rewriter  │                         ║ Optimizer ║
   └─────┬─────┘                         ╚═════▲═════╝
         │AST                                  │Plan
   ┌─────▼─────┐      ┌───────────┐      ┌─────┴─────┐
   │ Logical   │ Plan │ Heuristic │ Plan │           │
   │   Planner ├──────► Optimizer ├──────► Binder    │
   └───────────┘      └───────────┘      └─────▲─────┘
                                               │Schemas
                                         ┌─────┴─────┐
                                         │           │
                                         │ Catalogue │
                                         └───────────┘
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
from opteryx.exceptions import InvalidInternalStateError
from opteryx.planner.binder.binder_visitor import BinderVisitor
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.planner.logical_planner import LogicalPlan


def do_bind_phase(plan: LogicalPlan, connection=None, qid: str = None) -> LogicalPlan:
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

    # DEBUG: log ("AFTER BINDING")
    # DEBUG: log (plan.draw())

    return plan
