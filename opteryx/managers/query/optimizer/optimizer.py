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
This is a naive initial impementation of a query optimizer.

It is simply a set of rules which are executed in turn.

Query optimizers are the magic in a query engine, this is not magic, but all complex
things emerged from simple things, we we've set a low bar to get started on
implementing optimization.
"""

from opteryx.managers.query.optimizer import rules

RULESET: list = [
    rules.split_commutive_predicates.run
]

# split commutive expressions into multiple where filters (ANDs) - gives more opportunity to push down
# move selection nodes
# choose join based on the fields in the column (I don't know if there's a performance choice)


def run_optimizer(plan):

    for rule in RULESET:
        plan = rule(plan)

    return plan
