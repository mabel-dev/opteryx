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
Ordering of adjacent predicates using brute-force.

This is only used for small numbers of adjacent predicates - up to five. That is only 16 variations to test.
"""

import itertools

# Sample predicate selectivity and execution time estimates
predicate_selectivity = [0.8, 0.5, 0.3]
predicate_execution_time = [0.1, 0.2, 0.3]

# Generate all possible predicate arrangements
predicate_arrangements = list(itertools.permutations(range(len(predicate_selectivity))))

# Evaluate execution time for each arrangement
execution_times = []
for arrangement in predicate_arrangements:
    execution_time = sum(predicate_execution_time[i] for i in arrangement)
    execution_times.append(execution_time)

# Find the optimal arrangement with the lowest execution time
optimal_arrangement = predicate_arrangements[execution_times.index(min(execution_times))]

print("Optimal Predicate Arrangement:", optimal_arrangement)
print("Estimated Execution Time:", min(execution_times))
