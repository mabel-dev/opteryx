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

This is only used for small numbers of adjacent predicates - up to four. That is only 16 variations to test.
"""

import itertools


def calculate_predicate_costs(predicate_selectivity, predicate_execution_time):
    # Generate all possible predicate arrangements
    predicate_arrangements = list(itertools.permutations(range(len(predicate_selectivity))))

    # Evaluate execution time for each arrangement considering selectivity
    arrangement_costs = {}
    for arrangement in predicate_arrangements:
        cumulative_data_size = 1.0  # Assume initial data size is 1.0 (100%)
        execution_time = 0.0
        for i in arrangement:
            execution_time += predicate_execution_time[i] * cumulative_data_size
            cumulative_data_size *= predicate_selectivity[
                i
            ]  # Reduce data size based on selectivity
        arrangement_costs[arrangement] = execution_time

    return arrangement_costs


def print_arrangement_costs(arrangement_costs):
    for arrangement, cost in arrangement_costs.items():
        print(f"Arrangement {arrangement}: Execution Time = {cost}")


# Sample predicate selectivity and execution time estimates
predicate_selectivity = [0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5]
predicate_execution_time = [0.8, 0.2, 0.5, 0.1, 0.1, 0.4, 0.7]

# Calculate the costs for each arrangement
arrangement_costs = calculate_predicate_costs(predicate_selectivity, predicate_execution_time)

# Print the costs
print_arrangement_costs(arrangement_costs)
