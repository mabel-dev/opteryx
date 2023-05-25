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
