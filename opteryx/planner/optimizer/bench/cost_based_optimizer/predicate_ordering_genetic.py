# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Ordering of adjacent predicates using a genetic algorithm.

This is only used for larger numbers of predicates - five or more.

For 5 predicates, we do 4 generations of 3 variations (12 rather than 25 variations tested)
For 10 predicates, we do 5 generations of 6 variations (30 rather than 100)
Fro 100 predicates, we do 8 (upper limit) generations of 50 (limit) variations (400 rather 10,000)

This may not find the _best_ solution, but it should find a better than average solution
"""

import random


def log2(x):
    if x <= 0:
        raise ValueError("Invalid input. Logarithm is undefined for non-positive numbers.")

    result = 0
    while x > 1:
        x /= 2
        result += 1

    return result


# The number of times we mutate the orderings - capped at 8
generations = lambda x: min(log2(x) + 1, 8)
# The number of swaps per generation - capped at 50
mutations = lambda x: min((x // 2) + 1, 50)


def generate_initial_population(predicates):
    """
    Generate initial random ordering. For every order we randomly
    generate we also emit the same order in reverse. We were finding
    that if we started with a very inefficient order, that we were
    never getting to an efficient plan and by also reversing the order
    that we almost eliminated not getting a better than average order.
    """
    population = [tuple(predicates)]
    population_size = (len(predicates) // 2) + 1

    for _ in range(population_size):
        arrangement = random.sample(predicates, len(predicates))
        population.append(tuple(a for a in arrangement))
        arrangement.reverse()
        population.append(tuple(a for a in arrangement))

    return population


def evaluate_cost(arrangement, cost_model):
    # Evaluate the cost of an arrangement using the cost model
    # You should implement this function based on your specific cost model
    # The lower the cost, the better the arrangement
    return cost_model(arrangement)


def mutate(arrangement, variations=1):
    # Swap the order of two randomly selected predicates in the arrangement
    mutated_arrangement = list(arrangement)[:]
    for i in range(variations):
        idx1, idx2 = random.sample(range(len(arrangement)), 2)
        mutated_arrangement[idx1], mutated_arrangement[idx2] = (
            mutated_arrangement[idx2],
            mutated_arrangement[idx1],
        )
    return mutated_arrangement


def genetic_algorithm(predicates, cost_model, population_size, num_generations, num_mutations):
    population = generate_initial_population(predicates)

    mutations = max(int(len(predicates) / 5), 1)  # Ensure at least one mutation

    for i in range(num_generations):
        # Evaluate the cost for each individual in the population
        costs = [evaluate_cost(individual, cost_model) for individual in population]

        # Sort the population by cost in ascending order
        population = [x for _, x in sorted(zip(costs, population))]
        fastest_so_far = population[0]
        slowest_so_far = population[-1]
        print("Fastest So Far", fastest_so_far)

        if i == num_generations - 1:
            break
        if i == num_generations - 2:
            mutations = 1
        # include the fastest
        population = [fastest_so_far]

        # Mutate the current cheapest
        for _ in range(num_mutations):
            mutated_individual = mutate(fastest_so_far, mutations)
            population.append(tuple(mutated_individual))

        print("")

    # Return the best arrangement found
    best_arrangement = population[0]
    best_cost = evaluate_cost(best_arrangement, cost_model)

    print("naive cost", evaluate_cost(tuple(predicates), cost_model))
    print("worst cost", evaluate_cost(slowest_so_far, cost_model))

    return best_arrangement, best_cost


# Example usage
predicates = [
    (100, 0.5),
    (5000, 0.1),
    (1000000, 0.01),
    (1000, 0.2),
    (10, 0.9),
    (25, 0.7),
    (44, 0.66),
]  # List of predicates (time, selectivity)
population_size = mutations(len(predicates))  # capped at 50
num_generations = generations(len(predicates))  # capped at 8
num_mutations = population_size


seen = {}


def cost_model(arrangement):
    # Define your cost model here
    # Calculate the cost of the arrangement based on your specific criteria
    if arrangement in seen:
        return seen[arrangement]
    print(arrangement)
    approx_records = 1000000
    cost = 0
    for time, selectivity in arrangement:
        cost += time * (time / approx_records)
        approx_records *= selectivity
    print(cost, approx_records)
    seen[arrangement] = cost
    return cost


best_arrangement, best_cost = genetic_algorithm(
    predicates, cost_model, population_size, num_generations, num_mutations
)

print("Best Arrangement:", best_arrangement)
print("Best Cost:", best_cost)
