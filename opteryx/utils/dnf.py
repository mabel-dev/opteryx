# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
dnf.py
-------
Utilities for handling Disjunctive Normal Form (DNF) filter expressions.

This module provides functions to normalize, flatten, and simplify DNF predicates,
which are commonly used for query filtering and logical expression optimization.

Functions:
    - normalise_predicate: Recursively flatten predicates to (col, op, val) tuples.
    - normalise_dnf: Canonicalize DNF structures to lists of predicate lists.
    - simplify_dnf: Deduplicate, absorb, and factor DNF clauses for efficient evaluation.
"""


def normalise_predicate(p):
    """
    Recursively flatten a predicate structure until we get a list of (col, op, val) tuples.
    Accepts nested lists/tuples and returns a flat list of predicates.
    Unknown shapes are ignored.
    """
    if isinstance(p, tuple) and len(p) == 3:
        # Base case: already a predicate tuple
        return [p]  # return as a list so caller can extend
    elif isinstance(p, list):
        # Recursively flatten each sub-predicate
        preds = []
        for sub in p:
            preds.extend(normalise_predicate(sub))
        return preds
    else:
        # Ignore unknown shapes
        return []


def normalise_dnf(dnf):
    """
    Force filters into canonical Disjunctive Normal Form (DNF): [[predicates], ...].
    Ensures each clause is a list of (col, op, val) tuples, and the DNF is a list of such clauses.
    Handles redundant nesting and single flat clauses.
    """
    if not dnf:
        return []

    normalised = []

    # Case: single clause written flat, e.g. [(a, '=', 1), (b, '>', 2)]
    if all(isinstance(p, tuple) and len(p) == 3 for p in dnf):
        dnf = [dnf]

    for clause in dnf:
        # Unwrap redundant nesting like [[[...]]]
        while isinstance(clause, list) and len(clause) == 1 and isinstance(clause[0], list):
            clause = clause[0]

        preds = []
        for p in clause:
            preds.extend(normalise_predicate(p))

        if preds:  # only add non-empty clauses
            normalised.append(preds)

    return normalised


def simplify_dnf(dnf):
    """
    Simplify a DNF (Disjunctive Normal Form) filter expression.
    Steps:
      1. Normalize predicates and remove duplicates within clauses.
      2. Deduplicate identical clauses.
      3. Absorb clauses that are supersets of others.
      4. Factor out global common predicates.
    Returns a simplified DNF structure.
    """

    def make_hashable(pred):
        """Convert a predicate to a hashable tuple for set operations."""
        col, op, val = pred
        if isinstance(val, list):
            val = tuple(val)
        return (col, op, val)

    if not dnf:
        return []

    # 1) Normalize: remove duplicate predicates inside each clause
    dnf = normalise_dnf(dnf)
    clauses = [frozenset(make_hashable(pred) for pred in clause) for clause in dnf if clause]

    if not clauses:
        return []

    # 2) Deduplicate identical clauses
    clauses = list(set(clauses))

    # 3) Absorption: drop clauses that are supersets of another
    # If a clause is a superset of another, it is redundant
    absorbed = []
    for c in clauses:
        if any((o != c) and o.issubset(c) for o in clauses):
            continue
        absorbed.append(c)

    if not absorbed:
        return []

    # 4) Factor global common predicates
    # Find predicates common to all clauses (ternary for conciseness)
    common = set(absorbed[0]).intersection(*absorbed[1:]) if len(absorbed) > 1 else set(absorbed[0])

    if not common:
        # No common factor: return clauses as lists of predicates
        return [list(c) for c in absorbed]

    reduced = [c - common for c in absorbed]

    # If any reduced clause is empty, OR collapses to just the common predicates
    if any(len(r) == 0 for r in reduced):
        return [list(common)]

    # Otherwise build nested structure: [common, reduced_OR]
    reduced_dnf = [list(r) for r in reduced]
    return [list(common), reduced_dnf]
