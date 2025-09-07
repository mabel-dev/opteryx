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

from collections import Counter
from collections import defaultdict


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
    Normalize an arbitrary nested DNF-like structure into *flat DNF*:
      - returns a list of clauses, where each clause is a list of (col, op, val) tuples
      - correctly preserves OR-groups by distributing AND over OR
      - does NOT perform absorption/factoring; that's done later

    Accepted shapes (examples):
      - [(a), (b)]                         -> AND clause
      - [[(a)], [(b)]]                     -> OR group
      - [[(a)], [[(b)], [(c)]]]            -> a AND (b OR c)  --> expands to [(a,b), (a,c)]
      - [ [(a),(b)],  [(a),(c)] ]          -> already flat DNF (two clauses)
      - [ [common], [ [res1], [res2] ] ]   -> factored form (handled by distribution)
    """

    def is_predicate(x):
        return isinstance(x, tuple) and len(x) == 3

    def as_predicates(clause_like):
        # Flatten nested/padded lists inside a clause and return list of (col,op,val) tuples
        preds = []
        for p in clause_like:
            preds.extend(normalise_predicate(p))
        return preds

    def is_clause(x):
        # A clause is a list of predicates (tuples)
        return isinstance(x, list) and len(x) > 0 and all(is_predicate(p) for p in x)

    def is_or_group(x):
        return (
            isinstance(x, list)
            and len(x) > 0
            and all(isinstance(c, list) for c in x)  # still must be lists
            and any((all(is_predicate(p) for p in c)) for c in x if isinstance(c, list))
        )

    def is_factored_form(x):
        return (
            isinstance(x, list)
            and len(x) == 2
            and isinstance(x[0], list)
            and isinstance(x[1], list)
            and all(isinstance(c, list) for c in x[1])
        )

    def unwrap(expr):
        """Recursively strip redundant single-element wrappers at any depth."""
        # Peel this level
        while isinstance(expr, list) and len(expr) == 1 and isinstance(expr[0], list):
            expr = expr[0]

        # Recurse into children if it's still a list
        if isinstance(expr, list):
            return [unwrap(e) for e in expr]
        return expr

    def expand(expr):
        """
        Expand to flat DNF: returns List[List[predicate_tuple]]
        """
        # Base: a single predicate
        if is_predicate(expr):
            return [[expr]]

        # Base: a single clause (AND of predicates)
        if is_clause(expr):
            preds = as_predicates(expr)
            return [preds] if preds else []

        if is_factored_form(expr):
            common, or_group = expr
            common_preds = as_predicates(common)
            clauses = []
            for alt in or_group:
                for expanded in expand(alt):
                    clauses.append(common_preds + expanded)
            return clauses

        # Base: an OR group (list of clauses)
        if is_or_group(expr):
            clauses = []
            for c in expr:
                # each c is already a clause; normalize it
                clauses.extend(expand(c))  # expand(c) returns [list_of_preds]
            return clauses

        # General list: treat as AND of sub-expressions -> distribute across ORs
        if isinstance(expr, list):
            parts = [expand(sub) for sub in expr]  # each part is a list of clauses
            # Remove empty parts (they contribute nothing)
            parts = [p for p in parts if p]

            # If nothing left, no clauses
            if not parts:
                return []

            # Cross-product (AND across OR alternatives)
            acc = [[]]
            for part in parts:
                new_acc = []
                for left in acc:
                    for right in part:
                        new_acc.append(left + right)
                acc = new_acc
            return acc

        # Unknown shape -> no clauses
        return []

    unwrapped = unwrap(dnf)

    # Expand to flat DNF
    flat = expand(unwrapped)

    # Normalize each clause: dedup predicates within a clause; keep deterministic order
    norm = []
    for clause in flat:
        seen = set()
        clean = []
        for col, op, val in clause:
            if isinstance(val, list):
                val = tuple(val)
            key = (col, op, val)
            if key not in seen:
                seen.add(key)
                clean.append((col, op, val))
        if clean:
            norm.append(sorted(clean, key=_pred_key))

    # Deduplicate identical clauses
    out = []
    seen_clauses = set()
    for cl in norm:
        t = tuple(cl)
        if t not in seen_clauses:
            seen_clauses.add(t)
            out.append(cl)

    return out


def _pred_key(p):
    # Deterministic ordering for predicates (col, op, val)
    return (p[0], p[1], str(p[2]))


def factor_clauses(clauses):
    """
    Greedy factoring with deterministic ties:
      - Count predicate frequencies across clauses
      - Find max frequency
      - Group max-frequency predicates by their support set (indices of clauses they appear in)
      - If any support-set group has size > 1, factor that whole group together
      - Else factor a single predicate deterministically
    Input:  list[frozenset]   (each frozenset = a clause)
    Output: nested list structure
      - flat: [list_of_predicates] for a clause
      - factored: [ list(common_preds), [with_branch, without_branch] ]
    """
    if not clauses:
        return []

    # If all clauses are empty (shouldn't happen post-absorption), return []
    if all(len(c) == 0 for c in clauses):
        return []

    # Frequency & support sets
    counts = Counter(p for c in clauses for p in c)

    # If nothing repeats, just return flat
    max_freq = max(counts.values())
    if max_freq <= 1:
        return [sorted(list(c), key=_pred_key) for c in clauses]

    # Build support sets (clause index membership) for each predicate
    support = defaultdict(set)  # pred -> set(indices)
    for i, c in enumerate(clauses):
        for p in c:
            support[p].add(i)

    # Consider only max-frequency predicates
    max_preds = [p for p, cnt in counts.items() if cnt == max_freq]

    # Group by identical support set
    groups_by_support = defaultdict(list)  # frozenset(indices) -> list[preds]
    for p in max_preds:
        groups_by_support[frozenset(support[p])].append(p)

    # Choose the best group: largest number of predicates; deterministic tiebreak on predicate keys
    best_sig, best_group = None, None
    for sig, preds in groups_by_support.items():
        preds_sorted = sorted(preds, key=_pred_key)
        if best_group is None:
            best_sig, best_group = sig, preds_sorted
        else:
            # prefer larger groups; then lexicographically smaller predicate list
            if len(preds_sorted) > len(best_group) or (
                len(preds_sorted) == len(best_group) and preds_sorted < best_group
            ):
                best_sig, best_group = sig, preds_sorted

    # If the best group is only a single predicate, we still get determinism,
    # but we didn't get multi-predicate factoring. That's fine.
    preds_to_factor = set(best_group)

    # Split clauses into with/without (w.r.t. the chosen support signature)
    # best_sig are exactly the clause indices containing ALL preds in preds_to_factor
    with_indices = set(best_sig)
    with_clauses = [clauses[i] for i in sorted(with_indices)]
    without_clauses = [c for i, c in enumerate(clauses) if i not in with_indices]

    # Reduce the 'with' clauses by removing the factored predicates
    reduced_with = [c - preds_to_factor for c in with_clauses]

    # If any reduced clause becomes empty -> (∧preds_to_factor) AND (True OR …) == ∧preds_to_factor
    if reduced_with:
        if all(len(c) == 0 for c in reduced_with):
            return [sorted(list(preds_to_factor), key=_pred_key)]
        factored_with = factor_clauses([c for c in reduced_with if c])
    else:
        factored_with = []

    # Handle without branch
    factored_without = factor_clauses(without_clauses) if without_clauses else []

    common_block = sorted(list(preds_to_factor), key=_pred_key)

    result = []
    if factored_with:
        result.append([common_block, factored_with])
    if factored_without:
        result.extend(factored_without)

    if isinstance(result, list) and len(result) == 1 and isinstance(result[0], list):
        return result[0]
    return result


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
    factored = factor_clauses(absorbed)
    return factored
