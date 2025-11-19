"""
Draken-based group-by prototype for simple aggregates.

This module implements a Python prototype using Draken's Morsel hashing
and the Abseil FlatHashMap to group rows quickly by hash-based buckets.

It supports simple aggregation functions: COUNT, SUM, MIN, MAX, COUNT_DISTINCT, AVG.

Note: This prototype lives in Python for iteration speed and leverages existing
compiled helpers like `count_distinct` where helpful. For production we will
move this into a Cython module for speed and to leverage native FlatHashMap
inserts without Python overhead.
"""

from typing import List
from typing import Optional
from typing import Tuple

import numpy as np
import pyarrow as pa

from opteryx.compiled.aggregations.count_distinct import count_distinct
from opteryx.draken.morsels.morsel import Morsel
from opteryx.third_party.abseil.containers import FlatHashMap

# Supported aggregator functions (in the same names used in AGGREGATORS mapping)
SUPPORTED_FUNCS = {"sum", "min", "max", "count", "count_distinct", "hash_one", "mean"}


def _take_array_as_numpy(arr: pa.Array, indices: List[int]):
    # Fast path: use to_numpy if available, otherwise fall back to to_pylist
    try:
        return arr.to_numpy(False)
    except Exception:
        return np.asarray(arr.to_pylist(), dtype=object)


def _compute_agg_value(vec, func: str, indices) -> object:
    """
    vec: draken Vector (has methods `take()` and `to_arrow()` or `to_arrow()` directly)
    func: one of SUPPORTED_FUNCS
    indices: list-like indices
    """
    # Handle COUNT as special
    if func == "count":
        return len(indices)
    if func == "count_distinct":
        # Build arrow array for selected indices and call compiled count_distinct
        arr = vec.to_arrow() if hasattr(vec, "to_arrow") else vec
        taken = arr.take(pa.array(indices, type=pa.int32()))
        seen = count_distinct(taken, None)
        # count_distinct returns a FlatHashSet
        return seen.items()
    # Convert vec.take to a vector for other functions
    try:
        taken_vec = vec.take(indices)
    except Exception:
        # fallback to arrow array take
        arr = vec.to_arrow() if hasattr(vec, "to_arrow") else vec
        taken_pa = arr.take(pa.array(indices, type=pa.int32()))
        # Convert to numpy and use numpy to compute
        npvals = _take_array_as_numpy(taken_pa)
        if func == "sum":
            return np.nansum(npvals)
        elif func == "min":
            return np.nanmin(npvals)
        elif func == "max":
            return np.nanmax(npvals)
        elif func == "mean":
            return float(np.nanmean(npvals))
        else:
            raise ValueError(f"Unsupported aggregator function: {func}")

    # If taken_vec exposes methods for sum/min/max
    if func == "sum":
        try:
            return taken_vec.sum()
        except Exception:
            arr = taken_vec.to_arrow()
            return pa.compute.sum(arr).as_py()
    elif func == "min":
        try:
            return taken_vec.min()
        except Exception:
            arr = taken_vec.to_arrow()
            return pa.compute.min(arr).as_py()
    elif func == "max":
        try:
            return taken_vec.max()
        except Exception:
            arr = taken_vec.to_arrow()
            return pa.compute.max(arr).as_py()
    elif func == "mean":
        try:
            return taken_vec.sum() / len(indices) if len(indices) > 0 else None
        except Exception:
            arr = taken_vec.to_arrow()
            return pa.compute.mean(arr).as_py()
    elif func == "hash_one":
        try:
            return taken_vec.sum()
        except Exception:
            arr = taken_vec.to_arrow()
            # Best-effort fallback: sum of values
            return pa.compute.sum(arr).as_py()
    else:
        raise ValueError(f"Unsupported aggregator function: {func}")


def group_by_morsel(
    morsel: Morsel,
    group_by_columns: List[str],
    aggregate_functions: List[Tuple[str, str, Optional[object]]],
    internal_names: List[str],
    column_names: List[str],
):
    """
    Prototype function to group a Draken Morsel using Draken hash and compute simple aggregates.

    Parameters:
        morsel: Draken Morsel
        group_by_columns: list[str]
        aggregate_functions: list[(field_name, function, count_options)] - matches ``self.aggregate_functions`` from the Python
          plan nodes
        internal_names: list[str] the names of the aggregated columns (internal), same length as aggregate_functions
        column_names: list[str] external names to rename aggregated columns to

    Returns:
        pyarrow.Table: grouped result with columns in order internal_names + group_by_columns
    """
    # Fast-path: empty table
    if morsel is None or morsel.num_rows == 0:
        # Return an empty table with the expected schema
        return pa.Table.from_pydict({name: [] for name in internal_names + group_by_columns})

    # Convert group_by columns to encoded bytes as Draken expects (Morsel.column accepts bytes too)
    gb_cols = [c if isinstance(c, bytes) else c.encode("utf-8") for c in group_by_columns]

    # Use Draken's hash per-row for grouping
    if len(gb_cols) == 0:
        # entire table is a single group
        # compute per-aggregator result over all rows
        indices = list(range(morsel.num_rows))
        row_groups = {0: indices}
    else:
        # row_hashes is a memoryview of uint64
        row_hashes = morsel.hash(columns=gb_cols)

        # Map 64-bit hash -> list of indices
        fmap = FlatHashMap()
        # iterate rows
        num_rows = morsel.num_rows
        for i in range(num_rows):
            h = int(row_hashes[i])
            fmap.insert(h, i)

        # Move map to Python dict of key->list(indices)
        # `fmap.get` returns a vector[int64]; we can treat it as list
        row_groups = {}
        # Iterate keys using fmap.size and collecting get(key) (we can't iterate keys; but we can get all internal map.get for known keys; however FlatHashMap doesn't expose keys.
        # Workaround: we can rebuild the Python dict by iterating rows and using dict
        row_groups = {}
        for i in range(num_rows):
            h = int(row_hashes[i])
            if h not in row_groups:
                row_groups[h] = [i]
            else:
                row_groups[h].append(i)

    # Now compute group-level aggregations
    result_internal = {name: [] for name in internal_names}
    result_group = {name: [] for name in group_by_columns}

    # We'll need the draken vector for aggregator columns
    for key, indices in row_groups.items():
        # For group by values (take first row value of each gb column)
        for gb_col in gb_cols:
            vec = morsel.column(gb_col)
            val = vec[indices[0]]
            # store under original str column name
            result_group[gb_col.decode("utf-8")].append(val)

        # Compute each aggregator
        for (agg_idx, (field_name, func_name, _)), internal_name in zip(
            enumerate(aggregate_functions), internal_names
        ):
            # field_name is the actual field to aggregate
            vec = morsel.column(
                field_name.encode("utf-8") if isinstance(field_name, str) else field_name
            )
            if func_name in ("hash_one", "hash_list"):
                # use sum as proxy for hash_one
                val = _compute_agg_value(vec, "sum", indices)
            else:
                val = _compute_agg_value(vec, func_name, indices)
            # For AVG, compute as sum/count
            if func_name == "mean":
                # already computed as mean in compute_agg_value
                pass
            result_internal[internal_name].append(val)

    # Build pyarrow Table
    # Order: internal_names + group_by_columns
    columns = {}
    for name in internal_names:
        columns[name] = result_internal[name]
    for gb in group_by_columns:
        columns[gb] = result_group[gb]

    table = pa.Table.from_pydict(columns)

    # Rename internal columns to user column_names
    if len(internal_names) == len(column_names) - len(group_by_columns):
        # column_names were alias names; original code renames columns after selecting; we'll mirror expected behavior
        # column_names includes alias names + group_by
        # So positionally name selectors: first portion are alias names
        alias_map = {}
        alias_columns = column_names[: len(internal_names)]
        for internal, alias in zip(internal_names, alias_columns):
            # rename by building a new table
            table = table.rename_columns(
                [alias if n == internal else n for n in table.column_names]
            )

    return table
