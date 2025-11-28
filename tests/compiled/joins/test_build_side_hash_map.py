import numpy
import pyarrow as pa

from opteryx.compiled.joins.join_definitions import (
    build_side_hash_map,
)
from opteryx.compiled.table_ops.hash_ops import compute_hashes
from opteryx.compiled.table_ops.null_avoidant_ops import non_null_indices


def test_build_side_hash_map_basic_values():
    """
    Verify that build_side_hash_map produces a FlatHashMap containing a mapping for
    every non-null row hash computed from a small VALUES table.
    """
    table = pa.table({"x": [1, 2, 3]})

    # Build the Cython-side hash map
    ht = build_side_hash_map(table, ["x"])

    # Compute the row hashes and non-null indices via the compiled helper
    num_rows = table.num_rows
    row_hashes = compute_hashes(table, ["x"])  # returns array.array('Q')
    non_nulls = non_null_indices(table, ["x"])  # typed memoryview

    # For every non-null row, ensure the hash map returns a non-empty list
    for i in range(non_nulls.shape[0]):
        row_idx = int(non_nulls[i])
        key = int(row_hashes[row_idx])
        found = ht.get(key)
        assert found is not None and len(found) >= 1, f"Missing mapping for hash {key} (row {row_idx})"
        assert row_idx in found, f"Row idx {row_idx} not present in mapping for hash {key}: {found}"

