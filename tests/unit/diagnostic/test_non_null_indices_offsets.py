import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pyarrow as pa


def test_non_null_indices_with_chunk_offsets():
    try:
        from opteryx.compiled.table_ops.null_avoidant_ops import non_null_indices
    except ImportError:
        import pytest
        pytest.skip("Cython null_avoidant_ops not available")

    # Build an array with some nulls and then create a chunked array where one chunk is sliced
    base = [1, None, 3, None, 5, 6]
    arr = pa.array(base, type=pa.int64())

    # Create two chunks; second chunk will be a slice to create non-zero offset
    c0 = pa.array(base[:3], type=pa.int64())
    c1_src = pa.array([None] + base[3:], type=pa.int64())
    c1 = c1_src.slice(1, len(base) - 3)

    chunked = pa.chunked_array([c0, c1])
    table = pa.table({"v": chunked})

    idxs = non_null_indices(table, ["v"]).tolist()

    # Expected non-null positions in original full array
    expected = [i for i, v in enumerate(base) if v is not None]
    assert idxs == expected

if __name__ == "__main__":
    from tests import run_tests

    run_tests()
