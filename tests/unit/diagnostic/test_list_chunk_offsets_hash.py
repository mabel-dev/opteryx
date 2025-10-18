import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))


import pyarrow as pa


def test_list_chunk_offsets_hash_consistency():
    try:
        from opteryx.compiled.table_ops.hash_ops import compute_hashes
    except ImportError:
        import pytest
        pytest.skip("Cython hash_ops not available")

    full = [[1, 2], [3], [], None, [4, 5]]
    list_type = pa.list_(pa.int64())

    # Flat table
    t_flat = pa.table({"l": pa.array(full, type=list_type)})

    # Build a chunked column where the second chunk is a slice (non-zero offset)
    chunk0 = pa.array(full[:2], type=list_type)
    chunk1_src = pa.array([None] + full[2:], type=list_type)
    # slice away the leading None so this chunk has a non-zero offset into its buffers
    chunk1 = chunk1_src.slice(1, len(full) - 2)
    chunked = pa.chunked_array([chunk0, chunk1])
    t_chunked = pa.Table.from_arrays([chunked], names=["l"])

    h_flat = compute_hashes(t_flat, ["l"])
    h_chunked = compute_hashes(t_chunked, ["l"])

    assert list(h_flat) == list(h_chunked)

if __name__ == "__main__":
    from tests import run_tests

    run_tests()
