import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pyarrow as pa
from opteryx.compiled.table_ops.hash_ops import compute_hashes


def test_list_of_ints_matches_per_element_hashing():
    N = 1000
    data = [[i % 7 for _ in range(3)] for i in range(N)]
    t = pa.table({"l": pa.array(data)})

    # Compute hashes using our function
    h1 = compute_hashes(t, ["l"])
    # Compute a Python-level reference by hashing tuples per-row (exact parity)
    # Compute a reference using the same mixing logic as the Cython implementation
    SEED = 0x9e3779b97f4a7c15 & 0xFFFFFFFFFFFFFFFF
    c1 = 0xbf58476d1ce4e5b9 & 0xFFFFFFFFFFFFFFFF
    c2 = 0x94d049bb133111eb & 0xFFFFFFFFFFFFFFFF
    FINAL_MUL = 0x9e3779b97f4a7c15 & 0xFFFFFFFFFFFFFFFF

    ref = []
    for row in data:
        # per-list hash
        if len(row) == 0:
            list_hash = 0xab52d8afc1448992 & 0xFFFFFFFFFFFFFFFF  # EMPTY_HASH
        else:
            list_hash = SEED
            for elem in row:
                elem_hash = elem & 0xFFFFFFFFFFFFFFFF
                # mix element
                list_hash = elem_hash ^ list_hash
                list_hash = (list_hash ^ (list_hash >> 30)) * c1 & 0xFFFFFFFFFFFFFFFF
                list_hash = (list_hash ^ (list_hash >> 27)) * c2 & 0xFFFFFFFFFFFFFFFF
                list_hash = list_hash ^ (list_hash >> 31)

        # final row mix (update_row_hash behavior)
        h = 0
        h = (h ^ list_hash) * FINAL_MUL & 0xFFFFFFFFFFFFFFFF
        h ^= (h >> 32)
        ref.append(h)

    assert len(h1) == len(ref)
    for i in range(len(ref)):
        assert h1[i] == ref[i]


def test_list_of_strings_and_chunked_slices():
    # Create long list and then create a chunked / sliced version
    data = [[f'str{i % 10}' for _ in range(2)] for i in range(2000)]
    arr = pa.array(data)
    # make chunked by slicing into two
    a1 = arr.slice(0, 1200)
    a2 = arr.slice(1200)
    chunked = pa.chunked_array([a1, a2])
    t_chunked = pa.table({"l": chunked})

    t_flat = pa.table({"l": arr})

    h_flat = compute_hashes(t_flat, ["l"])
    h_chunked = compute_hashes(t_chunked, ["l"])

    assert len(h_flat) == len(h_chunked)
    for i in range(len(h_flat)):
        assert h_flat[i] == h_chunked[i]


def test_nested_and_boolean_lists():
    # Nested lists of primitives
    data_nested = [[[i % 3, (i + 1) % 3] for _ in range(2)] for i in range(500)]
    t_nested = pa.table({"nl": pa.array(data_nested)})

    # Boolean lists
    data_bool = [[(i + j) % 2 == 0 for j in range(4)] for i in range(500)]
    t_bool = pa.table({"bl": pa.array(data_bool)})

    hn = compute_hashes(t_nested, ["nl"])
    hb = compute_hashes(t_bool, ["bl"])

    assert len(hn) == 500
    assert len(hb) == 500

if __name__ == "__main__":
    from tests import run_tests

    run_tests()
