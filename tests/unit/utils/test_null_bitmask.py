
import os
import sys
import numpy
import pyarrow

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.compiled.table_ops.null_avoidant_ops import non_null_indices

def test_all_valid():
    table = pyarrow.table({
        "a": pyarrow.array([1, 2, 3], pyarrow.int64()),
        "b": pyarrow.array([4, 5, 6], pyarrow.int64())
    })
    result = non_null_indices(table, ["a", "b"])
    assert numpy.array_equal(result, numpy.array([0, 1, 2]))


def test_all_null_in_one_column():
    table = pyarrow.table({
        "a": pyarrow.array([None, None, None], pyarrow.int64()),
        "b": pyarrow.array([1, 2, 3], pyarrow.int64())
    })
    result = non_null_indices(table, ["a", "b"])
    assert len(result) == 0


def test_some_nulls():
    table = pyarrow.table({
        "a": pyarrow.array([1, None, 3], pyarrow.int64()),
        "b": pyarrow.array([4, 5, None], pyarrow.int64())
    })
    result = non_null_indices(table, ["a", "b"])
    assert numpy.array_equal(result, numpy.array([0]))


def test_sparse_nulls_large():
    a = pyarrow.array([i if i % 17 != 0 else None for i in range(1000)], pyarrow.int64())
    b = pyarrow.array([i if i % 23 != 0 else None for i in range(1000)], pyarrow.int64())
    table = pyarrow.table({"a": a, "b": b})
    result = non_null_indices(table, ["a", "b"])
    expected = numpy.array([
        i for i in range(1000)
        if (i % 17 != 0 and i % 23 != 0)
    ])
    assert numpy.array_equal(result, expected)


def test_non_contiguous_nulls():
    table = pyarrow.table({
        "a": pyarrow.array([None, 1, None, 2, None, 3], pyarrow.int64()),
        "b": pyarrow.array([10, 11, None, 12, None, 13], pyarrow.int64())
    })
    result = non_null_indices(table, ["a", "b"])
    assert numpy.array_equal(result, numpy.array([1, 3, 5]))


def test_multiple_chunks():
    # Simulate chunking manually
    chunk1 = pyarrow.array([1, None, 3], pyarrow.int64())
    chunk2 = pyarrow.array([4, 5, None], pyarrow.int64())
    col = pyarrow.chunked_array([chunk1, chunk2])
    table = pyarrow.table({
        "a": col,
        "b": pyarrow.array([10, 20, 30, 40, 50, 60], pyarrow.int64())
    })
    result = non_null_indices(table, ["a", "b"])
    assert numpy.array_equal(result, numpy.array([0, 2, 3, 4]))


def test_column_with_no_nulls_and_one_with_some():
    table = pyarrow.table({
        "a": pyarrow.array([1, 2, 3, 4, 5], pyarrow.int64()),
        "b": pyarrow.array([10, None, 30, None, 50], pyarrow.int64())
    })
    result = non_null_indices(table, ["a", "b"])
    assert numpy.array_equal(result, numpy.array([0, 2, 4]))
    

def test_single_column_all_null():
    table = pyarrow.table({"a": pyarrow.array([None] * 100, type=pyarrow.int32())})
    assert len(non_null_indices(table, ["a"])) == 0


def test_single_column_some_nulls():
    arr = [i if i % 2 == 0 else None for i in range(100)]
    table = pyarrow.table({"a": pyarrow.array(arr, type=pyarrow.int64())})
    expected = numpy.array([i for i in range(100) if i % 2 == 0])
    assert numpy.array_equal(non_null_indices(table, ["a"]), expected)


def test_wide_table_mixed_nulls():
    cols = {}
    expected = numpy.ones(2000, dtype=bool)
    for i in range(20):
        name = f"col_{i}"
        arr = [j if j % (i + 2) != 0 else None for j in range(2000)]
        cols[name] = pyarrow.array(arr, type=pyarrow.int32())
        expected &= numpy.array([(j % (i + 2)) != 0 for j in range(2000)], dtype=bool)
    table = pyarrow.table(cols)
    expected_indices = numpy.where(expected)[0]
    assert numpy.array_equal(non_null_indices(table, list(cols.keys())), expected_indices)


def test_all_combinations_three_columns():
    data = [
        (1, 1, 1), (None, 1, 1), (1, None, 1), (1, 1, None),
        (None, None, 1), (None, 1, None), (1, None, None), (None, None, None)
    ]
    table = pyarrow.table({
        "a": pyarrow.array([r[0] for r in data], type=pyarrow.int64()),
        "b": pyarrow.array([r[1] for r in data], type=pyarrow.int64()),
        "c": pyarrow.array([r[2] for r in data], type=pyarrow.int64())
    })
    assert numpy.array_equal(non_null_indices(table, ["a", "b", "c"]), numpy.array([0]))


def test_large_random_sparse_nulls():
    rng = numpy.random.default_rng(42)
    size = 100_000

    # Generate random data with ~5% nulls per column
    data = {
        "a": pyarrow.array([i if rng.random() > 0.05 else None for i in range(size)], pyarrow.int64()),
        "b": pyarrow.array([i if rng.random() > 0.05 else None for i in range(size)], pyarrow.int64()),
        "c": pyarrow.array([i if rng.random() > 0.05 else None for i in range(size)], pyarrow.int64())
    }

    table = pyarrow.table(data)

    # Correct way to check for nulls in PyArrow
    expected = numpy.array([
        i for i in range(size)
        if data["a"][i].is_valid and data["b"][i].is_valid and data["c"][i].is_valid
    ])

    result = non_null_indices(table, ["a", "b", "c"])

    if not numpy.array_equal(result, expected):
        diff = set(result) ^ set(expected)
        print(f"{len(diff)} differing indices: {list(diff)[:10]}")

        idx = list(diff)[0]
        print(f"a={data['a'][idx]}, b={data['b'][idx]}, c={data['c'][idx]}, result_idx={result[idx]}")

    assert numpy.array_equal(result, expected)


def test_utf8_and_int_mix_with_nulls():
    table = pyarrow.table({
        "a": pyarrow.array(["x", None, "z", None, "y"]),
        "b": pyarrow.array([1, 2, None, 4, None], pyarrow.int64())
    })
    assert numpy.array_equal(non_null_indices(table, ["a", "b"]), numpy.array([0]))


def test_all_but_one_row_nulls():
    table = pyarrow.table({
        "a": pyarrow.array([None, None, None, 99], pyarrow.int64()),
        "b": pyarrow.array([None, None, None, 42], pyarrow.int64())
    })
    assert numpy.array_equal(non_null_indices(table, ["a", "b"]), numpy.array([3]))


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
