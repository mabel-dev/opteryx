"""Arrow interoperability tests for Draken Vector types.

This module tests the zero-copy interoperability between Draken Vector types
and Apache Arrow arrays. It validates that:
- Draken can wrap Arrow arrays without copying data
- Operations on Draken vectors match Arrow compute results
- Round-trip conversion (Arrow -> Draken -> Arrow) preserves data integrity
- All supported data types (bool, int64, float64, binary) work correctly

The tests use parametrized test cases to cover different data types and
null value scenarios.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pytest
import pyarrow as pa
import pyarrow.compute as pc

from opteryx.draken import Vector

TESTS = [
    # Boolean: count trues
    (
        pa.array([True, False, True, None, False]),
        lambda arr: pc.sum(arr.cast(pa.int8())).as_py(),
        lambda vec: sum(vec.to_pylist()[i] or 0 for i in range(vec.length) if vec.to_pylist()[i] is not None),
    ),
    # Int64: sum
    (
        pa.array([1, 2, 3, None, 5], type=pa.int64()),
        lambda arr: pc.sum(arr).as_py(),
        lambda vec: vec.sum(),
    ),
    # Float64: sum
    (
        pa.array([1.5, 2.5, None, -1.0], type=pa.float64()),
        lambda arr: pc.sum(arr).as_py(),
        lambda vec: vec.sum(),
    ),
    # Binary: total length of all buffers
    (
        pa.array([b"a", b"bb", None, b"ccc"], type=pa.binary()),
        lambda arr: pc.sum(pc.binary_length(arr)).as_py(),
        lambda vec: sum(len(s) for s in vec.to_pylist() if s is not None),
    ),
    # Date32: min
    (
        pa.array([18000, 18500, None, 19000], type=pa.date32()),
        lambda arr: pc.min(arr).cast(pa.int32()).as_py(),
        lambda vec: vec.min(),
    ),
    # Timestamp: max
    (
        pa.array([1000000, None, 3000000, 2000000], type=pa.timestamp('us')),
        lambda arr: pc.max(arr).cast(pa.int64()).as_py(),
        lambda vec: vec.max(),
    ),
    # Time32: count non-null
    (
        pa.array([3600, 7200, None, 10800], type=pa.time32('s')),
        lambda arr: len(arr) - arr.null_count,
        lambda vec: vec.length - vec.null_count,
    ),
    # Time64: count non-null
    (
        pa.array([3600000000, None, 7200000000, None], type=pa.time64('us')),
        lambda arr: len(arr) - arr.null_count,
        lambda vec: vec.length - vec.null_count,
    ),
    # List/Array: count non-null
    (
        pa.array([[1, 2], [3], None, [4, 5, 6]], type=pa.list_(pa.int64())),
        lambda arr: len(arr) - arr.null_count,
        lambda vec: vec.length - vec.null_count,
    ),
]


@pytest.mark.parametrize("arrow_array,op_arrow,op_draken", TESTS)
def test_draken_roundtrip(arrow_array, op_arrow, op_draken):

    if hasattr(arrow_array, "combine_chunks"):
        arrow_array = arrow_array.combine_chunks()

    # Wrap Arrow array in Draken vector
    vec = Vector.from_arrow(arrow_array)

    # 1. Compare ops
    result_arrow = op_arrow(arrow_array)
    result_draken = op_draken(vec)
    assert result_arrow == result_draken, f"Draken and Arrow results differ: {result_draken} != {result_arrow}"

    # 2. Round trip back to Arrow and compare
    roundtrip = vec.to_arrow()
    # Normalize chunks (Arrow equality cares about that)
    assert roundtrip.equals(arrow_array), f"Round trip Arrow arrays differ: {roundtrip} != {arrow_array}"



if __name__ == "__main__":  # pragma: no cover
    # Running in the IDE we do some formatting - it's not functional but helps when reading the outputs.

    import shutil
    import time

    start_suite = time.monotonic_ns()
    width = shutil.get_terminal_size((80, 20))[0] - 15
    passed:int = 0
    failed:int = 0
    nl:str = "\n"
    failures = []

    print(f"RUNNING BATTERY OF {len(TESTS)} TESTS")
    for index, (arrow_array, op_arrow, op_draken) in enumerate(TESTS):
        print(
            f"\033[38;2;255;184;108m{(index + 1):04}\033[0m"
            f" {type(arrow_array.type).__name__}:{str(arrow_array.type).ljust(12)[:12]} ",
            end="",
            flush=True,
        )
        try:
            start = time.monotonic_ns()
            test_draken_roundtrip(arrow_array, op_arrow, op_draken)
            print(
                f"\033[38;2;26;185;67m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms\033[0m ✅",
                end="",
            )
            passed += 1
            if failed > 0:
                print(f" \033[0;31m{failed}\033[0m")
            else:
                print()
        except Exception as err:
            failed += 1
            print(f"\033[0;31m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms ❌ {failed}\033[0m")
            print(">", err)

    print("--- ✅ \033[0;32mdone\033[0m")

    print(
        f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m ({((time.monotonic_ns() - start_suite) / 1e9):.2f} seconds)\n"
        f"  \033[38;2;26;185;67m{passed} passed ({(passed * 100) // (passed + failed)}%)\033[0m\n"
        f"  \033[38;2;255;121;198m{failed} failed\033[0m"
    )
