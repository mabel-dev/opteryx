"""
Performance benchmarking for StringVector builder and iterators.

HOW TO USE THIS TEST:
---------------------

1. **Run the test normally:**
   python tests/performance/test_string_vector_perf.py
   
   This shows current performance with throughput metrics (rows/sec, MB/sec)

2. **Save a baseline before making changes:**
   SAVE_BASELINE=1 python tests/performance/test_string_vector_perf.py
   
   This saves current performance to .perf_baseline.json

3. **Make your changes** (edit code, recompile with `make compile`)

4. **Run again to compare:**
   python tests/performance/test_string_vector_perf.py
   
   You'll see:
   - "1.5x FASTER ✓" if your changes improved performance
   - "1.2x SLOWER ✗" if performance regressed
   - "~same" if no significant change

5. **Adjust dataset size** (if needed):
   DR_STRINGS_N=5000000 python tests/performance/test_string_vector_perf.py
   
   Default is 2M rows, increase for more stable measurements

WHAT THE NUMBERS MEAN:
----------------------
- **append() loop**: Building vectors by calling append() in a loop
  - FASTER is better
  - Typical: 20-30M rows/sec, 300-400 MB/sec
  
- **append_view() loop**: Building with memoryview (more overhead)
  - Should be slower than append()
  - Typical: 5-10M rows/sec, 80-120 MB/sec
  
- **Python iterator**: Iterating over vector items in Python
  - FASTER is better
  - Typical: 15-25M rows/sec, 200-400 MB/sec
  
- **Offsets-based**: Ultra-fast low-level access (baseline reference)
  - Always very fast (100x+ faster than Python iterator)
  - Typical: 1000M+ rows/sec

WHAT TO LOOK FOR:
-----------------
- If append() gets FASTER → your changes improved builder performance ✓
- If Python iterator gets FASTER → your changes improved iteration ✓
- If anything gets significantly SLOWER (>20%) → investigate why
- Small variations (±5%) are normal noise, run multiple times to confirm

"""

import os
import time
import random
import string
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from opteryx.draken.vectors import string_vector as _sv

# `string_vector` is a Cython module; bind the builder symbol through the module
StringVectorBuilder = _sv.StringVectorBuilder

# Baseline file to save/compare results
BASELINE_FILE = Path(__file__).parent / ".perf_baseline.json"


def _gen_values(n, avg_len=16, null_rate=0.05, seed=42):
    """Generate a list of bytes values with some nulls."""
    rnd = random.Random(seed)
    vals = []
    for _ in range(n):
        if rnd.random() < null_rate:
            vals.append(None)
            continue
        L = max(0, int(rnd.gauss(avg_len, avg_len * 0.25))) or 1
        s = ''.join(rnd.choices(string.ascii_letters + string.digits, k=L))
        vals.append(s.encode("utf8"))
    return vals


def _print(msg, *args):
    # Helper so pytest captures prints in a clear way
    print(msg.format(*args))


def string_vector_builder_and_iterators_perf():
    """Quick micro-benchmark for StringVector builder and iterators.

    This test is intended to be reasonably fast locally but still show
    relative timings. Use the environment variable `DR_STRINGS_N` to
    scale the number of rows (default 2_000_000).
    """
    N = int(os.environ.get("DR_STRINGS_N", "2000000"))
    AVG = int(os.environ.get("DR_STRINGS_AVG", "16"))

    values = _gen_values(N, AVG, null_rate=0.05)

    # --- Builder: append loop ---
    builder = StringVectorBuilder.with_estimate(N, AVG)
    t0 = time.perf_counter()
    for v in values:
        if v is None:
            builder.append_null()
        else:
            builder.append(v)
    vec = builder.finish()
    t1 = time.perf_counter()
    append_loop_time = t1 - t0
    _print("append loop: {0:.4f}s", append_loop_time)

    assert len(vec) == N

    # Sanity-check: sample a few values
    for i in (0, N // 3, N // 2, N - 1):
        a = values[i]
        b = vec[i]
        if a is None:
            assert b is None
        else:
            assert b == a

    # (Note) append_bulk not always exposed on all builds; skip bulk test here.

    # --- Builder: append_view (memoryview) ---
    builder3 = StringVectorBuilder.with_estimate(N, AVG)
    t0 = time.perf_counter()
    for v in values:
        if v is None:
            builder3.append_null()
        else:
            builder3.append_view(memoryview(v))
    vec3 = builder3.finish()
    t1 = time.perf_counter()
    append_view_time = t1 - t0
    _print("append_view loop: {0:.4f}s", append_view_time)

    # ensure vec3 was built correctly (used to avoid unused-variable lint)
    assert len(vec3) == N

    # --- Iterators: Python-level ---
    t0 = time.perf_counter()
    total_bytes = 0
    count_non_null = 0
    for item in vec:
        if item is None:
            continue
        total_bytes += len(item)
        count_non_null += 1
    t1 = time.perf_counter()
    py_iter_time = t1 - t0
    _print("python iterator: {0:.4f}s (non-null={1}, bytes={2})", py_iter_time, count_non_null, total_bytes)

    # --- Fast offsets-based measurement (safe, low-overhead) ---
    # Use the offsets buffer directly to compute total bytes and non-null count.
    t0 = time.perf_counter()
    offsets = vec.lengths()  # memory view over int32 offsets (n+1)
    total2 = int(offsets[len(vec)])
    count2 = len(vec) - vec.null_count
    t1 = time.perf_counter()
    c_iter_time = t1 - t0
    _print("offsets-based: {0:.6f}s (non-null={1}, bytes={2})", c_iter_time, count2, total2)

    # Cross-check
    assert total_bytes == total2
    assert count_non_null == count2

    # Calculate throughput metrics
    total_mb = total_bytes / (1024 * 1024)
    
    print("\n" + "="*70)
    print("PERFORMANCE RESULTS")
    print("="*70)
    print(f"Dataset: {N:,} rows, {total_mb:.2f} MB, avg {AVG} bytes/row")
    print()
    print("BUILDER OPERATIONS:")
    print(f"  append() loop:      {append_loop_time:.4f}s  ({N/append_loop_time:,.0f} rows/sec, {total_mb/append_loop_time:.1f} MB/sec)")
    print(f"  append_view() loop: {append_view_time:.4f}s  ({N/append_view_time:,.0f} rows/sec, {total_mb/append_view_time:.1f} MB/sec)")
    print()
    print("ITERATION OPERATIONS:")
    print(f"  Python iterator:    {py_iter_time:.4f}s  ({N/py_iter_time:,.0f} rows/sec, {total_mb/py_iter_time:.1f} MB/sec)")
    print(f"  Offsets-based:      {c_iter_time:.6f}s  ({N/c_iter_time:,.0f} rows/sec, {total_mb/c_iter_time:.1f} MB/sec)")
    print()
    print("INTERPRETATION:")
    print(f"  • append() is {append_view_time/append_loop_time:.1f}x faster than append_view()")
    print(f"  • offsets-based is {py_iter_time/c_iter_time:.0f}x faster than Python iterator")
    print(f"  • Python iterator processes ~{count_non_null/py_iter_time:,.0f} items/sec")
    
    # Save or compare with baseline
    results = {
        "N": N,
        "avg_len": AVG,
        "total_mb": total_mb,
        "append_time": append_loop_time,
        "append_view_time": append_view_time,
        "py_iter_time": py_iter_time,
        "offsets_time": c_iter_time,
        "append_throughput": N/append_loop_time,
        "py_iter_throughput": N/py_iter_time,
    }
    
    if os.environ.get("SAVE_BASELINE"):
        with open(BASELINE_FILE, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"\n✓ Baseline saved to {BASELINE_FILE}")
    elif BASELINE_FILE.exists():
        with open(BASELINE_FILE) as f:
            baseline = json.load(f)
        
        # Only compare if same parameters
        if baseline.get("N") == N and baseline.get("avg_len") == AVG:
            print("\nCOMPARISON TO BASELINE:")
            append_ratio = baseline['append_time'] / append_loop_time
            view_ratio = baseline['append_view_time'] / append_view_time
            iter_ratio = baseline['py_iter_time'] / py_iter_time
            
            def change_str(ratio):
                if ratio > 1.05:
                    return f"{ratio:.2f}x FASTER ✓"
                elif ratio < 0.95:
                    return f"{1/ratio:.2f}x SLOWER ✗"
                else:
                    return "~same"
            
            print(f"  append():        {change_str(append_ratio)}")
            print(f"  append_view():   {change_str(view_ratio)}")
            print(f"  Python iterator: {change_str(iter_ratio)}")
            print(f"\nTip: Run with SAVE_BASELINE=1 to update the baseline")
        else:
            print(f"\n(Baseline exists but uses different parameters: N={baseline.get('N')}, avg_len={baseline.get('avg_len')})")
    else:
        print(f"\nTip: Run with SAVE_BASELINE=1 to create a baseline for comparisons")
    
    print("="*70)


if __name__ == "__main__":
    string_vector_builder_and_iterators_perf()

"""
append loop: 0.0083s
append_view loop: 0.0390s
python iterator: 0.0099s (non-null=189967, bytes=2941116)
offsets-based: 0.000079s (non-null=189967, bytes=2941116)
summary: N=200000 avg_len=16 append=0.0083s view=0.0390s py_iter=0.0099s c_get_at=0.0001s


append loop: 0.0084s
append_view loop: 0.0325s
python iterator: 0.0093s (non-null=189967, bytes=2941116)
offsets-based: 0.000087s (non-null=189967, bytes=2941116)
summary: N=200000 avg_len=16 append=0.0084s view=0.0325s py_iter=0.0093s c_get_at=0.0001s
"""