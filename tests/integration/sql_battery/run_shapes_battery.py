#!/usr/bin/env python3
"""
Runner script for all shape battery tests.
Executes all test_shapes_*.py files in sequence.
"""
import subprocess
import sys
import time
from pathlib import Path

# Define test files in order
test_files = [
    'test_shapes_basic.py',
    'test_shapes_data_sources.py',
    'test_shapes_operators_expressions.py',
    'test_shapes_aliases_distinct.py',
    'test_shapes_functions_aggregates.py',
    'test_shapes_joins_subqueries.py',
    'test_shapes_edge_cases.py',
]

test_dir = Path(__file__).parent
total_start = time.time()
failed_files = []

print("=" * 70)
print("RUNNING SHAPES AND ERRORS BATTERY TEST SUITE")
print("=" * 70)

for test_file in test_files:
    test_path = test_dir / test_file
    if not test_path.exists():
        print(f"⚠️  Skipping {test_file} (not found)")
        continue
    
    print(f"\n{'▶ ' + test_file:─<70}")
    ret = subprocess.call([sys.executable, str(test_path)])
    
    if ret != 0:
        failed_files.append(test_file)

print("\n" + "=" * 70)
print(f"✅ COMPLETED IN {time.time() - total_start:.2f}s")

if failed_files:
    print(f"\n❌ FAILED FILES ({len(failed_files)}):")
    for f in failed_files:
        print(f"   - {f}")
    sys.exit(1)
else:
    print("✨ ALL TESTS PASSED")
    sys.exit(0)
