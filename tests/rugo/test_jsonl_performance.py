"""
Performance comparison between rugo JSON lines reader and PyArrow.
"""
import time
import json
import tempfile
import os

try:
    import pyarrow.json as paj
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

import opteryx.rugo.jsonl as rj


def generate_test_data(num_rows=10000):
    """Generate test JSON lines data."""
    data = []
    for i in range(num_rows):
        row = {
            'id': i,
            'name': f'person_{i}',
            'age': 20 + (i % 50),
            'salary': 30000.0 + (i % 100) * 1000.0,
            'active': i % 2 == 0,
        }
        data.append(json.dumps(row))
    return '\n'.join(data).encode('utf-8')


def test_rugo_performance():
    """Test rugo JSON lines reader performance."""
    print("\n=== Rugo JSON Lines Reader Performance ===\n")
    
    for num_rows in [1000, 10000, 100000]:
        print(f"Testing with {num_rows:,} rows...")
        data = generate_test_data(num_rows)
        
        # Test full read
        start = time.time()
        result = rj.read_jsonl(data)
        elapsed = time.time() - start
        print(f"  Full read: {elapsed:.4f}s ({num_rows/elapsed:.0f} rows/sec)")
        assert result['success']
        assert result['num_rows'] == num_rows
        
        # Test projection (read only 2 columns)
        start = time.time()
        result = rj.read_jsonl(data, columns=['id', 'salary'])
        elapsed = time.time() - start
        print(f"  Projection (2 cols): {elapsed:.4f}s ({num_rows/elapsed:.0f} rows/sec)")
        assert result['success']
        assert result['num_rows'] == num_rows
        assert len(result['columns']) == 2
        
        # Test schema extraction
        start = time.time()
        schema = rj.get_jsonl_schema(data)
        elapsed = time.time() - start
        print(f"  Schema extraction: {elapsed:.4f}s")
        assert len(schema) == 5


def test_pyarrow_performance():
    """Test PyArrow JSON reader performance."""
    if not HAS_PYARROW:
        print("\nPyArrow not available, skipping comparison")
        return
    
    print("\n=== PyArrow JSON Reader Performance ===\n")
    
    for num_rows in [1000, 10000, 100000]:
        print(f"Testing with {num_rows:,} rows...")
        data = generate_test_data(num_rows)
        
        # PyArrow requires a file
        with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.jsonl') as f:
            f.write(data)
            temp_file = f.name
        
        try:
            # Test full read
            start = time.time()
            table = paj.read_json(temp_file)
            elapsed = time.time() - start
            print(f"  Full read: {elapsed:.4f}s ({num_rows/elapsed:.0f} rows/sec)")
            assert len(table) == num_rows
            
            # Test projection (read only 2 columns) - done after reading
            start = time.time()
            table = paj.read_json(temp_file)
            selected = table.select(['id', 'salary'])
            elapsed = time.time() - start
            print(f"  Read + Select (2 cols): {elapsed:.4f}s ({num_rows/elapsed:.0f} rows/sec)")
            assert len(selected) == num_rows
            
        finally:
            os.unlink(temp_file)


def test_comparison():
    """Direct comparison between rugo and PyArrow."""
    if not HAS_PYARROW:
        print("\nPyArrow not available, skipping comparison")
        return
    
    print("\n=== Direct Comparison (100K rows) ===\n")
    
    num_rows = 100000
    data = generate_test_data(num_rows)
    
    # Rugo performance
    print("Rugo (projection pushdown):")
    start = time.time()
    rj.read_jsonl(data, columns=['id', 'name', 'salary'])
    rugo_time = time.time() - start
    print(f"  Time: {rugo_time:.4f}s ({num_rows/rugo_time:.0f} rows/sec)")
    
    # PyArrow performance
    with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.jsonl') as f:
        f.write(data)
        temp_file = f.name
    
    try:
        print("PyArrow (read all + select):")
        start = time.time()
        table = paj.read_json(temp_file)
        table.select(['id', 'name', 'salary'])
        pyarrow_time = time.time() - start
        print(f"  Time: {pyarrow_time:.4f}s ({num_rows/pyarrow_time:.0f} rows/sec)")
        
        if rugo_time < pyarrow_time:
            speedup = pyarrow_time / rugo_time
            print(f"\n✓ Rugo is {speedup:.2f}x faster than PyArrow!")
        else:
            slowdown = rugo_time / pyarrow_time
            print(f"\nPyArrow is {slowdown:.2f}x faster than Rugo")
            
    finally:
        os.unlink(temp_file)


if __name__ == "__main__":
    test_rugo_performance()
    test_pyarrow_performance()
    test_comparison()
