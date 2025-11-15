"""
Simple example demonstrating align_tables usage for join operations.
"""
import numpy as np
import pyarrow as pa

from opteryx.draken import Morsel
from opteryx.draken.morsels import align_tables_pyarray


def example_1_basic_alignment():
    """Basic example: align two tables by indices."""
    print("Example 1: Basic Alignment")
    print("-" * 50)
    
    # Create left table
    left = pa.table({
        "user_id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
        "name": pa.array(["Alice", "Bob", "Charlie", "David", "Eve"]),
        "age": pa.array([25, 30, 35, 40, 45], type=pa.int64()),
    })
    
    # Create right table
    right = pa.table({
        "order_id": pa.array([101, 102, 103, 104, 105], type=pa.int64()),
        "amount": pa.array([100.0, 200.0, 150.0, 300.0, 250.0]),
    })
    
    # Convert to morsels
    left_morsel = Morsel.from_arrow(left)
    right_morsel = Morsel.from_arrow(right)
    
    # Align: take rows [0, 2, 4] from left and [1, 3, 4] from right
    left_indices = np.array([0, 2, 4], dtype=np.int32)
    right_indices = np.array([1, 3, 4], dtype=np.int32)
    
    result = align_tables_pyarray(left_morsel, right_morsel, left_indices, right_indices)
    
    print(f"Input shapes: {left_morsel.shape} + {right_morsel.shape}")
    print(f"Output shape: {result.shape}")
    print(f"Columns: {result.column_names}\n")
    
    result_arrow = result.to_arrow()
    print(result_arrow)
    print()


def example_2_inner_join():
    """Example 2: Simulate an inner join."""
    print("Example 2: Inner Join Simulation")
    print("-" * 50)
    
    # Users table
    users = pa.table({
        "user_id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
        "name": pa.array(["Alice", "Bob", "Charlie", "David", "Eve"]),
    })
    
    # Orders table
    orders = pa.table({
        "order_id": pa.array([101, 102, 103, 104], type=pa.int64()),
        "user_id": pa.array([2, 1, 4, 2], type=pa.int64()),  # Foreign key
        "amount": pa.array([100.0, 200.0, 150.0, 300.0]),
    })
    
    users_morsel = Morsel.from_arrow(users)
    orders_morsel = Morsel.from_arrow(orders)
    
    # Simulate hash join: match user_id
    # orders.user_id = [2, 1, 4, 2]
    # Match to users indices: [1, 0, 3, 1]
    user_indices = np.array([1, 0, 3, 1], dtype=np.int32)  # Bob, Alice, David, Bob
    order_indices = np.array([0, 1, 2, 3], dtype=np.int32)  # All orders
    
    result = align_tables_pyarray(users_morsel, orders_morsel, user_indices, order_indices)
    
    print("Joined users with their orders:")
    print(result.to_arrow())
    print()


def example_3_duplicate_columns():
    """Example 3: Handle duplicate column names."""
    print("Example 3: Duplicate Column Handling")
    print("-" * 50)
    
    left = pa.table({
        "id": pa.array([1, 2, 3], type=pa.int64()),
        "value": pa.array([10, 20, 30], type=pa.int64()),
    })
    
    right = pa.table({
        "id": pa.array([4, 5, 6], type=pa.int64()),  # Same column name!
        "score": pa.array([100, 200, 300], type=pa.int64()),
    })
    
    left_morsel = Morsel.from_arrow(left)
    right_morsel = Morsel.from_arrow(right)
    
    indices = np.array([0, 1, 2], dtype=np.int32)
    
    result = align_tables_pyarray(left_morsel, right_morsel, indices, indices)
    
    print("Note: 'id' from right table is ignored (left takes precedence)")
    print(f"Result columns: {result.column_names}")
    print(result.to_arrow())
    print()


def example_4_large_scale():
    """Example 4: Large-scale performance."""
    print("Example 4: Large-Scale Performance")
    print("-" * 50)
    
    import time
    
    n_rows = 1_000_000
    
    # Large left table
    left = pa.table({
        "id": pa.array(np.arange(n_rows, dtype=np.int64)),
        "val1": pa.array(np.random.rand(n_rows)),
        "val2": pa.array(np.random.rand(n_rows)),
        "val3": pa.array(np.random.rand(n_rows)),
    })
    
    # Large right table
    right = pa.table({
        "metric1": pa.array(np.random.rand(n_rows)),
        "metric2": pa.array(np.random.rand(n_rows)),
    })
    
    left_morsel = Morsel.from_arrow(left)
    right_morsel = Morsel.from_arrow(right)
    
    # Select 50% of rows
    sample_size = n_rows // 2
    indices = np.random.choice(n_rows, size=sample_size, replace=True).astype(np.int32)
    
    print(f"Left table: {left_morsel.shape}")
    print(f"Right table: {right_morsel.shape}")
    print(f"Sample size: {sample_size:,} rows")
    
    # Warmup
    _ = align_tables_pyarray(left_morsel, right_morsel, indices, indices)
    
    # Benchmark
    num_runs = 5
    times = []
    
    for _ in range(num_runs):
        start = time.perf_counter()
        result = align_tables_pyarray(left_morsel, right_morsel, indices, indices)
        elapsed = time.perf_counter() - start
        times.append(elapsed)
    
    avg_time = np.mean(times) * 1000
    throughput = sample_size / (avg_time / 1000) / 1_000_000
    
    print("\nPerformance:")
    print(f"  Average time: {avg_time:.1f} ms")
    print(f"  Throughput:   {throughput:.1f} M rows/sec")
    print(f"  Result shape: {result.shape}")
    print()


def example_5_mixed_types():
    """Example 5: Mixed data types."""
    print("Example 5: Mixed Data Types")
    print("-" * 50)
    
    left = pa.table({
        "int_col": pa.array([1, 2, 3], type=pa.int64()),
        "float_col": pa.array([1.1, 2.2, 3.3]),
        "string_col": pa.array(["a", "b", "c"]),
        "bool_col": pa.array([True, False, True]),
    })
    
    right = pa.table({
        "date_col": pa.array([
            pa.scalar(1, type=pa.date32()),
            pa.scalar(2, type=pa.date32()),
            pa.scalar(3, type=pa.date32()),
        ]),
        "timestamp_col": pa.array([
            pa.scalar(1000, type=pa.timestamp('us')),
            pa.scalar(2000, type=pa.timestamp('us')),
            pa.scalar(3000, type=pa.timestamp('us')),
        ]),
    })
    
    left_morsel = Morsel.from_arrow(left)
    right_morsel = Morsel.from_arrow(right)
    
    indices = np.array([0, 2], dtype=np.int32)
    
    result = align_tables_pyarray(left_morsel, right_morsel, indices, indices)
    
    print(f"Handling {result.num_columns} columns of different types:")
    for name, dtype in zip(result.column_names, result.column_types):
        print(f"  {name.decode('utf-8')}: {dtype}")
    
    print(f"\nResult:\n{result.to_arrow()}")
    print()


if __name__ == "__main__":
    print("=" * 50)
    print("align_tables Usage Examples")
    print("=" * 50)
    print()
    
    example_1_basic_alignment()
    example_2_inner_join()
    example_3_duplicate_columns()
    example_4_large_scale()
    example_5_mixed_types()
    
    print("=" * 50)
    print("All examples completed!")
    print("=" * 50)
