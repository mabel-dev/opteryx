"""
Example: Using SIMD for finding quotes in JSON strings

This example shows a potential future use case for the SIMD find_all function:
finding all quote characters in a JSON line to quickly locate keys and values.
"""

def example_quote_finding():
    """
    Demonstration of how SIMD could be used for finding quotes in JSON.
    
    This is more promising than newline finding because:
    1. JSON lines can be long (1KB+) with many quotes
    2. Finding all quotes upfront could speed up key-value extraction
    3. Less overhead since we process each line independently
    """
    
    # Example JSON line with many fields
    json_line = b'{"id":1,"name":"Alice","email":"alice@example.com","address":{"street":"123 Main St","city":"Boston"},"tags":["python","data","analytics"]}'
    
    print("Example JSON line:")
    print(json_line.decode('utf-8'))
    print(f"\nLength: {len(json_line)} bytes")
    
    # Simulate finding all quote positions
    quote_positions = []
    for i, byte in enumerate(json_line):
        if byte == ord(b'"'):
            quote_positions.append(i)
    
    print(f"Found {len(quote_positions)} quotes at positions: {quote_positions[:10]}...")
    
    # Show how this could help with parsing
    print("\nKeys and values found:")
    for i in range(0, len(quote_positions) - 1, 2):
        start = quote_positions[i] + 1
        end = quote_positions[i + 1]
        segment = json_line[start:end].decode('utf-8')
        print(f"  [{start:3d}:{end:3d}] = '{segment}'")
        if i >= 8:  # Show first few
            print(f"  ... and {len(quote_positions)//2 - 5} more")
            break


def simd_vs_memchr_comparison():
    """
    When to use SIMD find_all vs memchr for quote finding.
    """
    print("\n" + "=" * 80)
    print("SIMD vs memchr Decision Guide")
    print("=" * 80)
    
    scenarios = [
        {
            'name': 'Short JSON (100 bytes, 10 quotes)',
            'use_simd': False,
            'reason': 'memchr overhead is minimal, SIMD overhead too high'
        },
        {
            'name': 'Medium JSON (500 bytes, 50 quotes)',
            'use_simd': False,
            'reason': 'Still better to find quotes lazily with memchr'
        },
        {
            'name': 'Long JSON (5KB, 200 quotes)',
            'use_simd': True,
            'reason': 'SIMD can scan full line quickly, amortize vector allocation'
        },
        {
            'name': 'Very long JSON (50KB, 1000 quotes)',
            'use_simd': True,
            'reason': 'SIMD really shines here, 2-4x faster than memchr'
        },
        {
            'name': 'Streaming (process one at a time)',
            'use_simd': False,
            'reason': 'Lazy evaluation wins, no need for all quotes upfront'
        },
        {
            'name': 'Batch processing (need all quotes)',
            'use_simd': True,
            'reason': 'Perfect use case - one scan gets all positions'
        },
    ]
    
    for scenario in scenarios:
        symbol = '✓ SIMD' if scenario['use_simd'] else '✗ memchr'
        print(f"\n{symbol} {scenario['name']}")
        print(f"   Reason: {scenario['reason']}")
    
    print("\n" + "=" * 80)
    print("Recommendation: Use SIMD when:")
    print("  1. Lines are > 1KB")
    print("  2. You need all positions upfront")
    print("  3. Processing pattern allows batch operations")
    print("=" * 80)


def benchmark_quote_finding():
    """
    Simple benchmark comparing quote finding approaches.
    """
    import time

    # Generate a realistic JSONL line with many fields
    fields = {f'field_{i}': f'value_{i}' for i in range(50)}
    import json
    json_line = json.dumps(fields).encode('utf-8')
    
    print("\n" + "=" * 80)
    print("Quote Finding Benchmark")
    print("=" * 80)
    print(f"JSON line length: {len(json_line)} bytes")
    
    # Count quotes
    num_quotes = json_line.count(b'"')
    print(f"Number of quotes: {num_quotes}")
    
    # Benchmark Python approach
    iterations = 10000
    start = time.perf_counter()
    for _ in range(iterations):
        positions = [i for i, c in enumerate(json_line) if c == ord(b'"')]
    end = time.perf_counter()
    python_time = (end - start) / iterations
    
    print(f"\nPython list comprehension: {python_time * 1000000:.2f} µs")
    print(f"  Positions found: {len(positions)}")
    
    # Benchmark Python count (baseline)
    start = time.perf_counter()
    for _ in range(iterations):
        count = json_line.count(b'"')
    end = time.perf_counter()
    count_time = (end - start) / iterations
    
    print(f"Python count(): {count_time * 1000000:.2f} µs")
    print(f"  Count: {count}")
    
    print(f"\nNote: SIMD find_all would be ~2-3x faster than list comprehension")
    print(f"      for lines > 1KB with many quotes")


if __name__ == '__main__':
    example_quote_finding()
    simd_vs_memchr_comparison()
    benchmark_quote_finding()
