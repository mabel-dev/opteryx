"""
Performance test for REGEXP_REPLACE optimization.

This test benchmarks the optimized Cython implementation of REGEXP_REPLACE
against the original PyArrow implementation, particularly focusing on the
Clickbench #29 query pattern.
"""

import os
import sys
import time
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx


def test_regexp_replace_clickbench_29():
    """
    Test REGEXP_REPLACE with the pattern from Clickbench query #29.
    This query was identified as extremely slow in the original implementation.
    """
    # This is a simplified version of the Clickbench #29 query
    # The full query requires the clickbench dataset
    query = """
    SELECT 
        REGEXP_REPLACE(url, b'^https?://(?:www\.)?([^/]+)/.*$', r'\\1') AS domain,
        COUNT(*) as cnt
    FROM (
        SELECT 'https://www.example.com/path/to/page' as url
        UNION ALL SELECT 'http://test.org/another/path'
        UNION ALL SELECT 'https://subdomain.example.net/xyz'
        UNION ALL SELECT 'https://www.google.com/search?q=test'
        UNION ALL SELECT ''
    )
    WHERE url <> ''
    GROUP BY REGEXP_REPLACE(url, b'^https?://(?:www\.)?([^/]+)/.*$', r'\\1')
    ORDER BY cnt DESC
    """
    
    start = time.time()
    result = opteryx.query(query)
    elapsed = time.time() - start
    
    # Verify results
    rows = list(result)
    assert len(rows) == 4, f"Expected 4 rows, got {len(rows)}"
    
    # Verify domains were extracted correctly
    domains = [row[0] for row in rows]
    assert b'example.com' in domains or 'example.com' in domains
    assert b'test.org' in domains or 'test.org' in domains
    
    print(f"Query completed in {elapsed*1000:.2f}ms")
    print(f"Results: {rows}")
    
    # Performance expectation: should complete in reasonable time
    # With optimization, this should be much faster than original
    assert elapsed < 5.0, f"Query took too long: {elapsed:.2f}s"


def test_regexp_replace_with_backreferences():
    """
    Test REGEXP_REPLACE with various backreference patterns.
    """
    query = """
    SELECT 
        REGEXP_REPLACE(text, '([a-z]+)-([0-9]+)', r'\\2_\\1') AS result
    FROM (
        SELECT 'test-123' as text
        UNION ALL SELECT 'hello-456'
        UNION ALL SELECT 'world-789'
    )
    """
    
    result = opteryx.query(query)
    rows = list(result)
    
    expected_results = ['123_test', '456_hello', '789_world']
    actual_results = [row[0] for row in rows]
    
    assert set(actual_results) == set(expected_results), \
        f"Expected {expected_results}, got {actual_results}"


def test_regexp_replace_with_empty_strings():
    """
    Test REGEXP_REPLACE handles empty strings and no matches correctly.
    """
    query = """
    SELECT 
        REGEXP_REPLACE(text, 'xyz', 'ABC') AS result
    FROM (
        SELECT 'no match here' as text
        UNION ALL SELECT ''
        UNION ALL SELECT 'xyz in middle xyz'
    )
    """
    
    result = opteryx.query(query)
    rows = list(result)
    
    assert len(rows) == 3
    
    # Find the row with replacement
    results = [row[0] for row in rows]
    assert 'ABC in middle ABC' in results or b'ABC in middle ABC' in results


@pytest.mark.benchmark
def benchmark_regexp_replace_large_dataset():
    """
    Benchmark REGEXP_REPLACE on a larger synthetic dataset.
    This helps measure the performance improvement of the optimized implementation.
    """
    # Create a larger test dataset
    urls = [
        "'https://www.site{}.com/path/to/page{}'".format(i % 100, i)
        for i in range(1000)
    ]
    
    query = f"""
    SELECT 
        REGEXP_REPLACE(url, b'^https?://(?:www\.)?([^/]+)/.*$', r'\\1') AS domain,
        COUNT(*) as cnt
    FROM (
        SELECT {urls[0]} as url
        {' UNION ALL SELECT '.join(urls[1:])}
    )
    GROUP BY domain
    ORDER BY cnt DESC
    LIMIT 10
    """
    
    iterations = 3
    times = []
    
    for i in range(iterations):
        start = time.time()
        result = opteryx.query(query)
        _ = list(result)  # Force evaluation
        elapsed = time.time() - start
        times.append(elapsed)
        print(f"Iteration {i+1}: {elapsed*1000:.2f}ms")
    
    avg_time = sum(times) / len(times)
    print(f"Average time: {avg_time*1000:.2f}ms")
    print(f"Min time: {min(times)*1000:.2f}ms")
    print(f"Max time: {max(times)*1000:.2f}ms")
    
    # Performance expectation: should be reasonably fast
    assert avg_time < 10.0, f"Average query time too slow: {avg_time:.2f}s"


if __name__ == "__main__":
    print("=" * 80)
    print("REGEXP_REPLACE Performance Tests")
    print("=" * 80)
    
    print("\nTest 1: Clickbench #29 pattern")
    test_regexp_replace_clickbench_29()
    
    print("\nTest 2: Backreferences")
    test_regexp_replace_with_backreferences()
    
    print("\nTest 3: Empty strings and no matches")
    test_regexp_replace_with_empty_strings()
    
    print("\nTest 4: Large dataset benchmark")
    benchmark_regexp_replace_large_dataset()
    
    print("\n" + "=" * 80)
    print("All tests passed!")
    print("=" * 80)
