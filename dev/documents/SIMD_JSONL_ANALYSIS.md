# SIMD JSONL Decoder Analysis

## Summary

Integrated SIMD `find_all` function into the JSONL decoder to pre-compute all newline positions. **Result: Performance regression of ~3x for newline finding**.

## Benchmark Results

### Full JSONL Decoding Performance

| Test Case | memchr | SIMD | Speedup |
|-----------|--------|------|---------|
| Small (1K lines, 10 cols) | 6.01 ms | 5.93 ms | **1.01x** ✓ |
| Medium (10K lines, 10 cols) | 61.22 ms | 60.68 ms | **1.01x** ✓ |
| Large (50K lines, 10 cols) | 313.75 ms | 340.58 ms | **0.92x** ✗ |
| Wide (10K lines, 50 cols) | 75.58 ms | 64.44 ms | **1.17x** ✓✓ |

### Isolated Newline Finding Performance

| Data Size | memchr | SIMD | Speedup |
|-----------|--------|------|---------|
| 1MB (10K lines) | 0.170 ms | 0.607 ms | **0.28x** ✗✗ |
| 10MB (100K lines) | 1.798 ms | 6.031 ms | **0.30x** ✗✗ |
| 50MB (500K lines) | 9.266 ms | 30.676 ms | **0.30x** ✗✗ |

**Baseline: Python `count()` = ~2.3 GB/s (much faster than both!)**

## Analysis

### Why SIMD is Slower

1. **Memory Allocation Overhead**: Creating a `std::vector` and allocating space for ~10K-500K offsets adds significant overhead
2. **Two-Pass Processing**: 
   - First pass: Scan entire buffer to find all newlines
   - Second pass: Process each line
   - vs memchr: One pass with lazy evaluation (find newline → process → next)
3. **Cache Efficiency**: memchr approach processes data sequentially with better cache locality
4. **Line Density**: With short lines (100 bytes), we have many newlines - SIMD overhead dominates

### Where SIMD Shows Promise

- **Wide datasets (many columns)**: 17% faster - the overhead is amortized over more column extraction
- **Small improvements**: 1% faster on small/medium datasets
- Best case is only 17% improvement, which is marginal

### Why memchr is So Fast

1. **Optimized C library function**: `memchr` is highly optimized, likely using SIMD internally
2. **Lazy evaluation**: Only finds next newline, doesn't scan entire buffer
3. **No memory allocation**: Works directly with pointers
4. **Better cache behavior**: Sequential access pattern

## Recommendations

### Option 1: Keep memchr (RECOMMENDED)
**Recommendation: Stay with current memchr implementation**

Reasons:
- 3x faster for newline finding
- Simpler code
- No memory allocation overhead
- Already optimized by C library
- Works well across all scenarios

### Option 2: Hybrid Approach
Use SIMD only for specific cases:
- Very long lines (> 1KB per line)
- Wide datasets (> 20 columns)
- Could add heuristic to choose implementation

**Not worth the complexity** for 17% max improvement

### Option 3: Different SIMD Strategy
Instead of pre-finding all newlines, use SIMD for:
- Finding keys within JSON objects
- Scanning for quote characters
- Pattern matching in values

**Might be more promising** but requires different implementation

## What We Learned

1. **SIMD isn't always faster**: Overhead can outweigh benefits
2. **Lazy evaluation wins**: For sparse patterns (newlines), finding one-at-a-time is faster
3. **memchr is highly optimized**: Hard to beat with custom SIMD for simple searches
4. **Test before optimizing**: Our SIMD implementation works correctly but isn't faster

## Files Created

- `src/cpp/simd_search.h` - SIMD search header
- `src/cpp/simd_search.cpp` - NEON/AVX2 implementations
- `opteryx/compiled/structures/jsonl_decoder.pyx` - Added `fast_jsonl_decode_columnar_simd`
- `bench_jsonl.py` - Comprehensive benchmark script
- `bench_newline_finding.py` - Detailed newline finding benchmark

## Conclusion

The SIMD `find_all` implementation is **correctly implemented and working**, but not beneficial for this use case. The current memchr-based approach is superior for JSONL parsing with typical line lengths.

**Final recommendation: Revert to memchr approach, keep SIMD code for potential future use with different patterns.**
