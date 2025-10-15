# SIMD Quick Reference

## Current Status
✓ SIMD functions implemented and available  
✓ Using **memchr** for JSONL newline detection (optimal)  
✓ SIMD ready for future use cases

## Available Functions

```cpp
// Find first occurrence (returns -1 if not found)
int neon_search(const char* data, size_t length, char target);
int avx_search(const char* data, size_t length, char target);

// Find all occurrences (returns vector of offsets)
std::vector<size_t> neon_find_all(const char* data, size_t length, char target = '\n');
std::vector<size_t> avx_find_all(const char* data, size_t length, char target = '\n');
```

## When to Use

| Scenario | Use | Reason |
|----------|-----|--------|
| JSONL newlines (100-500 byte lines) | **memchr** | 3.4x faster, lazy evaluation |
| Finding quotes in long JSON (> 1KB) | **SIMD** | Batch processing wins |
| Sequential processing | **memchr** | Better cache locality |
| Need all positions upfront | **SIMD** | One scan gets everything |
| Sparse patterns | **SIMD** | Amortize scan cost |
| Dense patterns | **memchr** | Lower overhead |

## Performance Numbers (M1 Mac)

| Pattern | memchr | SIMD | Winner |
|---------|--------|------|--------|
| Newlines (100B lines) | 5,357 MB/s | 1,597 MB/s | memchr (3.4x) |
| Quotes (1KB+ JSON) | ~1,500 MB/s | ~3,000 MB/s | SIMD (2x)* |

*Estimated based on pattern density

## Import in Cython

```cython
from libcpp.vector cimport vector
cdef extern from "simd_search.h":
    int neon_search(const char *data, size_t length, char target)
    vector[size_t] neon_find_all(const char *data, size_t length, char target)
```

## Example Usage

```cython
# Current: Using memchr for newlines (optimal)
newline_pos = <const char*>memchr(line_start, b'\n', remaining)

# Future: Using SIMD for quotes in long JSON
cdef vector[size_t] quote_positions = neon_find_all(json_line, line_len, b'"')
for i in range(quote_positions.size()):
    process_quote_at(quote_positions[i])
```

## Documentation
- `SIMD_USAGE_GUIDE.md` - Complete usage guide
- `SIMD_INTEGRATION_SUMMARY.md` - Implementation summary
- `SIMD_JSONL_ANALYSIS.md` - Performance analysis
- `examples/simd_quote_finding.py` - Example use cases

## Build
```bash
python setup.py build_ext --inplace
```

## Test
```bash
python bench_jsonl.py              # Full benchmark
python bench_newline_finding.py    # Newline-specific
./scratch/test_simd_find_all       # C++ test
```

---
**TL;DR**: SIMD works great, but memchr is faster for JSONL newlines. Use SIMD for long strings with sparse patterns.
