# SIMD Search Functions - Usage Guide

## Overview

The `simd_search` module provides SIMD-optimized character search functions for both ARM (NEON) and x86 (AVX2) architectures.

## Available Functions

### 1. Single Character Search (First Match)

```cpp
int neon_search(const char* data, size_t length, char target);
int avx_search(const char* data, size_t length, char target);
```

Returns the index of the **first** occurrence of `target` in `data`, or -1 if not found.

**Use when:** You only need to find the first occurrence and stop.

### 2. Find All Occurrences

```cpp
std::vector<size_t> neon_find_all(const char* data, size_t length, char target = '\n');
std::vector<size_t> avx_find_all(const char* data, size_t length, char target = '\n');
```

Returns a vector containing **all** offsets where `target` appears in `data`.

**Use when:** You need all positions upfront for batch processing.

## Performance Characteristics

### When SIMD is Faster

✅ **Long buffers** (> 1MB) with sparse pattern (few matches)
✅ **Need all matches** and will process them non-sequentially  
✅ **Fixed-size records** where you can pre-allocate result vector
✅ **Pattern matching** in wide data structures

### When memchr is Faster (Standard C Library)

✅ **Lazy evaluation** - find one, process, find next (JSONL newlines)
✅ **Short lines** with frequent matches (< 1KB lines)
✅ **Sequential processing** where cache locality matters
✅ **Memory-constrained** scenarios (no vector allocation)

## Benchmark Results

### Newline Finding in JSONL (100 byte lines)

| Implementation | Throughput | Notes |
|----------------|------------|-------|
| `memchr` | 5,357 MB/s | ✅ Winner - lazy evaluation |
| `find_all` (SIMD) | 1,597 MB/s | ❌ Overhead of vector allocation |
| Python `count()` | 2,326 MB/s | Surprisingly fast! |

### Takeaway
For JSONL parsing with typical line lengths (100-500 bytes), **use memchr for newlines**.

## Integration Examples

### Cython Integration

```cython
# Import the functions
from libcpp.vector cimport vector
cdef extern from "simd_search.h":
    int neon_search(const char *data, size_t length, char target)
    vector[size_t] neon_find_all(const char *data, size_t length, char target)

# Use in your code
cdef const char* data = PyBytes_AS_STRING(buffer)
cdef size_t length = PyBytes_GET_SIZE(buffer)

# Find first newline
cdef int first_newline = neon_search(data, length, b'\n')

# Find all newlines
cdef vector[size_t] all_newlines = neon_find_all(data, length, b'\n')
```

### C++ Direct Usage

```cpp
#include "simd_search.h"
#include <vector>

const char* data = "line1\nline2\nline3\n";
size_t length = strlen(data);

// Find first newline
int pos = neon_search(data, length, '\n');

// Find all newlines
std::vector<size_t> positions = neon_find_all(data, length, '\n');
```

## When to Use Each Function

### Use `neon_search` / `avx_search`
- Finding delimiters for simple split operations
- Locating start of a pattern
- Early termination scenarios
- Replacing `strchr` / `memchr` for better SIMD performance on long buffers

### Use `neon_find_all` / `avx_find_all`
- Parsing columnar data where you know all delimiters upfront
- Building index structures
- Bulk validation of structured data
- Cases where you'll iterate over all matches multiple times

### Use `memchr` (Standard Library)
- **JSONL newline detection** ← Current recommendation
- Sequential line-by-line processing
- Short lines with many matches
- Simple, maintainable code is priority

## Future Optimization Opportunities

Better SIMD use cases in JSONL decoder:

1. **Quote Finding**: Find all `"` characters in a JSON line to locate keys/values
2. **Escape Detection**: Find all `\` characters to handle escape sequences
3. **Delimiter Scanning**: Find commas in arrays/objects for structure parsing
4. **Multi-character patterns**: Using SIMD for pattern matching beyond single chars

## Compilation

The module is already integrated in `setup.py`:

```python
Extension(
    name="opteryx.compiled.structures.jsonl_decoder",
    sources=["opteryx/compiled/structures/jsonl_decoder.pyx", "src/cpp/simd_search.cpp"],
    language="c++",
    extra_compile_args=CPP_COMPILE_FLAGS,
)
```

Build with: `python setup.py build_ext --inplace`

## Testing

Run benchmarks:
```bash
python bench_jsonl.py              # Full JSONL decoder comparison
python bench_newline_finding.py    # Isolated newline finding
```

Test SIMD directly:
```bash
clang++ -std=c++11 -O2 -march=native scratch/test_simd_find_all.cpp src/cpp/simd_search.cpp -o scratch/test_simd_find_all
./scratch/test_simd_find_all
```

## Conclusion

The SIMD functions are **correctly implemented** and **working**. For JSONL newline detection specifically, `memchr` remains the better choice due to its lazy evaluation and lower overhead. The SIMD functions are available in the codebase for use cases where they provide better performance.
