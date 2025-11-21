# SIMD Optimization Opportunities in Opteryx

This document outlines the SIMD (Single Instruction, Multiple Data) optimizations implemented in Opteryx and opportunities for future improvements.

## Current SIMD Support

Opteryx currently uses SIMD instructions across several critical performance paths:

### 1. String/Character Search Operations (`src/cpp/simd_search.cpp`)

**Supported Instruction Sets:**
- **NEON** (ARM): 128-bit SIMD, processes 16 bytes per iteration
- **AVX2** (x86-64): 256-bit SIMD, processes 32 bytes per iteration  
- **AVX512** (x86-64): 512-bit SIMD, processes 64 bytes per iteration ✨ NEW

**Operations:**
- `avx_search()` / `neon_search()` - Find first occurrence of a character
- `avx_find_all()` / `neon_find_all()` - Find all occurrences of a character
- `avx_count()` / `neon_count()` - Count occurrences of a character
- `avx_find_delimiter()` / `neon_find_delimiter()` - Find JSON delimiters

**Performance Impact:**
- AVX512: **2x throughput** vs AVX2 (64-byte vs 32-byte chunks)
- AVX2: **2x throughput** vs NEON (32-byte vs 16-byte chunks)
- Critical for CSV/JSON parsing, string operations

### 2. Hash Mixing Operations (`src/cpp/simd_hash.cpp`)

**Supported Instruction Sets:**
- **NEON** (ARM): 2x uint64 parallel lanes
- **AVX2** (x86-64): 4x uint64 parallel lanes
- **AVX512** (x86-64): 8x uint64 parallel lanes ✨ NEW

**Operations:**
- `simd_mix_hash()` - Parallel hash value mixing for hash tables

**Performance Impact:**
- AVX512: **2x throughput** vs AVX2 (8 vs 4 parallel lanes)
- Native 64-bit multiply instruction (`vpmuludq`) on AVX512 (vs emulated on AVX2)
- Critical for JOIN operations, GROUP BY, DISTINCT

### 3. Base64 Encoding/Decoding (`third_party/alantsd/base64_*.c`)

**Supported Instruction Sets:**
- **NEON** (ARM): 128-bit SIMD
- **AVX2** (x86-64): 256-bit SIMD
- **AVX512** (x86-64): 512-bit SIMD ✨ NEW (currently delegates to scalar - placeholder for future optimization)

**Operations:**
- Base64 encode/decode with runtime CPU dispatch

**Performance Impact:**
- Important for binary data handling, API responses
- AVX512 implementation is currently a placeholder that delegates to scalar processing
- Future work: Implement full vectorized AVX512 base64 encode/decode for 2-3x speedup

### 4. JSON Parsing (`third_party/tktech/simdjson`)

**Supported Instruction Sets:**
- **NEON** (ARM)
- **Haswell/AVX2** (x86-64)
- **Icelake/AVX512** (x86-64) ✅ Already supported

**Performance Impact:**
- Critical for JSON data ingestion
- simdjson library already includes AVX512 support

### 5. Hashing (`third_party/cyan4973/xxhash`)

**Supported Instruction Sets:**
- **SSE4.1** (x86-64)
- **AVX2** (x86-64)
- **AVX512** (x86-64) ✅ Already supported

**Performance Impact:**
- Used for data deduplication, checksums
- xxHash library already includes AVX512 support

## Runtime CPU Feature Detection

Opteryx uses runtime CPU feature detection to automatically select the best SIMD implementation:

```c
// For base64 operations
b64_cpu_features features = b64_detect_cpu_features();
if (features.avx512 && len >= 64) {
    return b64tobin_avx512(dest, src, len);
} else if (features.avx2 && len >= 32) {
    return b64tobin_avx2(dest, src, len);
} else if (features.neon && len >= 16) {
    return b64tobin_neon(dest, src, len);
} else {
    return b64tobin_scalar(dest, src, len);
}
```

## Build Configuration

The build system (`setup.py`) automatically detects the target architecture and adds appropriate compiler flags:

- **x86-64**: `-msse4.2 -mavx2` (baseline)
- **ARM**: NEON enabled by default on aarch64
- **AVX512**: Enabled via conditional compilation (`#if defined(__AVX512F__)`)

AVX512 code is only compiled when the compiler supports it, ensuring backward compatibility.

## Target Deployment Environments

### Cloud Platforms with AVX512 Support

1. **AWS EC2**
   - Intel Ice Lake instances (C6i, M6i, R6i)
   - Intel Sapphire Rapids instances (C7i, M7i, R7i)
   - AMD Genoa instances (C7a, M7a, R7a) - AVX512 support

2. **Google Cloud Platform**
   - Intel Ice Lake instances (N2, C2, M2)
   - Intel Sapphire Rapids instances (C3, M3)

3. **Azure**
   - Dv5/Ev5 series (Intel Ice Lake)
   - Dasv5/Easv5 series (AMD Milan with AVX2, Genoa with AVX512)

### On-Premises Deployments

- **Intel**: Ice Lake (2019+), Sapphire Rapids (2023+)
- **AMD**: Zen 4 (2022+) with AVX512 support

## Future Optimization Opportunities

### Category A: Extending Existing SIMD Usage

These opportunities involve adding AVX512/wider SIMD support to operations already using some SIMD:

#### 1. Advanced AVX512 Instructions

**Current**: Using basic AVX512F + AVX512BW
**Opportunities**:
- AVX512VBMI2 for advanced string operations
- AVX512BITALG for bit manipulation
- AVX512VPOPCNTDQ for faster popcount

**Implementation Complexity**: Low-Medium
**Expected Performance Gain**: 10-20% for specific operations

#### 2. Optimized Base64 AVX512 Implementation

**Current**: Placeholder implementation falls back to scalar
**Opportunity**: Full vectorized base64 encode/decode

**Implementation Complexity**: High
**Expected Performance Gain**: 2-3x for base64 operations

#### 3. ARM SVE/SVE2 Support

**Target**: Modern ARM servers (AWS Graviton 3+, Ampere Altra)

**Benefits**:
- Scalable vector length (128 to 2048 bits)
- Better than NEON for large-scale data processing
- Native gather/scatter operations

**Implementation Complexity**: Medium
**Expected Performance Gain**: 1.5-2x on SVE2-capable ARM processors

### Category B: New SIMD Opportunities

These are operations that currently don't use SIMD but could benefit significantly:

#### 4. String Case Conversion ✨ NEW

**Current**: Character-by-character conversion in Python/Cython
**Opportunity**: SIMD case conversion for ASCII strings
**Location**: `src/cpp/simd_string_ops.cpp` (implemented)

**Implementation**: 
- AVX512: Processes 64 characters at once
- AVX2: Processes 32 characters at once
- Uses masked operations to selectively convert only alphabetic characters

**Implementation Complexity**: Low (completed)
**Expected Performance Gain**: 5-8x for UPPER/LOWER operations on ASCII strings

#### 5. Array Comparison Operations (HIGH PRIORITY)

**Current**: Scalar comparison loops with memcmp in `opteryx/compiled/list_ops/`
**Opportunity**: SIMD batch comparisons for ANY/ALL array operators

**Example Operations**:
- `list_anyop_eq` - Check if value equals any element in array
- `list_allop_eq` - Check if value equals all elements in array
- Similar for inequality, greater than, less than

**Implementation Complexity**: Medium
**Expected Performance Gain**: 3-4x for array predicate operations

#### 6. Bitwise Mask Operations

**Current**: Boolean mask operations done element-wise
**Opportunity**: Bulk AND/OR/XOR operations on boolean masks

**Use Cases**:
- Combining multiple filter conditions
- Bitmap indexes
- Null handling

**Implementation Complexity**: Low
**Expected Performance Gain**: 4-8x for filter combination operations

#### 7. IP Address Operations

**Current**: Scalar IP parsing and CIDR matching in `list_ip_in_cidr.pyx`
**Opportunity**: 
- SIMD parsing of IP addresses (parallel string-to-int)
- Vectorized subnet mask application

**Implementation Complexity**: Medium-High
**Expected Performance Gain**: 4-5x for IP filtering queries

#### 8. Numeric Aggregations on Nullable Data

**Current**: Using NumPy (which has SIMD but doesn't optimize nullable columns well)
**Opportunity**: Custom SIMD reductions with proper null handling

**Operations**: SUM, MIN, MAX, AVG on nullable columns

**Implementation Complexity**: Medium
**Expected Performance Gain**: 2-3x for aggregations on nullable data

#### 9. Date/Time Arithmetic

**Current**: Scalar date calculations in `compiled/functions/timestamp.pyx`
**Opportunity**:
- Vectorized date addition/subtraction
- Batch timezone conversions

**Implementation Complexity**: Medium
**Expected Performance Gain**: 2-4x for date-heavy queries

#### 10. Binary-to-Hex Conversion

**Current**: Scalar byte-to-hex conversion
**Opportunity**: SIMD parallel nibble extraction and lookup

**Implementation Complexity**: Low-Medium
**Expected Performance Gain**: 3-4x for binary display operations

## Priority Implementation Order

**High Priority (Immediate Impact)**:
1. ✅ String case conversion (UPPER/LOWER) - Completed
2. Array comparison operations (ANY/ALL) - Most common use case
3. Bitwise mask operations - Fundamental building block

**Medium Priority (Specialized Workloads)**:
4. Complete AVX512 base64 - Infrastructure exists
5. IP address operations - Network analytics
6. Numeric aggregations on nullable data - Common in real-world data

**Lower Priority (Specific Use Cases)**:
7. Date/time arithmetic - Specialized
8. Binary-to-hex conversion - Display operations
9. Advanced AVX512 instructions - Incremental gains
10. ARM SVE/SVE2 support - Future ARM deployments

## Performance Testing

To verify SIMD performance improvements:

```bash
# Run with SIMD disabled (for baseline)
export OPTERYX_FORCE_SCALAR=1
python tests/performance/benchmark.py

# Run with SIMD enabled (default)
unset OPTERYX_FORCE_SCALAR
python tests/performance/benchmark.py
```

## References

- [Intel Intrinsics Guide](https://www.intel.com/content/www/us/en/docs/intrinsics-guide/index.html)
- [ARM NEON Intrinsics](https://developer.arm.com/architectures/instruction-sets/intrinsics/)
- [simdjson](https://github.com/simdjson/simdjson)
- [xxHash](https://github.com/Cyan4973/xxHash)
