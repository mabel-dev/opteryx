List hashing fast-path — opteryx compiled/table_ops/hash_ops

Purpose
- Document when the list hashing implementation takes a buffer-aware fast path (no Python object allocation) and when it falls back to per-element Python hashing.

Where it lives
- Implementation: `opteryx/compiled/table_ops/hash_ops.pyx` — function `process_list_chunk`.
- Tests: `tests/unit/diagnostic/test_list_fast_paths.py`.

Fast-path conditions
- The list handler will use buffer-aware, zero-Python-object inner loops when the list child type is one of:
  - integer types (signed/unsigned, fixed-width)
  - floating point types
  - temporal types (timestamps/dates)
  - string or binary child types (string buffers + offsets)

- For the above child types the code reads child buffers directly and computes element hashes without creating Python objects. This gives a large performance win for dense numeric/string lists.

Fallback cases
- If the list child type is a complex/unrecognized Arrow type (for example, structs, maps, or arbitrary Python objects), the implementation falls back to slicing the child array and calling Python-level hashing for each element. This is correct but slower.

Correctness notes
- All paths account for Arrow `chunk.offset` on both the parent list array and on the child array. Validity bitmaps are checked with proper bit/byte arithmetic.
- 8-byte primitive loads are done via `memcpy` into a local `uint64_t` to avoid unaligned memory reads.

Testing and benchmarks
- Unit tests in `tests/unit/diagnostic/test_list_fast_paths.py` validate parity between flat and chunked arrays and basic correctness for nested and boolean lists.
- Benchmarks live in `tests/performance/benchmarks/bench_hash_ops.py`.

When to extend
- If you see nested lists of primitives commonly in workloads, consider implementing a dedicated nested-list stack-based fast path to avoid repeated slice() allocations.
- If child types are frequently small fixed-width types, additional micro-optimizations (incremental bit/byte pointers rather than recomputing shifts) can pay off.

"Why not always buffer-aware?"
- Some Arrow child types are not stored as simple contiguous buffers accessible by offset arithmetic (e.g., structs or other nested variable-width complex types). In those cases, the safe and correct approach is to create Python objects and hash them.

Contact
- If you have a representative large dataset that still performs poorly, attach it or a small reproducer and I'll benchmark and iterate.
