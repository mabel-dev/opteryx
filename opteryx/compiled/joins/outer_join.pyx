# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport uint64_t, int64_t
from libc.stdlib cimport malloc, free, calloc

from opteryx.third_party.abseil.containers cimport FlatHashMap
from opteryx.compiled.structures.hash_table cimport HashTable
from opteryx.compiled.structures.buffers cimport IntBuffer
from opteryx.compiled.table_ops.hash_ops cimport compute_row_hashes
from opteryx.compiled.table_ops.null_avoidant_ops cimport non_null_row_indices
from opteryx.utils.arrow import align_tables

import numpy
cimport numpy
numpy.import_array()

import pyarrow

cpdef HashTable probe_side_hash_map(object relation, list join_columns):
    """
    Build a hash table for the join operations (probe-side) using buffer-level hashing.
    """
    cdef HashTable ht = HashTable()
    cdef int64_t num_rows = relation.num_rows
    cdef int64_t[::1] non_null_indices
    cdef uint64_t[::1] row_hashes = numpy.empty(num_rows, dtype=numpy.uint64)
    cdef Py_ssize_t i

    non_null_indices = non_null_row_indices(relation, join_columns)

    # Compute hash of each row on the buffer level
    compute_row_hashes(relation, join_columns, row_hashes)

    # Insert into HashTable using row index + buffer-computed hash
    for i in range(non_null_indices.shape[0]):
        ht.insert(row_hashes[non_null_indices[i]], non_null_indices[i])

    return ht


def left_join_optimized(
    left_relation,
    right_relation,
    left_columns: list,
    right_columns: list,
    left_hash: FlatHashMap,
    filter_index
):
    """
    Optimized LEFT OUTER JOIN using Cython and efficient data structures.
    
    This implementation:
    - Uses the pre-built left hash map (no need to rebuild)
    - Uses efficient C-level memory for tracking matched left rows
    - Yields results incrementally to reduce memory usage
    - Supports bloom filter pre-filtering
    
    Parameters:
        left_relation: PyArrow table (build side)
        right_relation: PyArrow table (probe side)
        left_columns: Column names from left table to join on
        right_columns: Column names from right table to join on
        left_hash: Pre-built FlatHashMap of the left relation
        filter_index: Optional bloom filter for early filtering
        
    Yields:
        PyArrow tables containing matched and unmatched rows
    """
    cdef:
        int64_t left_num_rows = left_relation.num_rows
        int64_t right_num_rows = right_relation.num_rows
        int64_t chunk_size = 50_000
        int64_t i, j, row_idx
        uint64_t hash_val
        char* seen_flags
        IntBuffer left_indexes
        IntBuffer right_indexes
        int64_t[::1] right_non_null_indices
        uint64_t[::1] right_hashes
        size_t match_count
    
    # Allocate bit array to track which left rows have been matched
    # Use calloc to initialize to 0
    seen_flags = <char*>calloc(left_num_rows, sizeof(char))
    if seen_flags == NULL:
        raise MemoryError("Failed to allocate memory for seen_flags")
    
    try:
        # Apply bloom filter to right relation if available
        if filter_index is not None:
            possibly_matching_rows = filter_index.possibly_contains_many(right_relation, right_columns)
            right_relation = right_relation.filter(possibly_matching_rows)
            right_num_rows = right_relation.num_rows
            
            # Early exit if no matching rows in right relation
            if right_num_rows == 0:
                # Yield all left rows with NULL right columns in chunks
                for i in range(0, left_num_rows, chunk_size):
                    end_idx = min(i + chunk_size, left_num_rows)
                    chunk = list(range(i, end_idx))
                    yield align_tables(
                        source_table=left_relation,
                        append_table=right_relation.slice(0, 0),
                        source_indices=chunk,
                        append_indices=[None] * len(chunk),
                    )
                return
        
        # Get non-null indices and compute hashes for right relation
        right_non_null_indices = non_null_row_indices(right_relation, right_columns)
        right_hashes = numpy.empty(right_num_rows, dtype=numpy.uint64)
        compute_row_hashes(right_relation, right_columns, right_hashes)
        
        # Probe the left hash table with right relation rows
        left_indexes = IntBuffer()
        right_indexes = IntBuffer()
        
        for i in range(right_non_null_indices.shape[0]):
            row_idx = right_non_null_indices[i]
            hash_val = right_hashes[row_idx]
            
            # Get matching left rows from FlatHashMap
            left_matches = left_hash.get(hash_val)
            match_count = left_matches.size()
            if match_count == 0:
                continue
                
            for j in range(match_count):
                left_row = left_matches[j]
                seen_flags[left_row] = 1
                left_indexes.append(left_row)
                right_indexes.append(row_idx)
        
        # Yield matched rows
        if left_indexes.size() > 0:
            yield align_tables(
                right_relation,
                left_relation,
                right_indexes.to_numpy(),
                left_indexes.to_numpy(),
            )
        
        # Yield unmatched left rows with NULL right columns
        unmatched = [i for i in range(left_num_rows) if seen_flags[i] == 0]
        
        if unmatched:
            unmatched_left = left_relation.take(pyarrow.array(unmatched))
            # Create empty right table to leverage PyArrow's null column addition
            null_right = pyarrow.table(
                [pyarrow.nulls(0, type=field.type) for field in right_relation.schema],
                schema=right_relation.schema,
            )
            yield pyarrow.concat_tables([unmatched_left, null_right], promote_options="permissive")
    
    finally:
        # Always free the allocated memory
        free(seen_flags)


def right_join(
    left_relation,
    right_relation,
    left_columns: List[str],
    right_columns: List[str],
    left_hash,
    filter_index
):
    """
    Perform a RIGHT JOIN.

    This implementation ensures that all rows from the right table are included in the result set,
    with rows from the left table matched where possible, and columns from the left table
    filled with NULLs where no match is found.

    Parameters:
        left_relation (pyarrow.Table): The left pyarrow.Table to join.
        right_relation (pyarrow.Table): The right pyarrow.Table to join.
        left_columns (list of str): Column names from the left table to join on.
        right_columns (list of str): Column names from the right table to join on.

    Yields:
        pyarrow.Table: A chunk of the result of the RIGHT JOIN operation.
    """
    # Build hash table of left side
    cdef HashTable left_hash_table = HashTable()
    cdef Py_ssize_t num_left_rows = left_relation.num_rows
    cdef Py_ssize_t i

    cdef uint64_t* raw_hashes = <uint64_t*> malloc(num_left_rows * sizeof(uint64_t))
    if raw_hashes == NULL:
        raise MemoryError("Failed to allocate memory for hash table")
    cdef uint64_t[::1] left_hashes = <uint64_t[:num_left_rows]> raw_hashes

    compute_row_hashes(left_relation, left_columns, left_hashes)

    for i in range(num_left_rows):
        left_hash_table.insert(left_hashes[i], i)

    free(raw_hashes)

    cdef uint64_t* chunk_hashes
    cdef uint64_t[::1] right_hashes

    # Iterate over the right_relation in chunks
    for right_chunk in right_relation.to_batches(50_000):
        chunk_size = right_chunk.num_rows

        # Compute hashes for this right chunk
        chunk_hashes = <uint64_t*> malloc(chunk_size * sizeof(uint64_t))
        if chunk_hashes == NULL:
            raise MemoryError("Failed to allocate memory for chunk hashes")
        right_hashes = <uint64_t[:chunk_size]> chunk_hashes

        compute_row_hashes(right_chunk, right_columns, right_hashes)

        # Collect matches
        left_indexes = []
        right_indexes = []

        for i in range(chunk_size):
            left_matches = left_hash_table.get(right_hashes[i])
            if left_matches.size() > 0:
                left_indexes.extend(left_matches)
                right_indexes.extend([i] * len(left_matches))
            else:
                left_indexes.append(None)
                right_indexes.append(i)

        free(chunk_hashes)

        yield align_tables(left_relation, right_chunk, left_indexes, right_indexes)
