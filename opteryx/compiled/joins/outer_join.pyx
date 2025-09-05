# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport uint64_t
from libc.stdlib cimport malloc, free

from opteryx.compiled.structures.hash_table cimport HashTable
from opteryx.compiled.table_ops.hash_ops cimport compute_row_hashes
from opteryx.utils.arrow import align_tables


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
    left_hash_table = HashTable()
    num_left_rows = left_relation.num_rows

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
