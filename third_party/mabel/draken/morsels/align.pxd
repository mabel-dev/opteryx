from libc.stdint cimport int64_t
from opteryx.draken.morsels.morsel cimport Morsel


cpdef Morsel align_tables(
    Morsel source_morsel,
    Morsel append_morsel,
    int64_t[::1] source_indices,
    int64_t[::1] append_indices
)

cpdef Morsel align_tables_pyarray(
    Morsel source_morsel,
    Morsel append_morsel,
    object source_indices,
    object append_indices
)
