import numpy as np

from cjoin import cython_inner_join
from cjoin import cython_left_join

from .helpers import columns_to_array_denulled
from .helpers import groupify_array


def align_tables(source_table, append_table, source_indices, append_indices):
    # If either source_indices or append_indices is empty, return the source_table taken with source_indices immediately
    if len(source_indices) == 0 or len(append_indices) == 0:
        return source_table.take(source_indices)

    # Take the rows from source_table at the specified source_indices
    aligned_table = source_table.take(source_indices)

    # Create a set of column names from the source table for efficient existence checking
    source_column_names = set(source_table.column_names)

    # Iterate through the column names of append_table
    for column_name in append_table.column_names:
        # If the column_name is not found in source_column_names
        if column_name not in source_column_names:
            # Append the column from append_table to aligned_table, taking the elements at the specified append_indices
            aligned_table = aligned_table.append_column(
                column_name, append_table.column(column_name).take(append_indices)
            )

    # Return the aligned table
    return aligned_table


def inner_join(left, right, left_on, right_on):
    # Gather join columns - create arrays of the hashes of the values in the column
    # updated for Opteryx
    l_array, r_array = columns_to_array_denulled(left, left_on), columns_to_array_denulled(
        right, right_on
    )

    # Groupify the join array, this generates a set of data about the array
    # including the unique values in the array, and the sort order for the array.
    l_distinct, lc, l_sort_idxs, lbi = groupify_array(l_array)
    r_distinct, rc, r_sort_idxs, rbi = groupify_array(r_array)

    # Create the list of unique values combining the column from the left and the right
    # tables
    unique, inv = np.unique(np.concatenate([l_distinct, r_distinct]), return_inverse=True)

    # Align Left side
    # the inv array the positions in the unique list of the combined left and right list,
    # because we build this using np.concat, we know the first set of records is from the
    # left list.
    linv = inv[: l_distinct.shape[0]]
    # this creates empty masks
    lcc, lbic = np.zeros_like(unique), np.zeros_like(unique)
    # this sets the values at the positions in linv to the count values from groupify above
    lcc[linv] = lc
    # this sets the values at the positions in linv to the begin indexes of the groups
    lbic[linv] = lbi

    # Align right side
    # the inv array the positions in the unique list of the combined left and right list,
    # because we build this using np.concat, we know the end set of records is from the
    # right list.
    rinv = inv[l_distinct.shape[0] :]
    # this creates empty masks
    rcc, rbic = np.zeros_like(unique), np.zeros_like(unique)
    # this sets the values at the positions in rinv to the count values from groupify above
    rcc[rinv] = rc
    # this sets the values at the positions in rinv to the begin indexes of the groups
    rbic[rinv] = rbi

    # Perform cjoin
    left_align, right_align = cython_inner_join(
        l_sort_idxs.astype(np.int64),
        r_sort_idxs.astype(np.int64),
        lcc.astype(np.int64),
        rcc.astype(np.int64),
        lbic.astype(np.int64),
        rbic.astype(np.int64),
    )

    return align_tables(left, right, left_align, right_align)


def left_join(left, right, left_on, right_on):  # pragma: no cover - currently not called
    # Gather join columns - create arrays of the hashes of the values in the column
    # new for Opteryx
    l_array, r_array = columns_to_array_denulled(left, left_on), columns_to_array_denulled(
        right, right_on
    )

    # Groupify the join array, this generates a set of data about the array
    # including the unique values in the array, and the sort order for the array.
    l_distinct, lc, l_sort_idxs, lbi = groupify_array(l_array)
    r_distinct, rc, r_sort_idxs, rbi = groupify_array(r_array)

    # Create the list of unique values combining the column from the left and the right
    # tables
    unique, inv = np.unique(np.concatenate([l_distinct, r_distinct]), return_inverse=True)

    # Align Left side
    # the inv array the positions in the unique list of the combined left and right list,
    # because we build this using np.concat, we know the first set of records is from the
    # left list.
    linv = inv[: l_distinct.shape[0]]
    # this creates empty masks
    lcc, lbic = np.zeros_like(unique), np.zeros_like(unique)
    # this sets the values at the positions in linv to the count values from groupify above
    lcc[linv] = lc
    # this sets the values at the positions in linv to the begin indexes of the groups
    lbic[linv] = lbi

    # Align right side
    # the inv array the positions in the unique list of the combined left and right list,
    # because we build this using np.concat, we know the end set of records is from the
    # right list.
    rinv = inv[l_distinct.shape[0] :]
    # this creates empty masks
    rcc, rbic = np.zeros_like(unique), np.zeros_like(unique)
    # this sets the values at the positions in rinv to the count values from groupify above
    rcc[rinv] = rc
    # this sets the values at the positions in rinv to the begin indexes of the groups
    rbic[rinv] = rbi

    rows = len(l_array) * len(r_array)
    left_align, right_align = np.empty(rows, dtype=np.int64), np.empty(rows, dtype=np.int64)

    # Perform cjoin
    left_align, right_align = cython_left_join(
        l_sort_idxs.astype(np.int64),
        r_sort_idxs.astype(np.int64),
        lcc.astype(np.int64),
        rcc.astype(np.int64),
        lbic.astype(np.int64),
        rbic.astype(np.int64),
    )

    return align_tables(left, right, left_align, right_align)
