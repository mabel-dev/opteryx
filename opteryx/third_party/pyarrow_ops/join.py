from enum import unique
import time
import numpy as np
import pyarrow as pa
from .helpers import columns_to_array, groupify_array
from cjoin import inner_join


def align_tables(t1, t2, l1, l2):
    # Align tables
    table = t1.take(l1)
    for c in t2.column_names:
        if c not in t1.column_names:
            table = table.append_column(c, t2.column(c).take(l2))
    return table


def join(left, right, on):
    # Gather join columns - create arrays of the hashes of the values in the column
    t0 = time.time()
    l_array, r_array = columns_to_array(left, on), columns_to_array(right, on)

    # Groupify the join array, this generates a set of data about the array
    # including the unique values in the array, and the sort order for the array.
    t1 = time.time()
    l_distinct, lc, l_sort_idxs, lbi = groupify_array(l_array)
    r_distinct, rc, r_sort_idxs, rbi = groupify_array(r_array)

    # Create the list of unique values combining the column from the left and the right
    # tables
    t2 = time.time()
    unique, inv = np.unique(np.concatenate([l_distinct, r_distinct]), return_inverse=True)

    # Align Left side
    t3 = time.time()
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
    t4 = time.time()
    left_align, right_align = inner_join(
        l_sort_idxs.astype(np.int64),
        r_sort_idxs.astype(np.int64),
        lcc.astype(np.int64),
        rcc.astype(np.int64),
        lbic.astype(np.int64),
        rbic.astype(np.int64),
    )

    #print("Join took:", time.time() - t4, t4 - t3 , t2 - t1, t1 - t0)
    return align_tables(left, right, left_align, right_align)


# Old Code:
def single_key_hash_join(t1, t2, key):
    # Create idx_maps per distinct value
    # ht = defaultdict(list, split_array(column(t2, key)))
    ht = defaultdict(list)
    [
        ht[t].append(i)
        for i, t in enumerate(column(t2, key).to_numpy(zero_copy_only=False))
    ]
    f = operator.itemgetter(*column(t1, key).to_numpy(zero_copy_only=False))
    idx_maps = f(ht)

    # Gather indices
    l1 = [i1 for i1, idx_map in enumerate(idx_maps) for i2 in idx_map]
    l2 = list(itertools.chain.from_iterable(idx_maps))
    return align_tables(t1, t2, l1, l2)


def multi_key_hash_join(t1, t2, on):
    # List of tuples of columns
    on1, on2 = [c.to_numpy() for c in t1.select(on).itercolumns()], [
        c.to_numpy() for c in t2.select(on).itercolumns()
    ]

    # Zip idx / on values
    tup1 = map(hash, zip(*on1))
    tup2 = map(hash, zip(*on2))

    # Hash smaller table into dict {(on):[idx1, idx2, ...]}
    ht = defaultdict(list)
    [ht[t].append(i) for i, t in enumerate(tup2)]
    f = operator.itemgetter(*tup1)
    idx_maps = f(ht)

    # Gather indices
    l1 = [i1 for i1, idx_map in enumerate(idx_maps) for i2 in idx_map]
    l2 = list(itertools.chain.from_iterable(idx_maps))
    return align_tables(t1, t2, l1, l2)


def join_old(left, right, on):
    # We want the smallest table to be on the right
    if left.num_rows >= right.num_rows:
        t1, t2 = left, right
    else:
        t1, t2 = right, left

    # Choose join method
    if len(on) == 1:
        return single_key_hash_join(t1, t2, on[0])
    else:
        return multi_key_hash_join(t1, t2, on)
