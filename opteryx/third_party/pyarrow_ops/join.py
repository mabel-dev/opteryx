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
    # Gather join columns
    t0 = time.time()
    l_arr, r_arr = columns_to_array(left, on), columns_to_array(right, on)

    # Groupify the join array
    t1 = time.time()
    ld, lc, lidxs, lbi = groupify_array(l_arr)
    rd, rc, ridxs, rbi = groupify_array(r_arr)

    # Find both dicts
    t2 = time.time()
    bd, inv = np.unique(np.concatenate([ld, rd]), return_inverse=True)

    # Align Left side
    t3 = time.time()
    linv = inv[: ld.shape[0]]
    lcc, lbic = np.zeros_like(bd), np.zeros_like(bd)
    lcc[linv] = lc
    lbic[linv] = lbi

    # Align right side
    rinv = inv[ld.shape[0] :]
    rcc, rbic = np.zeros_like(bd), np.zeros_like(bd)
    rcc[rinv] = rc
    rbic[rinv] = rbi

    # Perform cjoin
    t4 = time.time()
    left_align, right_align = inner_join(
        lidxs.astype(np.int64),
        ridxs.astype(np.int64),
        lcc.astype(np.int64),
        rcc.astype(np.int64),
        lbic.astype(np.int64),
        rbic.astype(np.int64),
    )

    # print("Join took:", time.time() - t4, t4 - t3 , t2 - t1, t1 - t0)
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
