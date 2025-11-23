# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: infer_types=True

from libc.stdint cimport uint64_t, int64_t
from libc.stddef cimport size_t
from libcpp.vector cimport vector

cimport cython
import pyarrow as pa

from opteryx.draken.morsels.morsel cimport Morsel
from opteryx.third_party.abseil.containers cimport FlatHashMap
from opteryx.draken.vectors.int64_vector cimport Int64Vector
from opteryx.draken.vectors.float64_vector cimport Float64Vector
from opteryx.draken.vectors.vector cimport Vector
from opteryx.compiled.aggregations.count_distinct cimport count_distinct as c_count_distinct
from opteryx.compiled.structures.buffers cimport IntBuffer


cpdef Morsel group_by_morsel(
    Morsel morsel,
    list group_by_columns,
    list aggregate_functions,
    list internal_names,
    list column_names,
):
    """Group-by implemented directly on a Draken Morsel using hashing and per-group
    Draken vector operations. This avoids using numpy for intermediate calculations.

    Parameters:
        morsel: Draken Morsel
        group_by_columns: list of column names (str or bytes)
        aggregate_functions: list of tuples (field_name, function_name, count_options)
        internal_names: list of internal column names (after aggregation)
        column_names: list of final alias names (internal + grouping names)

    Returns:
        pyarrow.Table
    """
    cdef:
        FlatHashMap fmap = FlatHashMap()
        uint64_t[::1] row_hashes
        Py_ssize_t num_rows
        Py_ssize_t i
        object col
        object key
        list seen_keys = []
        object iter_indices
        int64_t group_val_int
        double group_val_double
        uint64_t h
        set seen_set

    if morsel is None or morsel.num_rows == 0:
        # return empty table similar to what pyarrow would return
        # build empty python lists for columns
        columns = {}
        for name in internal_names + group_by_columns:
            columns[name] = []
        return pa.Table.from_pydict(columns)

    # Normalize group_by column names to bytes if necessary
    cdef list gb_cols = []
    for col in group_by_columns:
        if isinstance(col, bytes):
            gb_cols.append(col)
        else:
            gb_cols.append(str(col).encode("utf-8"))

    # Compute row hashes for grouping
    if len(gb_cols) == 0:
        # whole morsel as single group
        # single group with all row indices
        num_rows = morsel.num_rows
        # create a python list of indices 0..num_rows-1 for a single group
        seen_keys = [0]
        fmap.insert(0, 0)  # ensure map has at least key 0
        for i in range(1, num_rows):
            fmap.insert(0, i)
    else:
        # call morsel.hash(columns=gb_cols)
        row_hashes = morsel.hash(columns=gb_cols)
        num_rows = morsel.num_rows

        # Build map of hash -> vector of row indices
        # Store seen keys in seen_keys (Python list) to iterate later
        for i in range(num_rows):
            h = <uint64_t> row_hashes[i]
            fmap.insert(h, i)
            # track seen keys
            # we avoid calling fmap.get to enumerate keys; instead maintain seen_keys
            if not seen_keys or seen_keys[-1] != h:
                # naive approach: append h only when it differs from last seen - not guaranteed unique
                # ensure uniqueness via small set
                pass

        # Build unique keys dict by iterating again and filling a Python dict
        seen_keys = []
        seen_set = set()
        for i in range(num_rows):
            h = <uint64_t> row_hashes[i]
            if h not in seen_set:
                seen_set.add(h)
                seen_keys.append(h)

    # Now compute aggregates per key
    cdef Py_ssize_t num_groups = len(seen_keys)

    # Prepare results as Python lists
    cdef dict internal_results = {}
    for alias in internal_names:
        internal_results[alias] = []

    cdef dict group_results = {}
    for gb in group_by_columns:
        group_results[gb] = []

    cdef int g_idx
    for g_idx in range(num_groups):
        h = <uint64_t> seen_keys[g_idx]
        # get indices vector
        cdef vector[int64_t] indices = fmap.get(h)
        cdef Py_ssize_t nindices = indices.size()

        # For each group column, fetch the representative value at first index
        for gb in gb_cols:
            cdef Vector gbvec = morsel.column(gb)
            # rely on Vector __getitem__ to return Python scalar
            if nindices > 0:
                val = gbvec[<Py_ssize_t>indices[0]]
            else:
                val = None
            group_results[gb.decode('utf-8')] .append(val)

        # For each aggregator, compute value
        for idx in range(len(aggregate_functions)):
            field_name, func_name, _count_opts = aggregate_functions[idx]
            internal_name = internal_names[idx]

            cdef object vec_field = None
            if isinstance(field_name, bytes):
                vec_field = morsel.column(field_name)
            else:
                vec_field = morsel.column(str(field_name).encode('utf-8'))

            # If count (wildcard or COUNT(*))
            if func_name == 'count':
                internal_results[internal_name].append(nindices)
                continue

            # For count distinct, we can call compiled count_distinct on the taken vector
            if func_name == 'count_distinct':
                # convert indices to IntBuffer and pass to count_distinct
                # Build a pyarrow array from vec_field.take using indices
                # Prefer direct vector.take with memoryview; fallback to python list
                try:
                    taken = vec_field.take(<int32_t[:nindices]> indices)
                except Exception:
                    try:
                        py_indices = [<int>(indices[i]) for i in range(nindices)]
                        taken = vec_field.take(py_indices)
                    except Exception:
                        taken = None

                if taken is None:
                    # No efficient path; fallback to computing with python loop
                    # Build a set of values via Python to compute distinct count
                    seen = set()
                    for ii in range(nindices):
                        seen.add(vec_field[<Py_ssize_t>indices[ii]])
                    result_set = len(seen)
                else:
                    # 'taken' is a vector accessible to compiled count_distinct
                    result_set = c_count_distinct(taken, None)
                internal_results[internal_name].append(result_set.items())
                continue

            # For numeric aggregations sum/min/max/mean, call typed vector methods
            # In Draken, typed vectors expose sum, min, max methods
            try:
                taken_vec = vec_field.take(<int32_t[:nindices]> indices)
            except Exception:
                try:
                    py_indices = [<int>(indices[i]) for i in range(nindices)]
                    taken_vec = vec_field.take(py_indices)
                except Exception:
                    taken_vec = None

            if taken_vec is None:
                # Fallback to computing using Python scalar accesses
                if func_name == 'sum':
                    s = 0
                    for ii in range(nindices):
                        v = vec_field[<Py_ssize_t>indices[ii]]
                        if v is None:
                            continue
                        s += v
                    internal_results[internal_name].append(s)
                elif func_name == 'min':
                    vals = [vec_field[<Py_ssize_t>indices[ii]] for ii in range(nindices) if vec_field[<Py_ssize_t>indices[ii]] is not None]
                    internal_results[internal_name].append(min(vals) if vals else None)
                elif func_name == 'max':
                    vals = [vec_field[<Py_ssize_t>indices[ii]] for ii in range(nindices) if vec_field[<Py_ssize_t>indices[ii]] is not None]
                    internal_results[internal_name].append(max(vals) if vals else None)
                elif func_name == 'mean':
                    s = 0
                    count = 0
                    for ii in range(nindices):
                        v = vec_field[<Py_ssize_t>indices[ii]]
                        if v is None:
                            continue
                        s += v
                        count += 1
                    internal_results[internal_name].append(float(s)/count if count > 0 else None)
                else:
                    internal_results[internal_name].append(None)
                continue

            if func_name == 'sum':
                internal_results[internal_name].append(taken_vec.sum())
            elif func_name == 'min':
                internal_results[internal_name].append(taken_vec.min())
            elif func_name == 'max':
                internal_results[internal_name].append(taken_vec.max())
            elif func_name == 'mean':
                # mean = sum/count
                if nindices > 0:
                    s = taken_vec.sum()
                    internal_results[internal_name].append(float(s) / nindices)
                else:
                    internal_results[internal_name].append(None)
            else:
                internal_results[internal_name].append(None)

    # Build pyarrow table from internal_results + group_results
    cdef dict out = {}
    for name in internal_names:
        out[name] = internal_results[name]
    for gb in group_by_columns:
        out[gb] = group_results[gb]

    table = pa.Table.from_pydict(out)

    # Rename columns if necessary to match column_names (aliases)
    # column_names contains final names in order internal_names + group_by_columns
    # We'll try a rename operation; if fails, ignore
    try:
        cdef list new_names = []
        for n in internal_names:
            new_names.append(n)
        for gb in group_by_columns:
            new_names.append(gb)
        # If alias names differ, just rename directly
        if len(column_names) == len(new_names):
            table = table.rename_columns(column_names)
    except Exception:
        pass

    return Morsel.from_arrow(table)
