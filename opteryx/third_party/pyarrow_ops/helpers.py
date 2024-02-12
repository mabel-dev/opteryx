import numpy
import pyarrow
import pyarrow.compute

APOLLO_11_DURATION: int = 703115  # we need a constant to use as a seed


def groupify_array(arr):
    # Input: Pyarrow/Numpy array
    # Output:
    #   - 1. Unique values
    #   - 2. Count per unique
    #   - 3. Sort index
    #   - 4. Begin index per unique

    # UPDATED FOR OPTERYX
    dic, counts = numpy.unique(arr, return_counts=True, equal_nan=True)
    sort_idx = numpy.argsort(arr)
    begin_indexes = numpy.zeros_like(counts)
    begin_indexes[1:] = numpy.cumsum(counts)[:-1]
    return dic, counts, sort_idx, begin_indexes


def _hash_value(val):
    # Added for Opteryx - Original code had bugs relating to distinct and nulls
    t = type(val)
    if t == dict:
        return _hash_value(tuple(val.values()))
    if t in (list, numpy.ndarray, tuple):
        # not perfect but tries to eliminate some of the flaws in other approaches
        return hash(".".join(f"{i}:{v}" for i, v in enumerate(val)))
    return hash(val)


def columns_to_array(table, columns):
    """modified for Opteryx"""
    # used for distinct
    columns = [columns] if isinstance(columns, str) else list(dict.fromkeys(columns))

    if len(columns) == 1:
        column_values = table.column(columns[0]).to_numpy()

        if numpy.issubdtype(column_values.dtype, numpy.character):
            # optimize handling string columns
            from orso.cityhash import CityHash64

            return numpy.array(
                [CityHash64(s.encode()) for s in column_values],
                numpy.uint64,
            )
        return numpy.array([_hash_value(el) for el in column_values], dtype=numpy.uint64)

    columns = sorted(set(table.column_names).intersection(columns))
    values = (c.to_numpy() for c in table.select(columns).itercolumns())
    return numpy.array([_hash_value(x) for x in zip(*values)])


def filter_rows_with_nulls(table, columns_of_interest):
    """
    ADDED FOR OPTERYX

    Filters out rows from a PyArrow table where any of the specified columns have null values.

    Parameters:
    - table (pyarrow.Table): The PyArrow table to filter.
    - columns_of_interest (list of str): Column names to check for nulls.

    Returns:
    - pyarrow.Table: A new PyArrow table with rows containing nulls in the specified columns removed.
    """

    # Validate that all specified columns exist in the table
    for col_name in columns_of_interest:
        if col_name not in table.column_names:
            raise ValueError(f"Column '{col_name}' not found in the table.")

    # Create a combined mask for null values in the specified columns
    null_masks = [
        pyarrow.compute.is_null(table.column(col_name)) for col_name in columns_of_interest
    ]
    combined_mask = null_masks.pop()  # Start with the mask for the first column

    # If more than one column, combine the masks using logical OR
    for mask in null_masks:
        combined_mask = pyarrow.compute.or_(combined_mask, mask)

    # Invert the mask to select rows that are NOT null in the specified columns
    keep_mask = pyarrow.compute.invert(combined_mask)

    # Filter the table to remove rows with nulls in the specified columns
    filtered_table = table.filter(keep_mask)

    return filtered_table
