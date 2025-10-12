# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Callable
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Tuple

import numpy
import pyarrow
import pyarrow.compute
from orso.types import OrsoTypes

MICROSECONDS_PER_SECOND = 1_000_000
MICROSECONDS_PER_MINUTE = 60 * MICROSECONDS_PER_SECOND
MICROSECONDS_PER_HOUR = 60 * MICROSECONDS_PER_MINUTE
MICROSECONDS_PER_DAY = 24 * MICROSECONDS_PER_HOUR
NANOSECONDS_PER_MICROSECOND = 1_000


def _normalise_interval_value(value) -> Tuple[int, int]:
    """
    Convert different interval representations into the internal (months, microseconds) tuple form.
    """
    if value is None:
        return (0, 0)
    if isinstance(value, tuple):
        return value
    if hasattr(value, "as_py"):
        value = value.as_py()
    if hasattr(value, "months") and hasattr(value, "nanoseconds"):
        months = int(value.months)
        micros = (
            int(value.days) * MICROSECONDS_PER_DAY
            + int(value.nanoseconds) // NANOSECONDS_PER_MICROSECOND
        )
        return (months, micros)
    return value


def normalize_interval_value(value) -> Tuple[int, int]:
    """
    Public wrapper for interval normalization.
    """
    return _normalise_interval_value(value)


def _intervals_to_month_day_nano(rows: Iterable[Optional[Tuple[int, int]]]) -> pyarrow.Array:
    """
    Convert an iterable of (months, microseconds) tuples into a month-day-nano INTERVAL Arrow array.
    """
    converted = []
    for entry in rows:
        if entry is None or any(component is None for component in entry):
            converted.append(None)
            continue
        months, microseconds = entry
        if microseconds is None:
            converted.append((int(months), 0, 0))
            continue
        days, remainder = divmod(int(microseconds), MICROSECONDS_PER_DAY)
        nanoseconds = remainder * NANOSECONDS_PER_MICROSECOND
        converted.append((int(months), int(days), int(nanoseconds)))
    return pyarrow.array(converted, type=pyarrow.month_day_nano_interval())


def to_arrow_interval(array: pyarrow.Array) -> pyarrow.Array:
    """
    Ensure the provided Arrow array uses the month-day-nano INTERVAL logical type.
    """
    if isinstance(array, pyarrow.ChunkedArray):
        converted = [to_arrow_interval(chunk) for chunk in array.chunks]
        return pyarrow.chunked_array(converted)

    if pyarrow.types.is_interval(array.type) and array.type == pyarrow.month_day_nano_interval():
        return array

    if pyarrow.types.is_list(array.type):
        months = pyarrow.compute.list_element(array, 0)
        microseconds = pyarrow.compute.list_element(array, 1)
        rows = zip(months.to_pylist(), microseconds.to_pylist())
        return _intervals_to_month_day_nano(rows)

    if pyarrow.types.is_struct(array.type):
        rows = array.to_pylist()
        converted = []
        for entry in rows:
            if entry is None:
                converted.append(None)
                continue
            months = entry.get("months", 0)
            days = entry.get("days", 0)
            nanoseconds = entry.get("nanoseconds", 0)
            if months is None or days is None or nanoseconds is None:
                converted.append(None)
                continue
            converted.append((int(months), int(days), int(nanoseconds)))
        return pyarrow.array(converted, type=pyarrow.month_day_nano_interval())

    # As a fallback, treat values as already normalised tuples.
    return _intervals_to_month_day_nano(array.to_pylist())


def add_months_numpy(dates, months_to_add):
    """
    Adds a specified number of months to dates in a numpy array, adjusting for end-of-month overflow.

    Parameters:
    - dates: np.ndarray of dates (numpy.datetime64)
    - months_to_add: int, the number of months to add to each date

    Returns:
    - np.ndarray: Adjusted dates
    """
    # Convert dates to 'M' (month) granularity for addition
    months = dates.astype("datetime64[M]")

    # Add months (broadcasts the scalar value across the array)
    new_dates = months + numpy.timedelta64(months_to_add, "M")

    # Calculate the last day of the new month for each date
    last_day_of_new_month = new_dates + numpy.timedelta64(1, "M") - numpy.timedelta64(1, "D")

    # Calculate the day of the month for each original date
    day_of_month = dates - months

    # Adjust dates that would overflow their new month
    overflow_mask = day_of_month > (last_day_of_new_month - new_dates)
    adjusted_dates = numpy.where(overflow_mask, last_day_of_new_month, new_dates + day_of_month)

    return adjusted_dates.astype("datetime64[us]")


def _date_plus_interval(left, left_type, right, right_type, operator):
    """
    Adds intervals to dates, utilizing integer arithmetic for performance improvements.
    """
    signum = 1 if operator == "Plus" else -1
    if left_type == OrsoTypes.INTERVAL:
        left, right = right, left

    months, microseconds = _normalise_interval_value(right[0])

    if hasattr(left, "to_numpy"):
        left = left.to_numpy(zero_copy_only=False)

    result = left.astype("datetime64[us]") + (microseconds * signum)

    # Handle months separately, requiring special logic
    if months:
        for index in range(len(result)):
            result[index] = add_months_numpy(result[index], months * signum)

    return result


def _simple_interval_op(left, left_type, right, right_type, operator):
    from opteryx.managers.expression.ops import _inner_filter_operations

    left_months = pyarrow.compute.list_element(left, 0)
    left_microseconds = pyarrow.compute.list_element(left, 1)

    right_months = pyarrow.compute.list_element(right, 0)
    right_microseconds = pyarrow.compute.list_element(right, 1)

    if (
        pyarrow.compute.any(pyarrow.compute.not_equal(left_months, 0)).as_py()
        or pyarrow.compute.any(pyarrow.compute.not_equal(right_months, 0)).as_py()
    ):
        from opteryx.exceptions import UnsupportedSyntaxError

        raise UnsupportedSyntaxError("Cannot compare INTERVALs with MONTH or YEAR components.")

    #    months = _inner_filter_operations(left_months, operator, right_months)
    #    months_eq = _inner_filter_operations(left_months, "Eq", right_months)
    microseconds = _inner_filter_operations(left_microseconds, operator, right_microseconds)

    return microseconds


INTERVAL_KERNELS: Dict[Tuple[OrsoTypes, OrsoTypes, str], Optional[Callable]] = {
    (OrsoTypes.INTERVAL, OrsoTypes.INTERVAL, "Plus"): _simple_interval_op,
    (OrsoTypes.INTERVAL, OrsoTypes.INTERVAL, "Minus"): _simple_interval_op,
    (OrsoTypes.INTERVAL, OrsoTypes.INTERVAL, "Eq"): _simple_interval_op,
    (OrsoTypes.INTERVAL, OrsoTypes.INTERVAL, "NotEq"): _simple_interval_op,
    (OrsoTypes.INTERVAL, OrsoTypes.INTERVAL, "Gt"): _simple_interval_op,
    (OrsoTypes.INTERVAL, OrsoTypes.INTERVAL, "GtEq"): _simple_interval_op,
    (OrsoTypes.INTERVAL, OrsoTypes.INTERVAL, "Lt"): _simple_interval_op,
    (OrsoTypes.INTERVAL, OrsoTypes.INTERVAL, "LtEq"): _simple_interval_op,
    (OrsoTypes.INTERVAL, OrsoTypes.TIMESTAMP, "Plus"): _date_plus_interval,
    (OrsoTypes.INTERVAL, OrsoTypes.TIMESTAMP, "Minus"): _date_plus_interval,
    (OrsoTypes.INTERVAL, OrsoTypes.DATE, "Plus"): _date_plus_interval,
    (OrsoTypes.INTERVAL, OrsoTypes.DATE, "Minus"): _date_plus_interval,
    (OrsoTypes.TIMESTAMP, OrsoTypes.INTERVAL, "Plus"): _date_plus_interval,
    (OrsoTypes.TIMESTAMP, OrsoTypes.INTERVAL, "Minus"): _date_plus_interval,
    (OrsoTypes.DATE, OrsoTypes.INTERVAL, "Plus"): _date_plus_interval,
    (OrsoTypes.DATE, OrsoTypes.INTERVAL, "Minus"): _date_plus_interval,
}
