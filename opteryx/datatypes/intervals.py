# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Callable
from typing import Dict
from typing import Optional
from typing import Tuple

import numpy
import pyarrow
import pyarrow.compute
from orso.types import OrsoTypes


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

    months, seconds = right[0].as_py()

    result = left.astype("datetime64[s]") + (seconds * signum)

    # Handle months separately, requiring special logic
    if months:
        for index in range(len(result)):
            result[index] = add_months_numpy(result[index], months * signum)

    return result


def _simple_interval_op(left, left_type, right, right_type, operator):
    from opteryx.managers.expression.ops import _inner_filter_operations

    left_months = pyarrow.compute.list_element(left, 0)
    left_seconds = pyarrow.compute.list_element(left, 1)

    right_months = pyarrow.compute.list_element(right, 0)
    right_seconds = pyarrow.compute.list_element(right, 1)

    if (
        pyarrow.compute.any(pyarrow.compute.not_equal(left_months, 0)).as_py()
        or pyarrow.compute.any(pyarrow.compute.not_equal(right_months, 0)).as_py()
    ):
        from opteryx.exceptions import UnsupportedSyntaxError

        raise UnsupportedSyntaxError("Cannot compare INTERVALs with MONTH or YEAR components.")

    #    months = _inner_filter_operations(left_months, operator, right_months)
    #    months_eq = _inner_filter_operations(left_months, "Eq", right_months)
    seconds = _inner_filter_operations(left_seconds, operator, right_seconds)

    return seconds


def _work_out_the_kernel(left, left_type, right, right_type, operator):
    if left_type in (OrsoTypes._MISSING_TYPE, 0) and len(left) > 0:
        sample = left[0]
        if isinstance(sample, numpy.datetime64):
            left_type = OrsoTypes.TIMESTAMP

    if right_type in (OrsoTypes._MISSING_TYPE, 0) and len(right) > 0:
        sample = right[0]
        if isinstance(sample, numpy.datetime64):
            right_type = OrsoTypes.TIMESTAMP

    if left_type == OrsoTypes.INTERVAL and right_type in (OrsoTypes.DATE, OrsoTypes.TIMESTAMP):
        return _date_plus_interval(left, left_type, right, right_type, operator)
    if right_type == OrsoTypes.INTERVAL and left_type in (OrsoTypes.DATE, OrsoTypes.TIMESTAMP):
        return _date_plus_interval(left, left_type, right, right_type, operator)
    return _simple_interval_op(left, left_type, right, right_type, operator)


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
    # we need to type the outcome of calcs better
    (0, OrsoTypes.INTERVAL, "Plus"): _work_out_the_kernel,
    (0, OrsoTypes.INTERVAL, "Minus"): _work_out_the_kernel,
    (0, OrsoTypes.INTERVAL, "Eq"): _work_out_the_kernel,
    (0, OrsoTypes.INTERVAL, "NotEq"): _work_out_the_kernel,
    (0, OrsoTypes.INTERVAL, "Gt"): _work_out_the_kernel,
    (0, OrsoTypes.INTERVAL, "GtEq"): _work_out_the_kernel,
    (0, OrsoTypes.INTERVAL, "Lt"): _work_out_the_kernel,
    (0, OrsoTypes.INTERVAL, "LtEq"): _work_out_the_kernel,
    (OrsoTypes.INTERVAL, 0, "Plus"): _work_out_the_kernel,
    (OrsoTypes.INTERVAL, 0, "Minus"): _work_out_the_kernel,
    (OrsoTypes.INTERVAL, 0, "Eq"): _work_out_the_kernel,
    (OrsoTypes.INTERVAL, 0, "NotEq"): _work_out_the_kernel,
    (OrsoTypes.INTERVAL, 0, "Gt"): _work_out_the_kernel,
    (OrsoTypes.INTERVAL, 0, "GtEq"): _work_out_the_kernel,
    (OrsoTypes.INTERVAL, 0, "Lt"): _work_out_the_kernel,
    (OrsoTypes.INTERVAL, 0, "LtEq"): _work_out_the_kernel,
}
