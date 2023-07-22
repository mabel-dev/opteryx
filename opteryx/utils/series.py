# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import numpy
from orso.types import OrsoTypes

from opteryx.exceptions import SqlError
from opteryx.utils import dates


def generate_series(*args):
    arg_len = len(args)
    arg_vals = [i.value for i in args]
    first_arg_type = args[0].type

    # if the parameters are numbers, generate series is an alias for range
    if first_arg_type in (
        OrsoTypes.INTEGER,
        OrsoTypes.DOUBLE,
    ):
        if arg_len not in (2, 3):
            raise SqlError("generate_series for numbers takes 2 or 3 parameters.")
        return numeric_range(*arg_vals)

    # if the params are timestamps, we create time intervals
    if args[0].type in (OrsoTypes.DATE, OrsoTypes.TIMESTAMP):
        if arg_len != 3:
            raise SqlError("generate_series for dates needs start, end, and interval parameters")
        return dates.date_range(*arg_vals)

    raise SqlError("Unsupported value for GENERATE_SERIES")


def numeric_range(*args):
    """
    Combines numpy.arange and numpy.isclose to mimic
    open, half-open, and closed intervals.
    Avoids floating-point rounding errors like in
    numpy.arange(1, 1.3, 0.1) returning
    array([1. , 1.1, 1.2, 1.3]).

    Args:
        [start, ]stop, [step, ]: Arguments as in numpy.arange.

    Returns:
        numpy.ndarray: Array of evenly spaced values.

    Raises:
        ValueError: If the number of arguments is not 1, 2, or 3.

    Examples:
        generate_range(5)
        generate_range(1, 5)
        generate_range(1, 5, 0.5)
    """

    # Process arguments
    if len(args) == 2:
        start, stop = args
        step = numpy.int8(1)
    elif len(args) == 3:
        start, stop, step = args
        # Ensure the last item is in the series
    else:
        raise ValueError("Invalid number of arguments. Expected 2, or 3: start, stop [, step].")

    # numpy does this different to SQL so always want one more
    stop += step

    # how many of the resulting series are we going to keep?
    max_items = int(((stop - start) / step) + 1)
    if start + (max_items * step) > stop:
        max_items -= 1

    # It's hard to work out if we need to do one more so do it and we'll truncate excess
    stop += step

    dtype = numpy.float64
    if (
        numpy.issubdtype(start, numpy.integer)
        and numpy.issubdtype(stop, numpy.integer)
        and numpy.issubdtype(step, numpy.integer)
    ):
        dtype = numpy.int64

    return numpy.arange(start, stop, step, dtype=dtype)[:max_items]
