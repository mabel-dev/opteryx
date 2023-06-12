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

from opteryx.exceptions import SqlError
from opteryx.utils import dates


def generate_series(*args):
    from opteryx.managers.expression import NodeType

    arg_len = len(args)
    arg_vals = [i.value for i in args]
    first_arg_type = args[0].node_type

    # if the parameters are numbers, generate series is an alias for range
    if first_arg_type in (NodeType.LITERAL_NUMERIC, numpy.float64):
        if arg_len not in (2, 3):
            raise SqlError("generate_series for numbers takes 2 or 3 parameters.")
        return numeric_range(*arg_vals)

    # if the params are timestamps, we create time intervals
    if first_arg_type == NodeType.LITERAL_TIMESTAMP:
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
        step = 1
        stop += step
    elif len(args) == 3:
        start, stop, step = args
        # Ensure the last item is in the series
        if numpy.isclose((stop - start) / step % 1, 0):
            stop += step
    else:
        raise ValueError("Invalid number of arguments. Expected 2, or 3: start, stop [, step].")

    return numpy.arange(start, stop, step, dtype=numpy.float64)
